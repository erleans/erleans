%%%----------------------------------------------------------------------------
%%% Copyright Space-Time Insight 2017. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%----------------------------------------------------------------------------

%%% ---------------------------------------------------------------------------
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_pgsql_blob_provider).

-behaviour(erleans_provider).

-export([init/2,
         post_init/2,
         connect/1,
         close/1,
         all/2,
         delete/3,
         read/3,
         read_by_hash/3,
         insert/5,
         insert/6,
         update/6,
         update/7]).

-include("erleans.hrl").

init(_ProviderName, Args) ->
    load_queries(?MODULE, filename:join(code:priv_dir(erleans), "blob_provider.sql")),
    {pool, Args}.

post_init(ProviderName, _Args) ->
    case do(ProviderName, fun(C) -> create_grains_table(C) end, 10) of
        {error, no_db_connection} ->
            error(no_db_connection);
        _ ->
            ok
    end.

connect(Args) ->
    case pgsql_connection:open(Args) of
        {pgsql_connection, Pid} ->
            {ok, Pid};
        {error, Reason} ->
            lager:error("pgsql_connection:open failed reason=~p", [Reason]),
            error
    end.

close(Pid) ->
    pgsql_connection:close({pgsql_connection, Pid}).

all(Type, ProviderName) ->
    do(ProviderName, fun(C) -> all_(Type, C) end).

read(Type, ProviderName, Id) ->
    do(ProviderName, fun(C) ->
                         case read(Id, Type, erlang:phash2({Id, Type}), C) of
                             {ok, {_, _, ETag, State}} ->
                                 {ok, binary_to_term(State), ETag};
                             error ->
                                 not_found
                         end
                     end).

delete(Type, ProviderName, Id) ->
    do(ProviderName,
       fun(C) ->
               delete(Id, Type, erlang:phash2({Id, Type}), C)
       end).

read_by_hash(Type, ProviderName, Hash) ->
    do(ProviderName, fun(C) -> read_by_hash_(Hash, Type, C) end).

insert(Type, ProviderName, Id, State, ETag) ->
    insert(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, ETag).

insert(Type, ProviderName, Id, Hash, State, ETag) ->
    do(ProviderName, fun(C) -> insert_(Id, Type, Hash, ETag, State, C) end).

update(Type, ProviderName, Id, State, OldETag, NewETag) ->
    update(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, OldETag, NewETag).

update(Type, ProviderName, Id, Hash, State, OldETag, NewETag) ->
    do(ProviderName, fun(C) -> update_(Id, Type, Hash, OldETag, NewETag, State, C) end).

%%%

do(ProviderName, Fun) ->
    do(ProviderName, Fun, 1).

do(_ProviderName, _Fun, 0) ->
    lager:error("failed to obtain database connection"),
    {error, no_db_connection};
do(ProviderName, Fun, Retry) ->
    C = erleans_conn_manager:get_connection(ProviderName),
    try C of
        {error, timeout} ->
            timer:sleep(200),
            do(ProviderName, Fun, Retry-1);
        _ ->
            Fun(C)
    after
        erleans_conn_manager:done(ProviderName, C)
    end.

create_grains_table(C) ->
    {{create,table},[]} = pgsql_connection:simple_query(query(create_table), {pgsql_connection, C}),
    pgsql_connection:simple_query(query(create_idx), {pgsql_connection, C}).

all_(Type, C) ->
    Q = query(select_all),
    {{select, _}, Rows} = pgsql_connection:extended_query(Q, [atom_to_binary(Type, utf8)], {pgsql_connection, C}),
    {ok, [{binary_to_term(IdBin), Type, ETag, binary_to_term(StateBin)}
         || {IdBin, ETag, StateBin} <- Rows]}.


read(Id, Type, RefHash, C) ->
    Q = query(select),
    RefHash = erlang:phash2({Id, Type}),
    {{select, _}, Rows} = pgsql_connection:extended_query(Q, [RefHash, atom_to_binary(Type, unicode)],
                                                          {pgsql_connection, C}),
    IdBin = term_to_binary(Id),
    TypeBin = atom_to_binary(Type, utf8),
    ec_lists:find(fun({RowId, RowType, _, _}) when IdBin =:= RowId
                                                 , TypeBin =:= RowType-> true;
                     (_) ->
                      false
                  end, Rows).

delete(Id, Type, RefHash, C) ->
    Q = query(delete),
    RefHash = erlang:phash2({Id, Type}),
    case pgsql_connection:extended_query(Q, [RefHash,
                                             term_to_binary(Id),
                                             atom_to_binary(Type, unicode)],
                                         {pgsql_connection, C}) of
        {{delete, 0}, []} ->
            ok
    end.

read_by_hash_(Hash, Type, C) ->
    Q = query(select),
    {{select, _}, Rows} = pgsql_connection:extended_query(Q, [Hash, atom_to_binary(Type, unicode)],
                                                          {pgsql_connection, C}),
    {ok, [{binary_to_term(IdBin), Type, ETag, binary_to_term(StateBin)}
         || {IdBin, _, ETag, StateBin} <- Rows]}.

insert_(Id, Type, RefHash, GrainETag, GrainState, C) ->
    Q = query(insert),
    IdBin = term_to_binary(Id),
    {{insert, _, 1}, []} = pgsql_connection:extended_query(Q, [IdBin, atom_to_binary(Type, utf8), RefHash,
                                                               GrainETag, term_to_binary(GrainState)],
                                                           {pgsql_connection, C}).

update_(Id, Type, RefHash, OldGrainETag, NewGrainETag, GrainState, C) ->
    Q = query(update),
    IdBin = term_to_binary(Id),
    case pgsql_connection:extended_query(Q, [NewGrainETag, term_to_binary(GrainState), RefHash, IdBin,
                                             atom_to_binary(Type, utf8), OldGrainETag],
                                         {pgsql_connection, C}) of
        {{update, 1}, []} ->
            ok;
        {{update, 0}, []} ->
            {error, bad_etag}
    end.

load_queries(Module, File) ->
    ets:new(Module, [named_table, set, {read_concurrency, true}]),
    {ok, Queries} = eql:compile(File),
    ets:insert(Module, Queries).

query(Name) ->
    case ets:lookup(?MODULE, Name) of
        [] ->
            not_found;
        [{_, Query}] ->
            Query
    end.
