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
         read/3,
         insert/5,
         replace/6]).

init(_ProviderName, Args) ->
    {pool, Args}.

post_init(ProviderName, _Args) ->
    do(ProviderName, fun(C) -> create_grains_table(C) end).

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

insert(Type, ProviderName, Id, State, ETag) ->
    do(ProviderName, fun(C) -> insert(Id, Type, erlang:phash2({Id, Type}), ETag, State, C) end).

replace(Type, ProviderName, Id, State, OldETag, NewETag) ->
    do(ProviderName, fun(C) -> replace(Id, Type, erlang:phash2({Id, Type}), OldETag, NewETag, State, C) end).

%%%

do(ProviderName, Fun) ->
    C = erleans_conn_manager:get_connection(ProviderName),
    try
        Fun(C)
    after
        erleans_conn_manager:done(ProviderName, C)
    end.

create_grains_table(C) ->
    {{create,table},[]} = pgsql_connection:simple_query(["CREATE TABLE IF NOT EXISTS erleans_grains ( "
                                                        "  grain_id BYTEA NOT NULL, ", % CHARACTER VARYING(2048), "
                                                        "  grain_type CHARACTER VARYING(2048), "
                                                        "  grain_ref_hash BIGINT NOT NULL, "
                                                        "  grain_etag BIGINT NOT NULL, "
                                                        "  grain_state BYTEA NOT NULL, "
                                                        "  change_time TIMESTAMP NOT NULL,"
                                                        "  PRIMARY KEY (grain_id, grain_type))"], {pgsql_connection, C}),
    pgsql_connection:simple_query(["CREATE INDEX IF NOT EXISTS ",
                                   "erleans_grains_term_idx ON erleans_grains USING HASH (grain_state)"], {pgsql_connection, C}).

all_(Type, C) ->
    Q = ["SELECT grain_id, grain_etag, grain_state ",
         "FROM erleans_grains WHERE grain_type = $1"],
    {{select, _}, Rows} = pgsql_connection:extended_query(Q, [atom_to_binary(Type, utf8)], {pgsql_connection, C}),
    {ok, [{binary_to_term(IdBin), Type, ETag, binary_to_term(StateBin)}
         || {IdBin, ETag, StateBin} <- Rows]}.


read(Id, Type, RefHash, C) ->
    Q = ["SELECT grain_id, grain_type, grain_etag, grain_state ",
         "FROM erleans_grains WHERE grain_ref_hash = $1"],
    RefHash = erlang:phash2({Id, Type}),
    {{select, _}, Rows} = pgsql_connection:extended_query(Q, [RefHash],
                                                          {pgsql_connection, C}),
    IdBin = term_to_binary(Id),
    TypeBin = atom_to_binary(Type, utf8),
    ec_lists:find(fun({RowId, RowType, _, _}) when IdBin =:= RowId
                                                 , TypeBin =:= RowType-> true;
                     (_) ->
                      false
                  end, Rows).

insert(Id, Type, RefHash, GrainETag, GrainState, C) ->
    Q = ["INSERT INTO erleans_grains (grain_id, grain_type, grain_ref_hash, grain_etag, grain_state, change_time) VALUES ",
         "($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)"],
    IdBin = term_to_binary(Id),
    {{insert, _, 1}, []} = pgsql_connection:extended_query(Q, [IdBin, atom_to_binary(Type, utf8), RefHash,
                                                               GrainETag, term_to_binary(GrainState)],
                                                           {pgsql_connection, C}).

replace(Id, Type, RefHash, OldGrainETag, NewGrainETag, GrainState, C) ->
    Q = ["UPDATE erleans_grains SET grain_etag = $1, grain_state = $2, change_time = CURRENT_TIMESTAMP ",
         "WHERE grain_ref_hash = $3 AND grain_id = $4 AND grain_type = $5 AND grain_etag = $6"],
    IdBin = term_to_binary(Id),
    case pgsql_connection:extended_query(Q, [NewGrainETag, term_to_binary(GrainState), RefHash, IdBin,
                                             atom_to_binary(Type, utf8), OldGrainETag],
                                         {pgsql_connection, C}) of
        {{update, 1}, []} ->
            ok;
        {{update, 0}, []} ->
            {error, bad_etag}
    end.
