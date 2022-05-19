-module(erleans_provider_riak).

-include_lib("kernel/include/logger.hrl").


-behaviour(gen_server).

-export([start_link/2,
         all/2,
         delete/3,
         read/3,
         read_by_hash/3,
         insert/5,
         insert/6,
         update/6,
         update/7]).


-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

start_link(ProviderName, Args) ->
    ?LOG_INFO("erleans_provider_riak start_link ~p~p~n", [ProviderName, Args]),
    gen_server:start_link({local, ProviderName}, ?MODULE, [ProviderName, Args], []).

all(Type, ProviderName) ->
    ?LOG_DEBUG("all(~p~p)~n", [Type, ProviderName]),
    {ok, []}.
    % {ok, [{binary_to_term(IdBin), TypeBin, ETag, binary_to_term(StateBin)}|| {IdBin, ETag, StateBin} <- Rows]}.


read(Type, ProviderName, Id) ->
    ?LOG_DEBUG("erleans_provider_riak read(~p, ~p, ~p)~n", [Type, ProviderName, Id]),
    PBSocket = provider_info(ProviderName),
    {IdBucket, _HashBucket} = type2Buckets(Type),
    case riakc_pb_socket:get(PBSocket, IdBucket, Id) of
        {ok, Fetched} -> 
            ?LOG_DEBUG("fetched ~p~n", [Id]),
            #{object := Object, etag := Etag} = binary_to_term(riakc_obj:get_value(Fetched)),
            {ok, Object, Etag};
        _ ->
            ?LOG_DEBUG("not found ~p~n", [Id]),
            {error, not_found}
    end.

read_by_hash(Type, ProviderName, Hash) ->
    ?LOG_DEBUG("erleans_provider_riak read_by_hash(~p, ~p, ~p)~n", [Type, ProviderName, Hash]),
    PBSocket = provider_info(ProviderName),
    {_IdBucket, HashBucket} = type2Buckets(Type),
    case riakc_pb_socket:get(PBSocket, HashBucket, Hash) of
        {ok, Grain} -> 
            % ?LOG_DEBUG("erleans_provider_riak fetched ~p~n", [binary_to_term(riakc_obj:get_value(Grain))]),
            #{object := State, etag := ETag, id := Id} = binary_to_term(riakc_obj:get_value(Grain)),
            {ok, [{Id, Type, ETag, State}]};
        _ ->
            {error, not_found}
    end.

insert(Type, ProviderName, Id, State, ETag) ->
    insert(Type, ProviderName, Id, erlang:integer_to_binary(erlang:phash2({Id, Type})), State, ETag).

insert(Type, ProviderName, Id, Hash, State, ETag) when is_binary(Id) andalso is_binary(Hash) ->
    PBSocket = provider_info(ProviderName),
    Grain = #{id => Id, type => Type, hash => Hash, etag => ETag, object => State},
    BGrain = term_to_binary(Grain),
    {IdBucket, HashBucket} = type2Buckets(Type),
    % ?LOG_DEBUG("erleans_provider_riak insert (~p, ~p, ~p, ~p, ~p, ~p)~n", [Type, ProviderName, Id, Hash, State, ETag]),
    ?LOG_DEBUG( #{ action => insert, type => Type, provider_name => ProviderName, id => Id, hash => Hash, state => State, etag => ETag}),
    Obj1 = riakc_obj:new(IdBucket, Id, BGrain),
    Obj2 = riakc_obj:new(HashBucket, Hash, BGrain),
    riakc_pb_socket:put(PBSocket, Obj1),
    riakc_pb_socket:put(PBSocket, Obj2),
    ok.


update(Type, ProviderName, Id, State, ETag, NewETag) ->
    update(Type, ProviderName, Id, erlang:integer_to_binary(erlang:phash2({Id, Type})), State, ETag, NewETag).

update(Type, ProviderName, Id, Hash, State, ETag, NewETag) ->
    % io:format("erleans_provider_riak update (~p, ~p, ~p, ~p, ~p, ~p, ~p)~n", [Type, ProviderName, Id, Hash, State, ETag, NewETag]),
    % ?LOG_DEBUG("erleans_provider_riak update (~p, ~p, ~p, ~p, ~p, ~p, ~p)~n", [Type, ProviderName, Id, Hash, State, ETag, NewETag]),
    ?LOG_DEBUG(#{ action => update, type => Type, provider_name => ProviderName, id => Id, hash => Hash,  etag => ETag, newtag => NewETag}),
    case read(Type, ProviderName, Id) of
        {ok, _Object, CurrentETag} -> do_update(Type, ProviderName, Id, Hash, State, CurrentETag, ETag, NewETag);
        Error -> Error
    end.

do_update(Type, ProviderName, Id, Hash, State, CurrentETag, ETag, NewETag) when CurrentETag =:= ETag -> 
    % ?LOG_DEBUG("erleans_provider_riak do_update (~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p)~n", [Type, ProviderName, Id, Hash, State, CurrentETag, ETag, NewETag]),
    insert(Type, ProviderName, Id, Hash, State, NewETag);
do_update(_Type, _ProviderName, _Id, _Hash, _State, CurrentETag, ETag, _NewETag) when CurrentETag =/= ETag -> {error, bad_etag}.

delete(Type, ProviderName, Id) ->
    ?LOG_DEBUG("erleans_provider_riak delete (~p, ~p, ~p)~n", [Type, ProviderName, Id]),
    case read(Type, ProviderName, Id) of
        {ok, Object, _ETag} -> 
            #{hash := Hash} = Object,
            do_delete(Type, ProviderName, Id, Hash);
        Error -> Error
    end.

do_delete(Type, ProviderName, Id, Hash) ->
    PBSocket = provider_info(ProviderName),
    {IdBucket, HashBucket} = type2Buckets(Type),
    riakc_pb_socket:delete(PBSocket, IdBucket, Id),
    riakc_pb_socket:delete(PBSocket, HashBucket, Hash).

% we use two different buckets for key(id) and hash
% bucket names are type+grains_id and hash+grains_id
type2Buckets(Type) ->
    B1 = <<"_grains_id">>,
    B2 = <<"_grains_hash">>,
    BT = atom_to_binary(Type),
    IdBucket = <<BT/binary, B1/binary>>,
    HashBucket = <<BT/binary, B2/binary>>,
    {IdBucket, HashBucket}.
 
init([ProviderName, #{address := Address, pb_port := PBport} = Args]) ->
    ?LOG_DEBUG("epr init ~p ~p~n", [ProviderName, Args]),
    case riakc_pb_socket:start_link(Address, PBport) of
        {ok, PBSocketPid} -> 
            ?LOG_DEBUG("epr init success sock id:~p process id: ~p~n", [PBSocketPid, self()]),
            ets:new(epr_info, [named_table, set, {read_concurrency, true}, protected]),
            ets:insert(epr_info, {pb_socket_id, PBSocketPid});
        Other ->
            ?LOG_WARNING("epr init WTF ~p~n", [Other]),
            error(no_db_connection)
        end,
        {ok, #{ provider => ProviderName, riak_address => Address, pb_port => PBport}}.


handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

provider_info(_ProviderName) ->
    % ?LOG_DEBUG("erleans_provider_riak provider_info(~p) proc id ~p~n", [ProviderName, self()]),
    case ets:lookup(epr_info, pb_socket_id) of
        [] -> {error, no_pb_socket};
        [ProviderInfo] ->
            % ?LOG_DEBUG("lookup returned ~p~n", [ProviderInfo]),
            element(2, ProviderInfo)
        end.
