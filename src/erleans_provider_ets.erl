-module(erleans_provider_ets).

-behaviour(erleans_provider).
-behaviour(gen_server).

-export([start_link/2,
         all/2,
         read/3,
         read_by_hash/3,
         insert/5,
         insert/6,
         update/6,
         update/7,
         delete/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

start_link(ProviderName, Args) ->
    gen_server:start_link({local, ProviderName}, ?MODULE, [ProviderName, Args], []).

all(Type, ProviderName) ->
    try
        {ok, ets:match_object(ProviderName, {'_', Type, '_', '_', '_'})}
    catch
        error:badarg ->
            {error, missing_table}
    end.

read(Type, ProviderName, Id) ->
    case ets:lookup(ProviderName, Id) of
        [{Id, Type, _Hash, ETag, Object}] ->
            {ok, Object, ETag};
        _ ->
            {error, not_found}
    end.

read_by_hash(Type, ProviderName, Hash) ->
    {ok, [{Id, Type, ETag, Object} ||
             {Id, _, _, ETag, Object} <- ets:match_object(ProviderName, {'_', Type, Hash, '_', '_'})]}.

insert(Type, ProviderName, Id, State, ETag) ->
    insert(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, ETag).

insert(Type, ProviderName, Id, Hash, State, ETag) ->
    true = ets:insert(ProviderName, {Id, Type, Hash, ETag, State}),
    ok.

update(Type, ProviderName, Id, State, ETag, NewETag) ->
    update(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, ETag, NewETag).

update(Type, ProviderName, Id, Hash, State, ETag, NewETag) ->
    case ets:lookup(ProviderName, Id) of
        [{Id, Type, _, E, _}] when E =:= ETag ->
            true = ets:insert(ProviderName, {Id, Type, Hash, NewETag, State}),
            ok;
        [{Id, Type, _, E, _}] when E =/= ETag ->
            {error, bad_etag};
        _ ->
            {error, not_found}
    end.

delete(_Type, ProviderName, Id) ->
    ets:delete(ProviderName, Id).


init([ProviderName, _]) ->
    Tid = ets:new(ProviderName, [public, named_table, set, {keypos, 1}]),
    {ok, Tid}.

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.
