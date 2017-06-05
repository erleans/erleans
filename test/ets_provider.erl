-module(ets_provider).

-behaviour(erleans_provider).

-export([init/2,
         post_init/2,
         all/2,
         read/3,
         read_by_hash/3,
         insert/5,
         insert/6,
         replace/6,
         replace/7,
         delete/3]).

-define(TAB, ets_provider_tab).

init(_ProviderName, _Args) ->
    ets:new(?TAB, [public, named_table, set, {keypos, 1}]),
    ok.

post_init(_ProviderName, _Args) ->
    ok.

all(Type, _ProviderName) ->
    try
        {ok, ets:match_object(?TAB, {'_', Type, '_', '_', '_'})}
    catch
        error:badarg ->
            {error, missing_table}
    end.

read(Type, _ProviderName, Id) ->
    case ets:lookup(?TAB, Id) of
        [{Id, Type, _Hash, ETag, Object}] ->
            {ok, Object, ETag};
        _ ->
            {error, not_found}
    end.

read_by_hash(Type, _ProviderName, Hash) ->
    {ok, [{Id, Type, ETag, Object} ||
             {Id, _, _, ETag, Object} <- ets:match_object(?TAB, {'_', Type, Hash, '_', '_'})]}.

insert(Type, ProviderName, Id, State, ETag) ->
    insert(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, ETag).

insert(Type, _ProviderName, Id, Hash, State, ETag) ->
    true = ets:insert(?TAB, {Id, Type, Hash, ETag, State}),
    ok.

replace(Type, ProviderName, Id, State, ETag, NewETag) ->
    replace(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, ETag, NewETag).

replace(Type, _ProviderName, Id, Hash, State, ETag, NewETag) ->
    case ets:lookup(?TAB, Id) of
        [{Id, Type, _, E, _}] when E =:= ETag ->
            true = ets:insert(?TAB, {Id, Type, Hash, NewETag, State}),
            ok;
        [{Id, Type, _, E, _}] when E =/= ETag ->
            {error, bad_etag};
        _ ->
            {error, not_found}
    end.

delete(_Type, _ProviderName, Id) ->
    ets:delete(?TAB, Id).
