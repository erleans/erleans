-module(ets_provider).

-behaviour(erleans_provider).

-export([init/2,
         post_init/2,
         all/2,
         read/3,
         insert/5,
         replace/6]).

-define(TAB, ets_provider_tab).

init(_ProviderName, _Args) ->
    ok.

post_init(_ProviderName, _Args) ->
    ets:new(?TAB, [public, named_table, set, {keypos, 1}]).

all(Type, _ProviderName) ->
    try
        {ok, ets:match_object(?TAB, {'_', Type, '_', '_'})}
    catch
        error:badarg ->
            {error, missing_table}
    end.

read(Type, _ProviderName, Id) ->
    case ets:lookup(?TAB, Id) of
        [{Id, Type, ETag, Object}] ->
            {ok, Object, ETag};
        _ ->
            {error, not_found}
    end.

insert(Type, _ProviderName, Id, State, ETag) ->
    true = ets:insert(?TAB, {Id, Type, ETag, State}),
    ok.

replace(Type, _ProviderName, Id, State, ETag, NewETag) ->
    case ets:lookup(?TAB, Id) of
        [{Id, Type, E, _}] when E =:= ETag ->
            true = ets:insert(?TAB, {Id, Type, NewETag, State}),
            ok;
        [{Id, Type, E, _}] when E =/= ETag ->
            {error, bad_etag};
        _ ->
            {error, not_found}
    end.
