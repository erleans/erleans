-module(ets_provider).

-export([init/1,
         all/1,
         read/2,
         insert/4,
         replace/5,
         update/5]).

-define(TAB, ets_provider_tab).

init(_Args) ->
    ets:new(?TAB, [public, named_table, set, {keypos, 1}]).

all(Type) ->
    ets:match_object(?TAB, {'_', Type, '_'}).

read(Type, Id) ->
    case ets:lookup(?TAB, Id) of
        [{Id, Type, {Object, ETag}}] ->
            {ok, Object, ETag};
        _ ->
            {error, not_found}
    end.

insert(Type, Id, State, ETag) ->
    true = ets:insert(?TAB, {Id, Type, {State, ETag}}),
    ok.

replace(Type, Id, State, ETag, NewETag) ->
    case ets:lookup(?TAB, Id) of
        [{Id, Type, {_, E}}] when E =:= ETag ->
            true = ets:insert(?TAB, {Id, Type, {State, NewETag}}),
            ok;
        [{Id, Type, {_, E}}] when E =/= ETag ->
            {error, {bad_etag, E, ETag}};
        _ ->
            {error, not_found}
    end.

update(_, _Id, _Updates, _ETag, _NewETag) ->
    ok.
