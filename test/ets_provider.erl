-module(ets_provider).

-export([init/0,
         read/1,
         insert/3,
         update/4,
         update/5]).

-define(TAB, ets_provider_tab).

init() ->
    ets:new(?TAB, [public, named_table, set, {keypos, 1}]).

read(Id) ->
    case ets:lookup(?TAB, Id) of
        [{Id, {Object, ETag}}] ->
            {ok, Object, ETag};
        _ ->
            {error, not_found}
    end.

insert(Id, State, ETag) ->
    true = ets:insert(?TAB, {Id, {State, ETag}}),
    ok.

update(Id, State, ETag, NewETag) ->
    case ets:lookup(?TAB, Id) of
        [{Id, {_, E}}] when E =:= ETag ->
            true = ets:insert(?TAB, {Id, {State, NewETag}}),
            ok;
        [{Id, {_, E}}] when E =/= ETag ->
            {error, {bad_etag, E, ETag}};
        _ ->
            {error, not_found}
    end.

update(_Ref, _Id, _Updates, _Predicates, _ETag) ->
    ok.
