-module(ets_provider).

-export([init/1,
         read/2,
         insert/4,
         replace/5,
         update/5]).

-define(TAB, ets_provider_tab).

init(_Args) ->
    ets:new(?TAB, [public, named_table, set, {keypos, 1}]).

read(_, Id) ->
    case ets:lookup(?TAB, Id) of
        [{Id, {Object, ETag}}] ->
            {ok, Object, ETag};
        _ ->
            {error, not_found}
    end.

insert(_, Id, State, ETag) ->
    true = ets:insert(?TAB, {Id, {State, ETag}}),
    ok.

replace(_, Id, State, ETag, NewETag) ->
    case ets:lookup(?TAB, Id) of
        [{Id, {_, E}}] when E =:= ETag ->
            true = ets:insert(?TAB, {Id, {State, NewETag}}),
            ok;
        [{Id, {_, E}}] when E =/= ETag ->
            {error, {bad_etag, E, ETag}};
        _ ->
            {error, not_found}
    end.

update(_, _Id, _Updates, _ETag, _NewETag) ->
    ok.
