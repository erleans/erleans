-module(ets_provider).

-export([init/0,
         read/1,
         insert/2,
         update/3,
         update/4]).

-define(TAB, ets_provider_tab).

init() ->
    ets:new(?TAB, [public, named_table, set, {keypos, 1}]).

read(Id) ->
    case ets:lookup(?TAB, Id) of
        [{Id, Object}] ->
            {ok, Object};
        _ ->
            {error, not_found}
    end.

insert(Id, State) ->
    true = ets:insert(?TAB, {Id, State}),
    ok.

update(Id, State, _Predicates) ->
    true = ets:insert(?TAB, {Id, State}),
    ok.

update(_Ref, _Id, _Updates, _Predicates) ->
    ok.
