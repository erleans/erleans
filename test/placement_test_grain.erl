-module(placement_test_grain).
-behaviour(erleans_grain).

-export([placement/0, handle_call/3, handle_cast/2]).

placement() -> erleans_config:get(test_placement, prefer_local).

handle_call(pid, From, State) ->
    {ok, State, [{reply, From, self()}]}.

handle_cast(_, State) -> {ok, State}.
