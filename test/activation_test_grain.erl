%% A grain/provider pair whose read and activation are controlled by the test.
-module(activation_test_grain).
-behaviour(erleans_grain).

-export([placement/0, read/3, activate/2, handle_call/3, handle_cast/2]).

placement() -> prefer_local.

read(_, {Test, Tag}, _) ->
    Test ! {Tag, reading, self()},
    receive {Tag, read_result, Result} -> Result end.

activate(_, State = {Test, Tag}) ->
    Test ! {Tag, activating, self()},
    receive
        {Tag, activate_result, ok} ->
            {ok, State, #{deactivate_after => 50000}};
        {Tag, activate_result, Error} ->
            Error
    end.

handle_call(pid, From, State) ->
    {ok, State, [{reply, From, self()}]}.

handle_cast(_, State) -> {ok, State}.
