-module(etag_test_grain).
-behaviour(erleans_grain).

-export([placement/0, state/1, activate/2, handle_call/3, handle_cast/2, deactivate/1]).

placement() -> prefer_local.
state(_) -> #{value => a}.

activate(#{id := {pause, Test, Tag}}, State) ->
    Test ! {Tag, activating, self()},
    receive {Tag, resume} -> ok end,
    {ok, State, #{deactivate_after => 50000}};
activate(_, State) ->
    {ok, State, #{deactivate_after => 50000}}.

handle_call(get, From, State = #{value := Value}) ->
    {ok, State, [{reply, From, Value}]};
handle_call(save, From, State) ->
    {ok, State, [save_state, {reply, From, ok}]};
handle_call({set, Value}, From, State) ->
    {ok, State#{value => Value}, [save_state, {reply, From, ok}]}.

handle_cast(_, State) -> {ok, State}.
deactivate(State) -> {save_state, State}.
