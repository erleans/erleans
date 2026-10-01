-module(fault_test_grain).
-behaviour(erleans_grain).

-export([placement/0, state/1, activate/2, handle_call/3, handle_cast/2, handle_info/2]).

placement() -> prefer_local.
state(_) -> 0.
activate(_, State) -> {ok, State, #{deactivate_after => 50000}}.

handle_call(pid, From, State) ->
    {ok, State, [{reply, From, self()}]};
handle_call(increment, From, State) ->
    {ok, State + 1, [{reply, From, State + 1}]};
handle_call({raise, Test, Tag, Class, Reason}, _From, _State) ->
    Test ! {Tag, invoked, self()},
    raise(Class, Reason);
handle_call({block_then_raise, Test, Tag}, _From, _State) ->
    Test ! {Tag, blocked, self()},
    receive {Tag, release} -> error(callback_crash) end;
handle_call({block_on, Node, Test, Tag}, From, State) ->
    case node() of
        Node ->
            Test ! {Tag, blocked, self()},
            receive {Tag, release} -> ok end;
        _ -> ok
    end,
    {ok, State, [{reply, From, self()}]};
handle_call({reply, Reply}, From, State) ->
    {ok, State, [{reply, From, Reply}]}.

handle_cast(crash, _) -> error(callback_crash);
handle_cast(_, State) -> {ok, State}.
handle_info(crash, _) -> error(callback_crash);
handle_info(_, State) -> {ok, State}.

raise(error, Reason) -> error(Reason);
raise(exit, Reason) -> exit(Reason);
raise(throw, Reason) -> throw(Reason).
