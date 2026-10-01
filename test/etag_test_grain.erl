-module(etag_test_grain).
-behaviour(erleans_grain).

-export([placement/0, state/1, activate/2, handle_call/3, handle_cast/2, deactivate/1]).

placement() -> prefer_local.
state({activation_mutations, _}) -> #{value => a, activations => 0};
state(_) -> #{value => a}.

activate(#{id := activation_failure}, _) ->
    {error, activation_failed};
activate(#{id := {activation_mutations, plain}}, State = #{activations := N}) ->
    {ok, State#{activations => N + 1}, #{deactivate_after => 50000}};
activate(#{id := {activation_mutations, ephemeral}}, State = #{activations := N}) ->
    {ok, {make_ref(), State#{activations => N + 1}}, #{deactivate_after => 50000}};
activate(#{id := {pause, Test, Tag}}, State) ->
    Test ! {Tag, activating, self()},
    receive {Tag, resume} -> ok end,
    {ok, State, #{deactivate_after => 50000}};
activate(_, State) ->
    {ok, State, #{deactivate_after => 50000}}.

handle_call({deactivate, Test, Tag, Kind}, From, State) ->
    {deactivate, {{Test, Tag, Kind}, State}, [{reply, From, ok}]};
handle_call(state, From, State) ->
    {ok, State, [{reply, From, State}]};
handle_call(get, From, State = #{value := Value}) ->
    {ok, State, [{reply, From, Value}]};
handle_call(save, From, State) ->
    {ok, State, [save_state, {reply, From, ok}]};
handle_call({set, Value}, From, State) ->
    {ok, State#{value => Value}, [save_state, {reply, From, ok}]}.

handle_cast(_, State) -> {ok, State}.
deactivate({{Test, Tag, Kind}, State}) ->
    Test ! {Tag, deactivating, self()},
    receive {Tag, finish_deactivate} -> ok end,
    case Kind of
        plain -> {save_state, State#{value => saved}};
        ephemeral -> {save_state, {undefined, State#{value => saved}}};
        no_save -> {ok, State}
    end;
deactivate(State = #{activations := _}) -> {ok, State};
deactivate(State = {_, #{activations := _}}) -> {ok, State};
deactivate(State) -> {save_state, State}.
