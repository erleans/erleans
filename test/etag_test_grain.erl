-module(etag_test_grain).
-behaviour(erleans_grain).

-export([test_key/1, test_control/1, clear_test_keys/0]).

-export([placement/0, state/1, activate/2, handle_call/3, handle_cast/2, deactivate/1]).

placement() -> prefer_local.
state(<<"activation_mutations:", _/binary>>) -> #{value => a, activations => 0};
state(_) -> #{value => a}.

activate(#{id := <<"activation_failure">>}, _) ->
    {error, activation_failed};
activate(#{id := <<"activation_mutations:plain">>}, State = #{activations := N}) ->
    {ok, State#{activations => N + 1}, #{deactivate_after => 50000}};
activate(#{id := <<"activation_mutations:ephemeral">>}, State = #{activations := N}) ->
    {ok, {make_ref(), State#{activations => N + 1}}, #{deactivate_after => 50000}};
activate(#{id := Id}, State) ->
    case test_control(Id) of
        {pause, Test, Tag} ->
            Test ! {Tag, activating, self()},
            receive {Tag, resume} -> ok end;
        _ -> ok
    end,
    {ok, State, #{deactivate_after => 50000}}.

%% Test coordination belongs outside the persistent grain identity.
test_key(Control) ->
    Id = integer_to_binary(erlang:unique_integer([positive])),
    persistent_term:put({?MODULE, Id}, Control),
    Id.

test_control(Id) -> persistent_term:get({?MODULE, Id}, undefined).

clear_test_keys() ->
    [persistent_term:erase(Key) || {Key = {?MODULE, _}, _} <- persistent_term:get()],
    ok.

handle_call({deactivate, Test, Tag, Kind}, From, State) ->
    {deactivate, {{Test, Tag, Kind}, State}, [{reply, From, ok}]};
handle_call({pending_on, Node, Test, Tag}, From, State) ->
    case node() of
        Node ->
            Test ! {Tag, pending, self()},
            {ok, State#{value => unsaved}};
        _ ->
            {ok, State, [{reply, From, self()}]}
    end;
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
