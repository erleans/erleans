-module(provider_etag_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         versions/1, concurrent_insert/1, concurrent_update/1,
         grain_keys_and_hashes/1, opaque_tokens/1,
         activation_insert_conflict/1, storage_failures/1,
         pending_calls_on_conflict/1,
         activation_mutations_require_save/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("test_utils.hrl").

-define(provider, erleans_provider_ets).
-define(store, in_memory).

all() ->
    [versions, concurrent_insert, concurrent_update, grain_keys_and_hashes,
     opaque_tokens, activation_insert_conflict, storage_failures,
     pending_calls_on_conflict, activation_mutations_require_save].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_) ->
    application:stop(erleans),
    etag_test_grain:clear_test_keys(),
    ok.

versions(_) ->
    Id = <<"versions">>,
    ?assertEqual({ok, 1}, ?provider:insert(?MODULE, ?store, Id, a)),
    ?assertEqual({error, bad_etag}, ?provider:insert(?MODULE, ?store, Id, overwritten)),
    ?assertEqual({ok, a, 1}, ?provider:read(?MODULE, ?store, Id)),
    ?assertEqual({ok, 2}, ?provider:update(?MODULE, ?store, Id, a, 1)),
    ?assertEqual({ok, 3}, ?provider:update(?MODULE, ?store, Id, b, 2)),
    ?assertEqual({ok, 4}, ?provider:update(?MODULE, ?store, Id, a, 3)),
    ?assertEqual({error, bad_etag}, ?provider:update(?MODULE, ?store, Id, stale, 1)),
    ?assertEqual({ok, a, 4}, ?provider:read(?MODULE, ?store, Id)),
    ?assertEqual({error, bad_etag}, ?provider:update(?MODULE, ?store, Id, stale, <<"4">>)),
    ?assertEqual({error, not_found}, ?provider:update(?MODULE, ?store, <<"missing">>, a, 1)).

concurrent_insert(_) ->
    Results = race(fun(I) -> ?provider:insert(?MODULE, ?store, <<"concurrent_insert">>, I) end),
    [{Winner, {ok, 1}}] = [{I, R} || {I, {ok, 1} = R} <- Results],
    ?assertEqual(15, length([ok || {_, {error, bad_etag}} <- Results])),
    ?assertEqual({ok, Winner, 1}, ?provider:read(?MODULE, ?store, <<"concurrent_insert">>)).

concurrent_update(_) ->
    {ok, 1} = ?provider:insert(?MODULE, ?store, <<"concurrent_update">>, initial),
    Results = race(fun(I) -> ?provider:update(?MODULE, ?store, <<"concurrent_update">>, I, 1) end),
    [{Winner, {ok, 2}}] = [{I, R} || {I, {ok, 2} = R} <- Results],
    ?assertEqual(15, length([ok || {_, {error, bad_etag}} <- Results])),
    ?assertEqual({ok, Winner, 2}, ?provider:read(?MODULE, ?store, <<"concurrent_update">>)).

grain_keys_and_hashes(_) ->
    %% Ids and payloads must remain literal even if they look like match specs.
    Id = <<"$1:_">>,
    Payload = #{value => {'$2', {const, '_'}}},
    {ok, 1} = ?provider:insert(first_type, ?store, Id, 123, first),
    {ok, 1} = ?provider:insert(second_type, ?store, Id, 123, second),
    {ok, 2} = ?provider:update(first_type, ?store, Id, 456, Payload, 1),
    ?assertEqual({ok, Payload, 2}, ?provider:read(first_type, ?store, Id)),
    ?assertEqual({ok, second, 1}, ?provider:read(second_type, ?store, Id)),
    ?assertEqual({ok, []}, ?provider:read_by_hash(first_type, ?store, 123)),
    ?assertEqual({ok, [{Id, first_type, 2, Payload}]},
                 ?provider:read_by_hash(first_type, ?store, 456)),
    ?assertEqual({ok, [{Id, first_type, 456, 2, Payload}]}, ?provider:all(first_type, ?store)),
    true = ?provider:delete(first_type, ?store, Id),
    ?assertEqual({error, not_found}, ?provider:read(first_type, ?store, Id)),
    ?assertEqual({ok, second, 1}, ?provider:read(second_type, ?store, Id)).

opaque_tokens(_) ->
    Grain = grain(<<"opaque_tokens">>),
    ?assertEqual(a, erleans_grain:call(Grain, get)),
    ?assertEqual({ok, #{value => a}, <<"version:1">>}, read(<<"opaque_tokens">>)),
    ok = erleans_grain:call(Grain, save),
    ?assertEqual({ok, #{value => a}, <<"version:2">>}, read(<<"opaque_tokens">>)),
    ok = erleans_grain:call(Grain, {set, b}),
    ?assertEqual({ok, #{value => b}, <<"version:3">>}, read(<<"opaque_tokens">>)),
    Pid = erleans_grain_registry:whereis_name(Grain),
    ok = gen_statem:stop(Pid, shutdown, infinity),
    ?assertEqual({ok, #{value => b}, <<"version:4">>}, read(<<"opaque_tokens">>)),
    %% A new activation must retain the token returned by read, too.
    ?assertEqual(b, erleans_grain:call(Grain, get)),
    ok = erleans_grain:call(Grain, save),
    ?assertEqual({ok, #{value => b}, <<"version:5">>}, read(<<"opaque_tokens">>)),

    Ephemeral0 = erleans:get_grain(test_ephemeral_state_grain, <<"opaque_ephemeral">>),
    Ephemeral = Ephemeral0#{provider => {opaque_etag_provider, ?store}},
    ?assertEqual({ok, 0}, test_ephemeral_state_grain:ephemeral_counter(Ephemeral)),
    ok = test_ephemeral_state_grain:increment_ephemeral_counter(Ephemeral),
    ?assertMatch({ok, #{activated_counter := 1}, <<"version:2">>},
                 opaque_etag_provider:read(test_ephemeral_state_grain, ?store, <<"opaque_ephemeral">>)).

activation_insert_conflict(_) ->
    Tag = make_ref(),
    Id = etag_test_grain:test_key({pause, self(), Tag}),
    Grain = grain(Id),
    {Caller, Monitor} = spawn_monitor(fun() -> erleans_grain:call(Grain, get) end),
    Activation = receive {Tag, activating, Pid} -> Pid
                 after 1000 -> ct:fail(activation_not_started)
                 end,
    Winner = #{value => winner},
    {ok, <<"version:1">>} = opaque_etag_provider:insert(etag_test_grain, ?store, Id, Winner),
    Activation ! {Tag, resume},
    receive {'DOWN', Monitor, process, Caller, {noproc, bad_etag}} -> ok
    after 1000 -> ct:fail(activation_did_not_reject_conflict)
    end,
    ?assertEqual({ok, Winner, <<"version:1">>}, read(Id)).

pending_calls_on_conflict(_) ->
    lists:foreach(fun(Shape) ->
        Test = self(),
        Tag = make_ref(),
        Id = etag_test_grain:test_key({conflict, Shape, Test, Tag}),
        Grain = grain(Id),
        ?assertEqual(a, erleans_grain:call(Grain, get)),
        Pid = erleans_grain_registry:whereis_name(Grain),
        true = is_pid(Pid),
        Monitor = monitor(process, Pid),
        SaveCaller = spawn_monitor(fun() ->
            Test ! {Tag, self(), erleans_grain:call(Grain, save)}
        end),
        receive {Tag, saving, Pid} -> ok
        after 1000 -> ct:fail(save_not_started)
        end,
        Readers = [spawn_monitor(fun() ->
            Test ! {Tag, self(), erleans_grain:call(Target, get)}
        end) || Target <- [Grain, Pid]],
        ?UNTIL(begin
            {messages, Messages} = process_info(Pid, messages),
            length([ok || {'$gen_call', _, _} <- Messages]) =:= 2
        end),
        %% Simulate a competing writer while the grain's save is blocked.
        {ok, 2} = ?provider:update(etag_test_grain, ?store, Id, #{value => winner}, 1),
        Pid ! {Tag, finish_save},
        [begin
             receive {Tag, Caller, Result} -> ?assertEqual({exit, saved_etag_changed}, Result)
             after 1000 -> ct:fail({missing_conflict_result, Shape})
             end,
             receive {'DOWN', M, process, Caller, normal} -> ok
             after 1000 -> ct:fail(caller_failed)
             end
         end || {Caller, M} <- [SaveCaller | Readers]],
        receive {'DOWN', Monitor, process, Pid, _} -> ok
        after 1000 -> ct:fail(stale_activation_survived)
        end,
        ?assertEqual(undefined, erleans_grain_registry:whereis_name(Grain)),
        ?assertEqual({ok, #{value => winner}, <<"version:2">>}, read(Id))
    end, [bare, detailed]).

storage_failures(_) ->
    ?assertExit({noproc, read_failed}, erleans_grain:call(grain(<<"read_failure">>), get)),
    ?assertExit({noproc, insert_failed}, erleans_grain:call(grain(<<"insert_failure">>), get)),
    ?assertEqual({error, not_found}, ?provider:read(etag_test_grain, ?store, <<"read_failure">>)),
    ?assertEqual({error, not_found}, ?provider:read(etag_test_grain, ?store, <<"insert_failure">>)),
    Existing = #{value => existing},
    {ok, 1} = ?provider:insert(etag_test_grain, ?store, <<"read_failure">>, Existing),
    ?assertExit({noproc, read_failed}, erleans_grain:call(grain(<<"read_failure">>), get)),
    ?assertEqual({ok, Existing, 1}, ?provider:read(etag_test_grain, ?store, <<"read_failure">>)),
    ?assertExit({noproc, activation_failed}, erleans_grain:call(grain(<<"activation_failure">>), get)),
    ?assertEqual({error, not_found}, ?provider:read(etag_test_grain, ?store, <<"activation_failure">>)),
    Grain = grain(<<"write_failure">>),
    ?assertEqual(a, erleans_grain:call(Grain, get)),
    ?assertExit({write_failed, _}, erleans_grain:call(Grain, {set, b})),
    ?assertEqual({ok, #{value => a}, <<"version:1">>}, read(<<"write_failure">>)).

activation_mutations_require_save(_) ->
    lists:foreach(fun(Kind) ->
        Id = <<"activation_mutations:", (atom_to_binary(Kind))/binary>>,
        Grain = grain(Id),
        Initial = #{value => a, activations => 0},
        Activated = Initial#{activations => 1},
        First = erleans_grain:call(Grain, state),
        assert_activation_state(Kind, Activated, First),
        ?assertEqual({ok, Initial, <<"version:1">>}, read(Id)),
        stop_without_saving(Grain),

        %% An unsaved first activation behaves exactly like later activations.
        Second = erleans_grain:call(Grain, state),
        assert_activation_state(Kind, Activated, Second),
        ?assertEqual({ok, Initial, <<"version:1">>}, read(Id)),
        ok = erleans_grain:call(Grain, save),
        ?assertEqual({ok, Activated, <<"version:2">>}, read(Id)),
        stop_without_saving(Grain),

        Third = erleans_grain:call(Grain, state),
        Updated = Initial#{activations => 2},
        assert_activation_state(Kind, Updated, Third),
        ?assertEqual({ok, Activated, <<"version:2">>}, read(Id)),
        ok = erleans_grain:call(Grain, save),
        ?assertEqual({ok, Updated, <<"version:3">>}, read(Id)),
        stop_without_saving(Grain)
    end, [plain, ephemeral]).

assert_activation_state(plain, Expected, State) ->
    ?assertEqual(Expected, State);
assert_activation_state(ephemeral, Expected, {Ephemeral, Persistent}) ->
    ?assert(is_reference(Ephemeral)),
    ?assertEqual(Expected, Persistent).

stop_without_saving(Grain) ->
    Pid = erleans_grain_registry:whereis_name(Grain),
    gen_statem:stop(Pid, shutdown, infinity).

grain(Id) ->
    Ref = erleans:get_grain(etag_test_grain, Id),
    Ref#{provider => {opaque_etag_provider, ?store}}.

read(Id) ->
    opaque_etag_provider:read(etag_test_grain, ?store, Id).

race(Fun) ->
    Test = self(),
    Tag = make_ref(),
    Callers = [spawn_monitor(fun() ->
                                    Test ! {Tag, ready, self()},
                                    receive {Tag, go} -> ok end,
                                    Test ! {Tag, self(), I, Fun(I)}
                            end) || I <- lists:seq(1, 16)],
    [receive {Tag, ready, Pid} -> ok after 1000 -> ct:fail(caller_not_ready) end
     || {Pid, _} <- Callers],
    [Pid ! {Tag, go} || {Pid, _} <- Callers],
    [begin
         Result = receive {Tag, Pid, I, R} -> {I, R}
                  after 1000 -> ct:fail(caller_did_not_reply)
                  end,
         receive {'DOWN', Monitor, process, Pid, normal} -> ok
         after 1000 -> ct:fail(caller_did_not_finish)
         end,
         Result
     end || {Pid, Monitor} <- Callers].
