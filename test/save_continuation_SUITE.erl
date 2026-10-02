-module(save_continuation_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         save_success/1, conflict_reload/1, unhandled_conflict/1,
         handled_stop/1, storage_error/1, invalid_recovery/1,
         reload_failure/1, continuation_failure/1, reload_serialization/1,
         duplicate_save/1, asynchronous_callbacks/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("test_utils.hrl").

all() ->
    [save_success, conflict_reload, unhandled_conflict, handled_stop,
     storage_error, invalid_recovery, reload_failure, continuation_failure,
     reload_serialization, duplicate_save, asynchronous_callbacks].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_) ->
    application:stop(erleans),
    etag_test_grain:clear_test_keys(),
    ok.

save_success(_) ->
    {Grain, Id, Pid} = grain(),
    Candidate = #{value => saved},
    Reply = actions(Grain, Candidate, fun(From) ->
        [{save_state, fun(ok) ->
            ?assertEqual(Pid, self()),
            ?assertEqual({ok, Candidate, <<"version:2">>}, read(Id)),
            {continue, [{reply, From, saved}]}
        end}]
    end),
    ?assertEqual(saved, Reply),
    ?assertEqual(Candidate, erleans_grain:call(Pid, state)),
    %% The next write must use the ETag installed before the continuation.
    ok = erleans_grain:call(Pid, save),
    ?assertEqual({ok, Candidate, <<"version:3">>}, read(Id)).

conflict_reload(_) ->
    lists:foreach(fun(Kind) ->
        {Grain, Id, Pid} = grain(),
        Ephemeral = make_ref(),
        Previous = wrap(Kind, Ephemeral, #{value => unsaved}),
        ok = actions(Grain, Previous, fun(From) -> [{reply, From, ok}] end),
        Winner = #{value => winner},
        {ok, <<"version:2">>} = write(Id, Winner, <<"version:1">>),
        Reloaded = wrap(Kind, Ephemeral, Winner),
        Candidate = wrap(Kind, make_ref(), #{value => rejected}),
        Test = self(),
        Tag = make_ref(),
        Reply = actions(Grain, Candidate, fun(From) ->
            [{reply, From, wrong_reply}, {reply, {Test, Tag}, wrong_effect},
             {save_state, fun({error, saved_etag_changed}) ->
                 {reload, fun(State) ->
                     ?assertEqual(Pid, self()),
                     {continue, [{reply, From, {reloaded, State}}]}
                 end}
             end},
             {reply, {Test, Tag}, wrong_effect_after}]
        end),
        ?assertEqual({reloaded, Reloaded}, Reply),
        ?assertEqual(Pid, erleans_grain_registry:whereis_name(Grain)),
        ?assertEqual(Reloaded, erleans_grain:call(Pid, state)),
        receive {Tag, _} -> ct:fail(sibling_action_on_failure) after 0 -> ok end,
        ?assertEqual({ok, Winner, <<"version:2">>}, read(Id)),
        ok = erleans_grain:call(Pid, save),
        ?assertEqual({ok, Winner, <<"version:3">>}, read(Id))
    end, [plain, ephemeral]).

unhandled_conflict(_) ->
    lists:foreach(fun(Reason) ->
        {Grain, Id, Pid} = grain(),
        Monitor = monitor(process, Pid),
        etag_test_grain:set_test_control(Id, {write_error, Reason}),
        ?assertEqual({exit, saved_etag_changed}, erleans_grain:call(Grain, save)),
        down(Pid, Monitor, saved_etag_changed),
        ?assertEqual({ok, #{value => a}, <<"version:1">>}, read(Id))
    end, [bad_etag, {bad_etag, <<"version:1">>, <<"version:2">>}]).

handled_stop(_) ->
    {Grain, Id, Pid} = grain(),
    Monitor = monitor(process, Pid),
    {ok, _} = write(Id, #{value => winner}, <<"version:1">>),
    ?assertEqual(conflict, actions(Grain, #{value => rejected}, fun(From) ->
        [{save_state, fun({error, saved_etag_changed}) ->
            {stop, [{reply, From, conflict}]}
        end}]
    end)),
    down(Pid, Monitor, saved_etag_changed),
    ?assertEqual({ok, #{value => winner}, <<"version:2">>}, read(Id)).

storage_error(_) ->
    {Grain, Id, Pid} = grain(),
    Monitor = monitor(process, Pid),
    %% Even a shutdown-shaped provider error must not save on termination
    %% or make the routing layer replay the request.
    etag_test_grain:set_test_control(Id, {write_error, shutdown}),
    ?assertEqual(unavailable, actions(Grain, #{value => rejected}, fun(From) ->
        [{save_state, fun({error, shutdown}) ->
            etag_test_grain:set_test_control(Id, undefined),
            {stop, [{reply, From, unavailable}]}
        end}]
    end)),
    down(Pid, Monitor, {save_failed, shutdown}),
    ?assertEqual({ok, #{value => a}, <<"version:1">>}, read(Id)).

invalid_recovery(_) ->
    lists:foreach(fun({Error, Result}) ->
        {Grain, Id, Pid} = grain(),
        Monitor = monitor(process, Pid),
        etag_test_grain:set_test_control(Id, {write_error, Error}),
        ?assertExit({{bad_save_continuation_result, _, _}, _},
            actions(Grain, #{value => rejected}, fun(_From) ->
                [{save_state, fun(_) -> Result end}]
            end)),
        receive {'DOWN', Monitor, process, Pid, {bad_save_continuation_result, _, _}} -> ok
        after 1000 -> ct:fail(invalid_recovery_survived)
        end,
        ?assertEqual({ok, #{value => a}, <<"version:1">>}, read(Id))
    end, [{bad_etag, {continue, []}},
          {timeout, {reload, fun(_) -> {continue, []} end}}]).

reload_failure(_) ->
    lists:foreach(fun(Failure) ->
        {Grain, Id, Pid} = grain(),
        Monitor = monitor(process, Pid),
        {ok, _} = write(Id, #{value => winner}, <<"version:1">>),
        Test = self(),
        Tag = make_ref(),
        Outcome = try actions(Grain, #{value => rejected}, fun(_From) ->
            [{save_state, fun({error, saved_etag_changed}) ->
                case Failure of
                    missing -> erleans_provider_ets:delete(etag_test_grain, in_memory, Id);
                    _ -> etag_test_grain:set_test_control(Id, Failure)
                end,
                {reload, fun(_) -> Test ! {Tag, unexpected_reload}, {continue, []} end}
            end}]
        end)
        catch exit:Reason -> {exited, Reason}
        end,
        ?assertMatch({exited, {_, {gen_statem, call, _}}}, Outcome),
        receive
            {'DOWN', Monitor, process, Pid, {state_reload_failed, _}} -> ok;
            {'DOWN', Monitor, process, Pid, {state_reload_failed, exit, shutdown, _}} -> ok
        after 1000 -> ct:fail(reload_failure_survived)
        end,
        receive {Tag, _} -> ct:fail(reload_callback_after_failure) after 0 -> ok end
    end, [missing, {read_error, read_failed}, {read_exit, shutdown}]).

continuation_failure(_) ->
    lists:foreach(fun(Phase) ->
        {Grain, Id, Pid} = grain(),
        Monitor = monitor(process, Pid),
        case Phase of
            success -> ok;
            _ -> {ok, _} = write(Id, #{value => winner}, <<"version:1">>)
        end,
        ?assertExit({{save_continuation_failed, exit, shutdown, _}, _},
            actions(Grain, #{value => saved}, fun(_From) ->
                [{save_state, fun
                    (ok) -> exit(shutdown);
                    ({error, saved_etag_changed}) when Phase =:= reload ->
                        {reload, fun(_) -> exit(shutdown) end};
                    ({error, saved_etag_changed}) -> exit(shutdown)
                end}]
            end)),
        receive {'DOWN', Monitor, process, Pid, {save_continuation_failed, exit, shutdown, _}} -> ok
        after 1000 -> ct:fail(continuation_failure_survived)
        end,
        Value = case Phase of success -> saved; _ -> winner end,
        ?assertEqual({ok, #{value => Value}, <<"version:2">>}, read(Id))
    end, [success, failure, reload]).

reload_serialization(_) ->
    {Grain, Id, Pid} = grain(),
    {ok, _} = write(Id, #{value => winner}, <<"version:1">>),
    Test = self(),
    Tag = make_ref(),
    {Caller, Monitor} = spawn_monitor(fun() ->
        ?assertEqual(recovered, actions(Grain, #{value => rejected}, fun(From) ->
            [{save_state, fun({error, saved_etag_changed}) ->
                {reload, fun(_) ->
                    Test ! {Tag, reloading},
                    receive {Tag, resume} -> ok end,
                    {continue, [{reply, From, recovered}]}
                end}
            end}]
        end))
    end),
    receive {Tag, reloading} -> ok after 1000 -> ct:fail(reload_not_started) end,
    {Reader, ReaderMonitor} = spawn_monitor(fun() ->
        Test ! {Tag, read, erleans_grain:call(Pid, state)}
    end),
    ?UNTIL(begin
        {messages, Messages} = process_info(Pid, messages),
        length([ok || {'$gen_call', _, _} <- Messages]) =:= 1
    end),
    Pid ! {Tag, resume},
    down(Caller, Monitor, normal),
    receive {Tag, read, State} -> ?assertEqual(#{value => winner}, State)
    after 1000 -> ct:fail(reader_did_not_finish)
    end,
    down(Reader, ReaderMonitor, normal),
    ?assertEqual(Pid, erleans_grain_registry:whereis_name(Grain)).

duplicate_save(_) ->
    {Grain, Id, Pid} = grain(),
    Monitor = monitor(process, Pid),
    ?assertExit({{bad_action, multiple_save_state}, _},
        actions(Grain, #{value => rejected}, fun(_) ->
            [save_state, {save_state, fun(ok) -> {continue, []} end}]
        end)),
    down(Pid, Monitor, {bad_action, multiple_save_state}),
    ?assertEqual({ok, #{value => a}, <<"version:1">>}, read(Id)).

asynchronous_callbacks(_) ->
    lists:foreach(fun(Kind) ->
        {Grain, Id, Pid} = grain(),
        {ok, _} = write(Id, #{value => winner}, <<"version:1">>),
        Test = self(),
        Tag = make_ref(),
        Actions = [{save_state, fun({error, saved_etag_changed}) ->
            {reload, fun(_) -> {continue, [{reply, {Test, Tag}, recovered}]} end}
        end}],
        Msg = {actions, #{value => rejected}, Actions},
        case Kind of
            cast -> erleans_grain:cast(Grain, Msg);
            info -> Pid ! Msg
        end,
        receive {Tag, recovered} -> ok after 1000 -> ct:fail(no_async_recovery) end,
        ?assertEqual(#{value => winner}, erleans_grain:call(Pid, state))
    end, [cast, info]).

grain() ->
    Id = etag_test_grain:test_key(undefined),
    Ref = erleans:get_grain(etag_test_grain, Id),
    Grain = Ref#{provider => {opaque_etag_provider, in_memory}},
    ?assertEqual(a, erleans_grain:call(Grain, get)),
    {Grain, Id, erleans_grain_registry:whereis_name(Grain)}.

actions(Grain, Candidate, Fun) ->
    erleans_grain:call(Grain, {actions, Candidate, Fun}).

wrap(plain, _, Persistent) -> Persistent;
wrap(ephemeral, Ephemeral, Persistent) -> {Ephemeral, Persistent}.

read(Id) -> opaque_etag_provider:read(etag_test_grain, in_memory, Id).
write(Id, State, ETag) -> opaque_etag_provider:update(etag_test_grain, in_memory, Id, State, ETag).

down(Pid, Monitor, Reason) ->
    receive {'DOWN', Monitor, process, Pid, Actual} -> ?assertEqual(Reason, Actual)
    after 1000 -> ct:fail(process_did_not_exit)
    end.
