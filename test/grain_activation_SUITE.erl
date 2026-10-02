-module(grain_activation_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         concurrent_activation/1, activation_failures/1]).

-include_lib("eunit/include/eunit.hrl").

all() -> [concurrent_activation, activation_failures].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_) ->
    application:stop(erleans),
    ok.

concurrent_activation(_) ->
    lists:foreach(fun check_concurrent_activation/1,
                  [{prefer_local, read}, {prefer_local, activate},
                   {{stateless, 1}, read}, {{stateless, 1}, activate}]).

check_concurrent_activation({Placement, Stage}) ->
    Test = self(),
    Tag = make_ref(),
    Grains = [grain(Placement, Tag) || _ <- lists:seq(1, 3)],
    Callers = [spawn_monitor(fun() ->
        Pid = erleans_grain:call(Grain, pid),
        Test ! {Tag, result, Pid}
    end) || Grain <- Grains],
    try
        %% Every read must begin while the others are blocked in initialization.
        Pids = [await_stage(Tag, reading) || _ <- Grains],
        case Stage of
            read -> ok;
            activate ->
                [Pid ! {Tag, read_result, {ok, {Test, Tag}, 1}} || Pid <- Pids],
                ?assertEqual(lists:sort(Pids),
                             lists:sort([await_stage(Tag, activating) || _ <- Grains]))
        end,
        %% The supervisor can also process shutdowns of already active children.
        Healthy = erleans:get_grain(fault_test_grain,
                                    integer_to_binary(erlang:unique_integer([positive]))),
        HealthyPid = erleans_grain:call(Healthy, pid),
        ok = supervisor:terminate_child(erleans_grain_sup, HealthyPid),
        ?assertNot(is_process_alive(HealthyPid)),
        [await_call(Pid, 100) || Pid <- Pids],
        receive {Tag, result, _} -> ct:fail(replied_before_activation)
        after 0 -> ok
        end,
        case Stage of
            read ->
                [Pid ! {Tag, read_result, {ok, {Test, Tag}, 1}} || Pid <- Pids],
                [await_stage(Tag, activating) || _ <- Grains];
            activate -> ok
        end,
        [Pid ! {Tag, activate_result, ok} || Pid <- Pids],
        Results = [receive {Tag, result, Pid} -> Pid
                   after 1000 -> ct:fail(missing_reply)
                   end || _ <- Grains],
        ?assertEqual(lists:sort(Pids), lists:sort(Results)),
        [normal_down(Caller, Monitor) || {Caller, Monitor} <- Callers],
        [gen_statem:stop(Pid) || Pid <- Pids]
    after
        %% Kill any blocked initializations if an assertion fails.
        [exit(Pid, kill) || {_, Pid, _, _} <- supervisor:which_children(erleans_grain_sup),
                            is_pid(Pid)],
        [exit(Caller, kill) || {Caller, _} <- Callers]
    end.

activation_failures(_) ->
    lists:foreach(fun({Placement, Stage, Reason}) ->
        Test = self(),
        Tag = make_ref(),
        Grain = grain(Placement, Tag),
        {Caller, Monitor} = spawn_monitor(fun() ->
            ?assertExit({Reason, {gen_statem, call, _}}, erleans_grain:call(Grain, pid))
        end),
        Pid = await_stage(Tag, reading),
        try
            await_call(Pid, 100),
            case Stage of
                read -> Pid ! {Tag, read_result, {error, Reason}};
                state -> Pid ! {Tag, read_result, {ok, notfound, 1}};
                activate ->
                    Pid ! {Tag, read_result, {ok, {Test, Tag}, 1}},
                    Pid = await_stage(Tag, activating),
                    Pid ! {Tag, activate_result, {error, Reason}}
            end,
            normal_down(Caller, Monitor),
            %% A retry would block on another controlled read and fail above.
            receive {Tag, reading, _} -> ct:fail(activation_retried)
            after 0 -> ok
            end
        after
            exit(Pid, kill),
            exit(Caller, kill)
        end
    end, [{Placement, Stage, Reason}
          || Placement <- [prefer_local, {stateless, 1}],
             {Stage, Reason} <- [{read, read_failed}, {state, notfound},
                                 {activate, notfound}, {activate, activation_failed}]]).

grain(Placement, Tag) ->
    Grain = erleans:get_grain(activation_test_grain,
                             integer_to_binary(erlang:unique_integer([positive]))),
    Grain#{placement => Placement, provider => {activation_test_grain, {self(), Tag}}}.

await_stage(Tag, Stage) ->
    receive {Tag, Stage, Pid} -> Pid
    after 1000 -> ct:fail({initialization_blocked, Stage})
    end.

await_call(_, 0) -> ct:fail(call_not_queued);
await_call(Pid, Attempts) ->
    {messages, Messages} = process_info(Pid, messages),
    case [ok || {'$gen_call', _, _} <- Messages] of
        [] -> timer:sleep(10), await_call(Pid, Attempts - 1);
        _ -> ok
    end.

normal_down(Pid, Monitor) ->
    receive {'DOWN', Monitor, process, Pid, Reason} -> ?assertEqual(normal, Reason)
    after 1000 -> ct:fail(caller_did_not_finish)
    end.
