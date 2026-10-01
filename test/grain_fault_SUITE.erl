-module(grain_fault_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         callback_exceptions/1, queued_calls/1, stateful_reroute/1,
         stateless_reroute/1, direct_pid_no_retry/1, timeout_no_retry/1,
         retry_limit/1, retry_deadline/1, remaining_timeout/1, pool_timeout/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("erleans.hrl").

all() ->
    [callback_exceptions, queued_calls, stateful_reroute, stateless_reroute,
     direct_pid_no_retry, timeout_no_retry, retry_limit, retry_deadline,
     remaining_timeout, pool_timeout].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_) ->
    application:stop(erleans),
    ok.

callback_exceptions(_) ->
    lists:foreach(fun(Placement) ->
        Grain = grain(Placement),
        Pid = erleans_grain:call(Grain, pid),
        true = is_pid(Pid),
        ?assertEqual(1, erleans_grain:call(Grain, increment)),
        lists:foreach(fun({Class, Reason}) ->
            Tag = make_ref(),
            ?assertException(Class, Reason,
                erleans_grain:call(Grain, {raise, self(), Tag, Class, Reason})),
            receive {Tag, invoked, Pid} -> ok after 1000 -> ct:fail(callback_not_invoked) end,
            ?assertEqual(Pid, erleans_grain:call(Grain, pid)),
            receive {Tag, invoked, _} -> ct:fail(callback_replayed) after 0 -> ok end
        end, [{error, badarg}, {throw, thrown}, {exit, shutdown},
              {exit, {shutdown, deactivated}}, {exit, noconnection},
              {exit, {noproc, {gen_statem, call, []}}}, {exit, bad_etag},
              {exit, {bad_etag, old, new}}]),
        ?assertEqual(2, erleans_grain:call(Grain, increment)),
        %% Ordinary reply data cannot be confused with an exception envelope.
        Reply = {'$erleans_callback_error', make_ref(), error, data, []},
        ?assertEqual(Reply, erleans_grain:call(Grain, {reply, Reply})),
        erleans_grain:cast(Grain, crash),
        ?assertEqual(Pid, erleans_grain:call(Grain, pid)),
        Pid ! crash,
        ?assertEqual(Pid, erleans_grain:call(Grain, pid))
    end, [prefer_local, {stateless, 1}]).

queued_calls(_) ->
    Grain = grain(prefer_local),
    Pid = erleans_grain:call(Grain, pid),
    true = is_pid(Pid),
    Test = self(),
    Tag = make_ref(),
    {BadCaller, BadMonitor} = spawn_monitor(fun() ->
        ?assertError(callback_crash, erleans_grain:call(Grain, {block_then_raise, Test, Tag}))
    end),
    receive {Tag, blocked, Pid} -> ok after 1000 -> ct:fail(callback_not_blocked) end,
    Callers = [spawn_monitor(fun() -> Test ! {Tag, result, erleans_grain:call(Grain, increment)} end)
               || _ <- lists:seq(1, 3)],
    await_queue(Pid, 3, 100),
    Pid ! {Tag, release},
    normal_down(BadCaller, BadMonitor),
    Results = [receive {Tag, result, N} -> N after 1000 -> ct:fail(queued_call_failed) end
               || _ <- Callers],
    ?assertEqual([1, 2, 3], lists:sort(Results)),
    [normal_down(P, M) || {P, M} <- Callers],
    ?assertEqual(Pid, erleans_grain:call(Grain, pid)).

stateful_reroute(_) -> reroute(prefer_local).
stateless_reroute(_) -> reroute({stateless, 1}).

reroute(Placement) ->
    lists:foreach(fun(Reason) ->
        Grain = grain(Placement),
        Pid = stand_in(Grain, Reason),
        NewPid = erleans_grain:call(Grain, pid),
        ?assert(is_pid(NewPid)),
        ?assertNotEqual(Pid, NewPid),
        ?assertNot(is_process_alive(Pid))
    end, [normal, noproc, shutdown, {shutdown, deactivated}, {shutdown, replacement},
          {nodedown, 'lost@node'}, noconnection]).

direct_pid_no_retry(_) ->
    Grain = grain(prefer_local),
    Pid = stand_in(Grain, shutdown),
    ?assertExit({shutdown, {gen_statem, call, _}}, erleans_grain:call(Pid, pid)),
    ?assertEqual(undefined, erleans_grain_registry:whereis_name(Grain)).

timeout_no_retry(_) ->
    Grain = grain(prefer_local),
    Pid = erleans_grain:call(Grain, pid),
    true = is_pid(Pid),
    Tag = make_ref(),
    try
        ?assertExit({timeout, {gen_statem, call, _}},
                    erleans_grain:call(Grain, {block_on, node(), self(), Tag}, 20)),
        receive {Tag, blocked, Pid} -> ok after 1000 -> ct:fail(callback_not_invoked) end
    after
        Pid ! {Tag, release}
    end,
    ?assertEqual(Pid, erleans_grain:call(Grain, pid)),
    receive {Tag, blocked, _} -> ct:fail(timeout_replayed) after 0 -> ok end.

retry_limit(_) ->
    %% No local supervisor: retries must finish even with an infinite timeout.
    ok = application:stop(erleans),
    try
        Grain = grain(prefer_local),
        Start = erlang:monotonic_time(millisecond),
        ?assertExit({noproc, {gen_server, call, _}}, erleans_grain:call(Grain, pid, infinity)),
        ?assert(erlang:monotonic_time(millisecond) - Start < 1000)
    after
        {ok, _} = application:ensure_all_started(erleans)
    end.

retry_deadline(_) ->
    Grain = grain(prefer_local),
    Pid = stand_in(Grain, shutdown),
    %% With no time left for another attempt, preserve the transport failure.
    ?assertExit({shutdown, {gen_statem, call, _}}, erleans_grain:call(Grain, pid, 10)),
    ?assertNot(is_process_alive(Pid)).

remaining_timeout(_) ->
    Grain = grain(prefer_local),
    _ = stand_in(Grain, shutdown, 50),
    Tag = make_ref(),
    try erleans_grain:call(Grain, {block_on, node(), self(), Tag}, 200) of
        _ -> ct:fail(expected_timeout)
    catch
        exit:{timeout, {gen_statem, call, [_, _, Remaining]}} ->
            ?assert(Remaining < 200)
    after
        receive {Tag, blocked, Pid} -> Pid ! {Tag, release}
        after 1000 -> ct:fail(retry_not_dispatched)
        end
    end.

pool_timeout(_) ->
    Grain = grain({stateless, 1}),
    Pid = erleans_grain:call(Grain, pid),
    true = is_pid(Pid),
    Test = self(),
    Tag = make_ref(),
    {Caller, Monitor} = spawn_monitor(fun() ->
        erleans_grain:call(Grain, {block_on, node(), Test, Tag})
    end),
    receive {Tag, blocked, Pid} -> ok after 1000 -> ct:fail(worker_not_busy) end,
    try
        Start = erlang:monotonic_time(millisecond),
        ?assertExit(timeout, erleans_grain:call(Grain, pid, 20)),
        %% A short timeout must not incur the default one-second pool wait.
        ?assert(erlang:monotonic_time(millisecond) - Start < 500)
    after
        Pid ! {Tag, release}
    end,
    normal_down(Caller, Monitor).

grain(Placement) ->
    Ref = erleans:get_grain(fault_test_grain, make_ref()),
    Ref#{placement => Placement}.

%% A selected worker which dies as a call arrives, before producing a reply.
%% This exercises the real gen_statem exit wrapping and routing/pool cleanup.
stand_in(Grain, Reason) ->
    stand_in(Grain, Reason, 0).

stand_in(Grain, Reason, Delay) ->
    Test = self(),
    Tag = make_ref(),
    Pid = spawn(fun() ->
        case Grain of
            #{placement := {stateless, _}} ->
                ok = gproc_pool:new(?pool(Grain), claim, [{autosize, true}]),
                gproc_pool:add_worker(?pool(Grain), self()),
                gproc_pool:connect_worker(?pool(Grain), self());
            _ -> yes = global:register_name(Grain, self())
        end,
        Test ! {Tag, ready},
        receive {'$gen_call', _, _} -> timer:sleep(Delay), exit(Reason) end
    end),
    receive {Tag, ready} -> Pid after 1000 -> ct:fail(worker_not_ready) end.

await_queue(_, _, 0) -> ct:fail(calls_not_queued);
await_queue(Pid, Count, Left) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, N} when N >= Count -> ok;
        _ -> timer:sleep(5), await_queue(Pid, Count, Left - 1)
    end.

normal_down(Pid, Monitor) ->
    receive {'DOWN', Monitor, process, Pid, normal} -> ok
    after 1000 -> ct:fail(caller_failed)
    end.
