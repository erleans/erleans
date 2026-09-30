%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% ---------------------------------------------------------------------------
-module(stateless_grain_SUITE).

-export([all/0,
         init_per_suite/1,
         end_per_suite/1,
         single_activation/1,
         concurrent_first_use/1,
         crash_worker/1,
         timeout_no_workers/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("erleans.hrl").
-include("test_utils.hrl").

all() ->
    [single_activation, concurrent_first_use, crash_worker, timeout_no_workers].

init_per_suite(Config) ->
    application:load(erleans),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    ok.

single_activation(_Config) ->
    Grain1 = erleans:get_grain(stateless_test_grain, <<"stateless-test-suite-grain1">>),
    Grain2 = erleans:get_grain(stateless_test_grain, <<"stateless-test-suite-grain2">>),

    %% each call should block until stateless grain is returned to pool
    %% so only 1 grain will ever activate
    ?assertEqual({ok, 1}, stateless_test_grain:call_counter(Grain1)),
    ?assertEqual({ok, 2}, stateless_test_grain:call_counter(Grain1)),
    ?assertEqual({ok, 3}, stateless_test_grain:call_counter(Grain1)),
    ?assertEqual({ok, 4}, stateless_test_grain:call_counter(Grain1)),
    ?assertEqual({ok, 1}, stateless_test_grain:call_counter(Grain2)),

    ok.

concurrent_first_use(_Config) ->
    Grain = erleans:get_grain(stateless_test_grain, <<"concurrent-first-use">>),
    Test = self(),
    PoolServer = whereis(gproc_pool),
    true = is_pid(PoolServer),
    %% Hold pool creation until all callers have observed the missing pool.
    ok = sys:suspend(gproc_pool),
    Callers = try
                  Pids = [spawn_monitor(fun() ->
                                                Result = stateless_test_grain:call_counter(Grain),
                                                Test ! {self(), Result}
                                        end) || _ <- lists:seq(1, 8)],
                  ?UNTIL(begin
                             {messages, Messages} = process_info(PoolServer, messages),
                             Creates = [ok || {'$gen_call', _, {new, Pool, _, _}} <- Messages,
                                              Pool =:= ?pool(Grain)],
                             length(Creates) =:= 8
                         end),
                  Pids
              after
                  sys:resume(gproc_pool)
              end,
    lists:foreach(fun({Pid, Monitor}) ->
                          receive
                              {Pid, {ok, _}} -> ok;
                              {'DOWN', Monitor, process, Pid, Reason} ->
                                  ct:fail({caller_failed, Reason})
                          after 5000 -> ct:fail(caller_did_not_reply)
                          end,
                          receive {'DOWN', Monitor, process, Pid, normal} -> ok
                          after 1000 -> ct:fail(caller_did_not_finish)
                          end
                  end, Callers).

crash_worker(_Config) ->
    Grain1 = erleans:get_grain(stateless_test_grain, <<"stateless-test-suite-grain3">>),

    ?assertEqual({ok, 1}, stateless_test_grain:call_counter(Grain1)),
    spawn_monitor(stateless_test_grain, hold, [Grain1]),
    spawn_monitor(stateless_test_grain, hold, [Grain1]),

    ?assertMatch({ok, _}, stateless_test_grain:call_counter(Grain1)),
    ?assertMatch({ok, _}, stateless_test_grain:call_counter(Grain1)),

    ?assertMatch({ok, N} when N > 1, stateless_test_grain:call_counter(Grain1)),

    {ok, Pid} = stateless_test_grain:pid(Grain1),
    exit(Pid, shutdown),
    ?UNTIL(is_process_alive(Pid) == false),

    ?assertEqual({ok, 1}, stateless_test_grain:call_counter(Grain1)),

    ok.

timeout_no_workers(_Config) ->
    Grain1 = erleans:get_grain(stateless_test_grain, <<"stateless-test-suite-grain4">>),

    ?assertEqual({ok, 1}, stateless_test_grain:call_counter(Grain1)),
    spawn_link(stateless_test_grain, hold, [Grain1]),
    spawn_link(stateless_test_grain, hold, [Grain1]),
    spawn_link(stateless_test_grain, hold, [Grain1]),

    ?UNTIL(3 =:= length(gproc_pool:active_workers(?pool(Grain1)))),

    ?assertExit(timeout, stateless_test_grain:call_counter(Grain1)),
    ok.
