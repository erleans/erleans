%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(stateless_grain_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("erleans.hrl").
-include("test_utils.hrl").

all() ->
    [single_activation, crash_worker, timeout_no_workers].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    application:load(erleans),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    application:stop(pgo),
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

crash_worker(_Config) ->
    Grain1 = erleans:get_grain(stateless_test_grain, <<"stateless-test-suite-grain3">>),

    ?assertEqual({ok, 1}, stateless_test_grain:call_counter(Grain1)),
    spawn_monitor(stateless_test_grain, hold, [Grain1]),
    spawn_monitor(stateless_test_grain, hold, [Grain1]),

    ?assertMatch({ok, _}, stateless_test_grain:call_counter(Grain1)),
    %% ?UNTIL(3 =:= length(gproc_pool:defined_workers(?pool(Grain1)))),
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
