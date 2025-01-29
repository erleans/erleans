%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(grain_timer_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("test_utils.hrl").

-define(g, timer_test_grain).

all() ->
    [{group, defaults},
     {group, deactivate_after_30}].

groups() ->
    [{defaults, [], [single_timer, multiple_timers, crashy_timer]},
     {deactivate_after_30, [], [timer_shutdown]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(defaults, Config) ->
    application:load(erleans),
    {ok, _} = application:ensure_all_started(erleans),
    Config;
init_per_group(deactivate_after_30, Config) ->
    application:load(erleans),
    application:set_env(erleans, deactivate_after, 30),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_group(_, _Config) ->
    application:stop(erleans),
    application:unload(erleans),
    ok.

single_timer(_Config) ->
    Grain = erleans:get_grain(?g, <<"timer-test-grain">>),
    ?g:start_one_timer(Grain),
    timer:sleep(50),
    {ok, Acc} = ?g:clear(Grain),
    ?assertEqual([a, a, a, a, a], Acc),
    timer:sleep(50),
    ?g:cancel_one_timer(Grain),
    timer:sleep(50),
    {ok, Acc1} = ?g:clear(Grain),
    ?assertEqual([a, a, a, a, a], Acc1),
    ?g:start_one_timer(Grain),
    timer:sleep(50), % acc should be [a, a, a, a, a]
    Pid = erleans_grain_registry:whereis_name(Grain),
    ok = ?g:stop(Grain), % but should clear when it stops
    (fun Loop() ->
             case is_process_alive(Pid) of
                 false -> ok;
                 _ -> timer:sleep(20), Loop()
             end
     end)(),
    ?assertMatch({ok, _Node}, ?g:node(Grain)),  % reactivate the grain
    ?assertNotEqual(Pid, erleans_grain_registry:whereis_name(Grain)),
    ?assertEqual({ok, []}, ?g:clear(Grain)),
    ok.

multiple_timers(_Config) ->
    Grain = erleans:get_grain(timer_test_grain, <<"multiple-timer-test-grain">>),
    ?g:start_timers(Grain),
    timer:sleep(40),
    {ok, Acc} = ?g:clear(Grain),
    ?assertEqual([b, a, c, b, c, b, c, b],
                 lists:reverse(Acc)),
    timer:sleep(42),
    {ok, Acc1} = ?g:clear(Grain),
    ?assertEqual([c, b, c, b, c, b, c, b, c], % make sure a doesn't recur
                 lists:reverse(Acc1)),
    ?g:cancel_timers(Grain),
    timer:sleep(40),
    {ok, Acc2} = ?g:clear(Grain),
    ?assertEqual([], Acc2),
    ok.


crashy_timer(_Config) ->
    Grain = erleans:get_grain(timer_test_grain, <<"crashy-timer-test-grain">>),
    ?g:crashy_timer(Grain),
    timer:sleep(50),
    {ok, Acc} = ?g:clear(Grain),
    ?assertEqual([a, a, a, a, a, {erleans_timer_error,exit,boom}],
                 lists:reverse(Acc)),
    ok.

timer_shutdown(_Config) ->
    Grain = erleans:get_grain(timer_test_grain, <<"shutdown-timer-test-grain">>),

    ?g:long_timer(Grain),

    timer:sleep(80),  % sleep till we should be way shut down, but timer should keep us awake

    %% recover
    ?g:node(Grain),
    timer:sleep(15),

    {ok, Acc0} = ?g:clear(Grain),
    Acc = lists:reverse(Acc0),

    {Calls, Pids} = lists:unzip(Acc),
    %% two short before that timer gets cancelled, then a long and a
    %% short from the restarted timer
    ?assertEqual([short, short, long, short], Calls),
    %% we should have 3 pids here, two for the short, one for the
    %% long.  If we waited long enough for another long, there would
    %% be four
    ?assertEqual(3, length(lists:usort(Pids))),

    ok.
