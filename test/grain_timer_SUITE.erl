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
    [single_timer, multiple_timers, crashy_timer].

init_per_suite(Config) ->
    application:ensure_all_started(pgsql),
    application:load(erleans),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    application:stop(pgsql),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
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
    Pid = erleans_pm:whereis_name(Grain),
    ok = ?g:stop(Grain), % but should clear when it stops
    (fun Loop() ->
             case is_process_alive(Pid) of
                 false -> ok;
                 _ -> timer:sleep(20), Loop()
             end
     end)(),
    ?assertMatch({ok, _Node}, ?g:node(Grain)),  % reactivate the grain
    ?assertNotEqual(Pid, erleans_pm:whereis_name(Grain)),
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
