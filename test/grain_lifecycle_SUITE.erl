%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(grain_lifecycle_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("test_utils.hrl").

all() ->
    [manual_start_stop, bad_etag_save, ephemeral_state].

init_per_suite(Config) ->
    application:load(erleans),
    %% set a really low lease time for testing deactivations
    application:set_env(erleans, default_lease_time, 1),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

manual_start_stop(_Config) ->
    Grain1 = erleans:get_grain(test_grain, <<"manual-start-stop-grain1">>),
    Grain2 = erleans:get_grain(test_grain, <<"manual-start-stop-grain2">>),

    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain2)),

    %% with a leasetime of 1 second it should be gone now
    ?UNTIL(erleans_pm:whereis_name(Grain1) =:= undefined),
    ?UNTIL(erleans_pm:whereis_name(Grain2) =:= undefined),

    %% sending message by asking for the counter again will re-activate grain
    %% and increment the activated counter
    ?assertEqual({ok, 2}, test_grain:activated_counter(Grain1)),

    %% deactivation should be 1 for grain2
    ?assertEqual({ok, 1}, test_grain:deactivated_counter(Grain2)),

    ok.

bad_etag_save(_Config) ->
    application:set_env(erleans, default_lease_time, 60),
    Grain = erleans:get_grain(test_grain, <<"bad-etag-save-grain">>),

    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain)),

    NewState = #{activated_counter => 2, deactivated_counter => 0},
    NewETag = erlang:phash2(NewState),
    ets_provider:insert(test_grain, <<"bad-etag-save-grain">>, NewState, NewETag),

    %% Now a save call should crash the grain
    ?assertMatch({exit, saved_etag_changed}, test_grain:save(Grain)),

    ?UNTIL(erleans_pm:whereis_name(Grain) =:= undefined),

    %% resulting in a new activation when called again
    ?assertEqual({ok, 3}, test_grain:activated_counter(Grain)),

    ok.

ephemeral_state(_Config) ->
    application:set_env(erleans, default_lease_time, 1),
    Grain = erleans:get_grain(test_ephemeral_state_grain, <<"ephemeral-state-grain">>),

    ?assertEqual({ok, 1}, test_ephemeral_state_grain:activated_counter(Grain)),
    ?assertEqual({ok, 0}, test_ephemeral_state_grain:ephemeral_counter(Grain)),

    ?assertEqual(ok, test_ephemeral_state_grain:increment_ephemeral_counter(Grain)),

    %% with a leasetime of 1 second it should be gone now
    ?UNTIL(erleans_pm:whereis_name(Grain) =:= undefined),

    %% sending message by asking for the counter again will re-activate grain
    %% and increment the activated counter
    ?assertEqual({ok, 2}, test_ephemeral_state_grain:activated_counter(Grain)),
    %% But ephemeral counter should be 0 again
    ?UNTIL({ok, 0} =:= test_ephemeral_state_grain:ephemeral_counter(Grain)),

    ok.
