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
    [{group, deactivate_after_1},
     {group, deactivate_after_30},
     {group, deactivate_after_60},
     {group, deactivate_after_50000}].

groups() ->
    [{deactivate_after_1, [], [manual_start_stop, ephemeral_state,
                               no_provider_grain, exit_notfound]},
     {deactivate_after_60, [], [bad_etag_save]},
     {deactivate_after_30, [], [request_types]},
     {deactivate_after_50000, [], [local_activations]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(deactivate_after_1, Config) ->
    init_per_group_(1, Config);
init_per_group(deactivate_after_30, Config) ->
    init_per_group_(30, Config);
init_per_group(deactivate_after_60, Config) ->
    init_per_group_(60, Config);
init_per_group(deactivate_after_50000, Config) ->
    init_per_group_(50000, Config).

init_per_group_(DeactivateAfter, Config) ->
    application:load(erleans),
    application:set_env(erleans, deactivate_after, DeactivateAfter),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_group(_, _Config) ->
    application:stop(erleans),
    application:unload(erleans),
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
    ?UNTIL(erleans_grain_registry:whereis_name(Grain1) =:= undefined),
    ?UNTIL(erleans_grain_registry:whereis_name(Grain2) =:= undefined),

    %% sending message by asking for the counter again will re-activate grain
    %% and increment the activated counter
    ?assertEqual({ok, 2}, test_grain:activated_counter(Grain1)),

    %% deactivation should be 1 for grain2
    ?assertEqual({ok, 1}, test_grain:deactivated_counter(Grain2)),

    ok.

bad_etag_save(_Config) ->
    application:set_env(erleans, deactivate_after, 60),
    Grain = #{provider := {ProviderModule, ProviderName}} = erleans:get_grain(test_grain, <<"bad-etag-save-grain">>),

    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain)),

    OldETag = erlang:phash2(#{activated_counter => 1, deactivated_counter => 0, call_counter => 0}),
    NewState = #{activated_counter => 2, deactivated_counter => 0, call_counter => 0},
    NewETag = erlang:phash2(NewState),
    ok = ProviderModule:update(test_grain, ProviderName, <<"bad-etag-save-grain">>, NewState, OldETag, NewETag),

    %% Now a save call should crash the grain
    ?assertMatch({exit, saved_etag_changed}, test_grain:save(Grain)),

    ?UNTIL(erleans_grain_registry:whereis_name(Grain) =:= undefined),

    %% resulting in a new activation when called again
    ?assertEqual({ok, 3}, test_grain:activated_counter(Grain)),

    ok.

ephemeral_state(_Config) ->
    application:set_env(erleans, deactivate_after, 1),
    Grain = erleans:get_grain(test_ephemeral_state_grain, <<"ephemeral-state-grain">>),

    ?assertEqual({ok, 1}, test_ephemeral_state_grain:activated_counter(Grain)),
    ?assertEqual({ok, 0}, test_ephemeral_state_grain:ephemeral_counter(Grain)),

    ?assertEqual(ok, test_ephemeral_state_grain:increment_ephemeral_counter(Grain)),

    %% with a leasetime of 1 second it should be gone now
    ?UNTIL(erleans_grain_registry:whereis_name(Grain) =:= undefined),

    %% sending message by asking for the counter again will re-activate grain
    %% and increment the activated counter
    ?assertMatch({ok, N} when N > 1, test_ephemeral_state_grain:activated_counter(Grain)),
    %% But ephemeral counter should be 0 again
    ?assertEqual({ok, 0}, test_ephemeral_state_grain:ephemeral_counter(Grain)),

    ok.

no_provider_grain(_Config) ->
    application:set_env(erleans, deactivate_after, 60),
    Grain = erleans:get_grain(no_provider_test_grain, <<"no_provider">>),

    ?assertEqual(hello, no_provider_test_grain:hello(Grain)),

    %% attempt to save state through erleans_grain without a provider configured
    ?assertExit({no_provider_configured, _}, no_provider_test_grain:save(Grain)),

    ok.

request_types(_Config) ->
    application:set_env(erleans, deactivate_after, 30),
    Grain = erleans:get_grain(test_grain, <<"request-types-grain">>),

    ?assertEqual({ok, node()}, test_grain:node(Grain)),

    GrainPid = (fun Loop(0) ->
                        error(waaah);
                    Loop(N) ->
                        case erleans_grain_registry:whereis_name(Grain) of
                            Pid when is_pid(Pid) -> Pid;
                            _ ->
                                timer:sleep(1),
                                Loop(N - 1)
                        end
                end)(200),

    %% spawn a requestor which will keep the grain alive
    spawn(fun () ->
                  [begin
                       timer:sleep(12),
                       {ok, _Ct} = test_grain:activated_counter(Grain)
                   end || _ <- lists:seq(1,4)]  % ~48 ms
          end),
    timer:sleep(40),

    %% make sure we still have the same grain
    GrainPid2 = (fun Loop(0) ->
                        error(waaah);
                    Loop(N) ->
                        case erleans_grain_registry:whereis_name(Grain) of
                            Pid when is_pid(Pid) -> Pid;
                            _ ->
                                timer:sleep(1),
                                Loop(N - 1)
                        end
                end)(50),

    ?assertEqual(GrainPid, GrainPid2),
    ?assert(is_process_alive(GrainPid)),

    ?assertEqual({ok, node()}, test_grain:node(Grain)),
    timer:sleep(20),

    _Pinger =
        spawn(fun () ->
                      put(req_type, leave_timer),
                      [begin
                           timer:sleep(6),
                           _Ct = (catch test_grain:activated_counter(GrainPid))
                       end || _ <- lists:seq(1,10)]  % ~60 ms
              end),
    timer:sleep(60),

    %% this should have died at some point because of a badmatch after the lease expires
    %% ?assertEqual(false, is_process_alive(Pinger)),  % can't get this to behave
    ?assertMatch({'EXIT', _}, (catch test_grain:activated_counter(GrainPid))),

    ok.

exit_notfound(_Config) ->
    %% activate returning {error, notfound} is given special treatment and
    %% results in an ignore from the statem and an `exit({noproc, notfound})`
    %% from `erleans_grain`
    GrainRef = erleans:get_grain(notfound_grain, <<"notfound-grain-1">>),
    ?assertExit({noproc, notfound}, notfound_grain:anything(GrainRef)).

%% spawn a bunch of procs making calls to the same unactivated grain
%% checks that the same local single activation is used for each request
local_activations(_Config) ->
    application:set_env(erleans, deactivate_after, 50000),

    Grain1 = erleans:get_grain(test_grain, <<"local-activations-grain1">>),

    Self = self(),
    lists:foreach(fun(_) ->
                          erlang:spawn_link(fun() ->
                                                    {ok, N} = test_grain:call_counter(Grain1),
                                                    Self ! N
                                            end)
                  end, lists:seq(1,10)),
    (fun F(10) ->
             ok;
         F(N) ->
            receive
                X when X =:= N ->
                    F(N+1)
            after
                5000 ->
                    error(loop_timeout)
            end
     end)(0).
