%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% ---------------------------------------------------------------------------
-module(grain_lifecycle_SUITE).

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,
         manual_start_stop/1,
         bad_etag_save/1,
         ephemeral_state/1,
         no_provider_grain/1,
         request_types/1,
         exit_notfound/1,
         existing_global_registration/1,
         callback_crash_does_not_save/1,
         shutdown_saves_state/1,
         local_activations/1]).

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
     {deactivate_after_50000, [], [local_activations, existing_global_registration,
                                  callback_crash_does_not_save, shutdown_saves_state]}].

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

    {ok, _, OldETag} = ProviderModule:read(test_grain, ProviderName, <<"bad-etag-save-grain">>),
    NewState = #{activated_counter => 2, deactivated_counter => 0, call_counter => 0},
    {ok, _NewETag} = ProviderModule:update(test_grain, ProviderName, <<"bad-etag-save-grain">>, NewState, OldETag),

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
                           try test_grain:activated_counter(GrainPid)
                           catch
                               exit:_ -> ok
                           end
                       end || _ <- lists:seq(1,10)]  % ~60 ms
              end),
    timer:sleep(60),

    ?assertExit(_, test_grain:activated_counter(GrainPid)),

    ok.

exit_notfound(_Config) ->
    %% activate returning {error, notfound} is given special treatment and
    %% results in an ignore from the statem and an `exit({noproc, notfound})`
    %% from `erleans_grain`
    GrainRef = erleans:get_grain(notfound_grain, <<"notfound-grain-1">>),
    ?assertExit({noproc, notfound}, notfound_grain:anything(GrainRef)).

callback_crash_does_not_save(_Config) ->
    lists:foreach(
      fun(Kind) ->
              Grain = #{id := Id, provider := {Provider, Name}} =
                  erleans:get_grain(test_grain, {callback_crash, Kind}),
              ok = test_grain:save(Grain),
              Saved = Provider:read(test_grain, Name, Id),
              ?assertMatch({ok, #{deactivated_counter := 0}, _}, Saved),
              {ok, _} = test_grain:call_counter(Grain),
              Pid = erleans_grain_registry:whereis_name(Grain),
              Monitor = monitor(process, Pid),
              case Kind of
                  call ->
                      ?assertExit({{callback_crash, _}, _}, erleans_grain:call(Grain, crash));
                  cast ->
                      erleans_grain:cast(Grain, crash);
                  info ->
                      Pid ! crash
              end,
              receive {'DOWN', Monitor, process, Pid, {callback_crash, _}} -> ok
              after 1000 -> ct:fail(grain_did_not_crash)
              end,
              ?assertEqual(Saved, Provider:read(test_grain, Name, Id))
      end, [call, cast, info]).

shutdown_saves_state(_Config) ->
    lists:foreach(
      fun(Reason) ->
              Grain = #{id := Id, provider := {Provider, Name}} =
                  erleans:get_grain(test_grain, {shutdown_saves_state, Reason}),
              {ok, 0} = test_grain:call_counter(Grain),
              Pid = erleans_grain_registry:whereis_name(Grain),
              ok = gen_statem:stop(Pid, Reason, infinity),
              ?assertMatch({ok, #{deactivated_counter := 1, call_counter := 1}, _},
                           Provider:read(test_grain, Name, Id))
      end, [shutdown, {shutdown, test}]).

existing_global_registration(_Config) ->
    Grain = erleans:get_grain(test_grain, <<"existing-global-registration">>),
    Owner = self(),
    yes = global:register_name(Grain, Owner),
    try
        ?assertEqual({error, {already_started, Owner}},
                     erleans_grain_sup:start_child(Grain)),
        ?assertEqual(Owner, erleans_grain_registry:whereis_name(Grain))
    after
        global:unregister_name(Grain)
    end.

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
