%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(grain_streams_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("test_utils.hrl").

all() ->
    [no_stream_config_check,
     simple_subscribe,
     grain_wakeup
    ].

init_per_suite(Config) ->
    application:ensure_all_started(pgsql),
    application:ensure_all_started(vonnegut),
    Config.

end_per_suite(_Config) ->
    application:stop(vonnegut),
    application:stop(pgsql),
    ok.

init_per_testcase(no_stream_config_check, Config) ->
    application:load(erleans),
    application:unset_env(erleans, stream_providers),
    application:set_env(erleans, deactivate_after, 60000),
    {ok, _} = application:ensure_all_started(erleans),
    Config;
init_per_testcase(grain_wakeup, Config) ->
    init(500),
    Config;
init_per_testcase(_, Config) ->
    init(60000),
    Config.

init(LeaseTime) ->
    application:load(erleans),
    application:set_env(erleans, deactivate_after, LeaseTime),
    {ok, _} = application:ensure_all_started(erleans).

end_per_testcase(_, _Config) ->
    application:stop(erleans),
    application:unload(erleans),
    ok.

%% regression test for stream supervisor not starting if not configured: #27
no_stream_config_check(_Config) ->
    ?assertEqual(undefined, whereis(erleans_streams_sup)).

simple_subscribe(_Config) ->
    Topic = <<"simple-subscribe-topic">>,
    Grain1 = erleans:get_grain(stream_test_grain, <<"simple-subscribe-grain1">>),
    Grain2 = erleans:get_grain(stream_test_grain, <<"simple-subscribe-grain2">>),
    Grain3 = erleans:get_grain(stream_test_grain, <<"simple-subscribe-grain3">>),
    ok = stream_test_grain:subscribe(Grain1, Topic),
    RcrdList = lists:duplicate(5 + rand:uniform(100), <<"repeated simple record">>),
    erleans_test_utils:produce(Topic, RcrdList),
    ct:pal("grain1 ~p~n~p", [Grain1, erleans_pm:whereis_name(Grain1)]),
    ?UNTIL(stream_test_grain:records_read(Grain1) >= length(RcrdList)),

    %% test joining afterwards
    ok = stream_test_grain:subscribe(Grain2, Topic),
    ?UNTIL(stream_test_grain:records_read(Grain2) >= length(RcrdList)),

    %% test joining not at 0
    ok = stream_test_grain:subscribe(Grain3, Topic, 5),
    ?UNTIL(stream_test_grain:records_read(Grain3) == length(RcrdList) - 5),
    stream_test_grain:unsubscribe(Grain1, Topic),
    stream_test_grain:unsubscribe(Grain2, Topic),
    stream_test_grain:unsubscribe(Grain3, Topic),
    ok.

grain_wakeup(_Config) ->
    Topic = <<"wakeup-topic">>,
    Grain1 = erleans:get_grain(stream_test_grain, <<"wakeup-grain">>),
    RcrdList = lists:duplicate(10 + rand:uniform(100), <<"repeated wakeup record">>),
    erleans_test_utils:produce(Topic, RcrdList),
    ok = stream_test_grain:subscribe(Grain1, Topic),
    {ok, GrainPid} =
        (fun Loop(0) ->
                 {error, took_too_long};
             Loop(N) ->
                 R = stream_test_grain:records_read(Grain1),
                 ct:pal("R ~p", [R]),
                 case erleans_pm:whereis_name(Grain1) of
                     Pid when is_pid(Pid)-> {ok, Pid};
                     _ -> timer:sleep(10), Loop(N -1)
                 end
         end)(30),
    %% make sure we've gotten everything so far
    Len = length(RcrdList),
    ?until_match(Len, stream_test_grain:records_read(Grain1), 5),
    %% reset count and wait till the grain goes away
    stream_test_grain:reset(Grain1),
    ?UNTIL(erleans_pm:whereis_name(Grain1) =:= undefined andalso
           is_process_alive(GrainPid) =:= false),

    %% send some new stuff!
    RcrdList2 = lists:duplicate(10 + rand:uniform(100), <<"repeated wakeup record 2">>),
    Len2 = length(RcrdList2),
    ct:pal("doing produce 2 with ~p records", [Len2]),
    erleans_test_utils:produce(Topic, RcrdList2),
    {ok, _GrainPid1} =
        (fun Loop(0) ->
                 {error, took_too_long};
             Loop(N) ->
                 case erleans_pm:whereis_name(Grain1) of
                     Pid when is_pid(Pid)-> {ok, Pid};
                     _ -> timer:sleep(50), Loop(N -1)
                 end
         end)(20 * 5), % 5s
    %% make sure that the grain actually wakes up
    ?UNTIL(begin Ret = erleans_pm:whereis_name(Grain1),
                 Ret =/= undefined andalso Ret =/= GrainPid
           end),
    Len2 = length(RcrdList2),
    ct:pal("len ~p len2 ~p", [Len, Len2]),
    %% just Len2 because the we've reset
    ?until_match(Len2, stream_test_grain:records_read(Grain1), 5),


    %% test unsubscribe and make sure that we don't wake up again
    stream_test_grain:unsubscribe(Grain1, Topic),
    %% wait till the grain goes away
    ?UNTIL(erleans_pm:whereis_name(Grain1) =:= undefined andalso
           is_process_alive(GrainPid) =:= false),

    erleans_test_utils:produce(Topic, RcrdList2),

    %% negative test, 500ms
    {error, took_too_long} =
        (fun Loop(0) ->
                 {error, took_too_long};
             Loop(N) ->
                 case erleans_pm:whereis_name(Grain1) of
                     Pid when is_pid(Pid)-> {ok, Pid};
                     _ -> timer:sleep(50), Loop(N -1)
                 end
         end)(10),

    ok.
