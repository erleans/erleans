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
    [no_stream_config_check, simple_subscribe].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(no_stream_config_check, Config) ->
    application:ensure_all_started(pgsql),
    application:load(erleans),
    application:unset_env(erleans, stream_providers),
    application:set_env(erleans, default_lease_time, 60000),
    {ok, _} = application:ensure_all_started(erleans),
    Config;
init_per_testcase(simple_subscribe, Config) ->
    application:load(erleans),
    application:set_env(erleans, default_lease_time, 60000),
    application:ensure_all_started(pgsql),
    application:ensure_all_started(vonnegut),
    {ok, _} = application:ensure_all_started(erleans),
    RcrdList = lists:duplicate(5 + rand:uniform(100), <<"repeated record">>),
    case lists:keyfind(vonnegut, 1, application:loaded_applications()) of
        false ->
            test_stream:produce([{<<"simple-subscribe-grain1-some-topic">>, RcrdList}]);
        _ ->
            ok = vg_client_pool:start(),
            %%vg_client:ensure_topic(<<"simple-subscribe-grain1-some-topic">>),
            vg_client:produce(<<"simple-subscribe-grain1-some-topic">>, RcrdList)
    end,
    [{len, length(RcrdList)} | Config];
init_per_testcase(_, Config) ->
    application:ensure_all_started(pgsql),
    application:ensure_all_started(vonnegut),
    application:load(erleans),
    application:set_env(erleans, default_lease_time, 60000),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_testcase(_, _Config) ->
    application:stop(erleans),
    application:unload(erleans),
    application:stop(pgsql),
    application:stop(vonnegut),
    ok.

simple_subscribe(Config) ->
    Grain1 = erleans:get_grain(stream_test_grain, <<"simple-subscribe-grain1">>),
    ct:pal("grain1 ~p~n~p", [Grain1, erleans_pm:whereis_name(Grain1)]),
    ?UNTIL(stream_test_grain:records_read(Grain1) >= ?config(len, Config)),
    ok.

%% regression test for stream supervisor not starting if not configured: #27
no_stream_config_check(_Config) ->
    ?assertEqual(undefined, whereis(erleans_streams_sup)).
