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
    [simple_subscribe, no_stream_config_check].

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
init_per_testcase(_, Config) ->
    application:ensure_all_started(pgsql),
    application:load(erleans),
    application:set_env(erleans, default_lease_time, 60000),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_testcase(_, _Config) ->
    application:stop(erleans),
    application:stop(pgsql),
    ok.

simple_subscribe(_Config) ->
    Grain1 = erleans:get_grain(stream_test_grain, <<"simple-subscribe-grain1">>),
    ?UNTIL(stream_test_grain:records_read(Grain1) >= 3),
    ok.

%% regression test for stream supervisor not starting if not configured: #27
no_stream_config_check(_Config) ->
    ?assertEqual(undefined, whereis(erleans_streams_sup)).
