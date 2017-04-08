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
    [simple_subscribe].

init_per_suite(Config) ->
    code:ensure_loaded(stream_test_grain),
    code:ensure_loaded(erleans_consumer_grain),
    application:load(erleans),
    application:set_env(erleans, default_lease_time, 60000),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

simple_subscribe(_Config) ->
    Grain1 = erleans:get_grain(stream_test_grain, <<"simple-subscribe-grain1">>),
    ?UNTIL(stream_test_grain:records_read(Grain1) >= 3),
    ok.
