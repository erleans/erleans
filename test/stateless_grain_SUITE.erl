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

-include("test_utils.hrl").

all() ->
    [limits].

init_per_suite(Config) ->
    code:ensure_loaded(stateless_test_grain),
    application:load(erleans),    
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

limits(_Config) ->
    Grain1 = erleans:get_grain(stateless_test_grain, <<"grain1">>),
    Grain2 = erleans:get_grain(stateless_test_grain, <<"grain2">>),

    ?assertEqual({ok, 1}, stateless_test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, stateless_test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, stateless_test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, stateless_test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, stateless_test_grain:activated_counter(Grain2)),

    ok.
