-module(grain_placement_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         stateful_placements/1, stateless_normalization/1, unsupported_placement/1]).

-include_lib("eunit/include/eunit.hrl").

all() -> [stateful_placements, stateless_normalization, unsupported_placement].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_) ->
    application:stop(erleans),
    ok.

stateful_placements(_) ->
    lists:foreach(fun(Placement) ->
        ok = erleans_config:set(test_placement, Placement),
        Grain = erleans:get_grain(placement_test_grain, make_ref()),
        ?assertEqual(Placement, maps:get(placement, Grain)),
        assert_activation(Grain)
    end, [prefer_local, random]).

stateless_normalization(_) ->
    ok = erleans_config:set(test_placement, stateless),
    Default = erleans:get_grain(placement_test_grain, make_ref()),
    ?assertEqual({stateless, 5}, maps:get(placement, Default)),
    assert_activation(Default),
    ok = erleans_config:set(default_stateless_max, 2),
    Configured = erleans:get_grain(placement_test_grain, make_ref()),
    ?assertEqual({stateless, 2}, maps:get(placement, Configured)),
    assert_activation(Configured),
    ok = erleans_config:set(test_placement, {stateless, 3}),
    Explicit = erleans:get_grain(placement_test_grain, make_ref()),
    ?assertEqual({stateless, 3}, maps:get(placement, Explicit)),
    assert_activation(Explicit).

unsupported_placement(_) ->
    lists:foreach(fun(Placement) ->
        ok = erleans_config:set(test_placement, Placement),
        ?assertError({invalid_placement, Placement},
                     erleans:get_grain(placement_test_grain, make_ref()))
    end, [system_grain, unsupported]).

assert_activation(Grain) ->
    ?assertEqual(undefined, erleans_grain_registry:whereis_name(Grain)),
    Pid = erleans_grain:call(Grain, pid),
    true = is_pid(Pid),
    ?assertEqual(Pid, erleans_grain:call(Grain, pid)),
    ?assertEqual(Pid, erleans_grain_registry:whereis_name(Grain)),
    ok = gen_statem:stop(Pid, shutdown, infinity),
    ?assertEqual(undefined, erleans_grain_registry:whereis_name(Grain)).
