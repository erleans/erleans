%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(dist_lifecycle_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("test_utils.hrl").

-define(NODE_CT, ct@fanon).
-define(NODE_A, a@fanon).

all() ->
    [manual_start_stop].

init_per_suite(Config) ->
    application:load(erleans),
    application:load(partisan),
    application:load(lasp),
    application:set_env(partisan, peer_port, 10200),
    code:ensure_loaded(test_grain),
    {ok, _} = application:ensure_all_started(erleans),
    start_nodes(),
    Config.

end_per_suite(_Config) ->
    {ok, _} = ct_slave:stop(a),
    application:stop(erleans),
    application:stop(lasp_pg),
    application:stop(lasp),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

start_nodes() ->
    Nodes = [{a, 10201}], %, b, c, d],
    start_nodes(Nodes, []).

start_nodes([], Acc) ->
    Acc;
start_nodes([{Node, PeerPort} | T], Acc) ->
    ErlFlags = "-config ../../../../test/sys.config",
    CodePath = code:get_path(),
    {ok, HostNode} = ct_slave:start(Node,
                                    [{kill_if_fail, true},
                                     {monitor_master, true},
                                     {init_timeout, 3000},
                                     {startup_timeout, 3000},
                                     {startup_functions,
                                      [{code, set_path, [CodePath]},
                                       {application, load, [partisan]},
                                       {application, set_env, [partisan, peer_port, PeerPort]},
                                       {application, load, [erleans]},
                                       {code, ensure_loaded, [test_grain]},
                                       {application, ensure_all_started, [erleans]}]},
                                     {erl_flags, ErlFlags}]),
    ct:print("\e[32m Node ~p [OK] \e[0m", [HostNode]),
    net_kernel:connect(?NODE_A),
    rpc:call(?NODE_A, partisan_peer_service, join, [{?NODE_CT, "127.0.0.1", 10200}]),
    ok = lasp_peer_service:join({?NODE_A, "127.0.0.1", PeerPort}),
    start_nodes(T, [HostNode | Acc]).

manual_start_stop(_Config) ->
    Grain1 = erleans:get_grain(test_grain, <<"grain1">>),
    Grain2 = erleans:get_grain(test_grain, <<"grain2">>),

    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, rpc:call(?NODE_A, test_grain, activated_counter, [Grain2])),

    %% ensure we've waited a broadcast interval
    timer:sleep(200),

    %% verify grain1 is on node ct and grain2 is on node a
    ?assertEqual({ok, ?NODE_CT}, test_grain:node(Grain1)),
    ?assertEqual({ok, ?NODE_A}, test_grain:node(Grain2)),

    ?assertEqual({ok, ?NODE_CT}, rpc:call(?NODE_A, test_grain, node, [Grain1])),
    ?assertEqual({ok, 1}, rpc:call(?NODE_A, test_grain, activated_counter, [Grain2])),
    timer:sleep(200),

    ?assertEqual({ok, ?NODE_A}, rpc:call(?NODE_A, test_grain, node, [Grain2])),
    ?assertEqual({ok, ?NODE_A}, test_grain:node(Grain2)),

    ok.
