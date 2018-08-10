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

-define(NODE_CT, 'ct@127.0.0.1').
-define(NODE_A, 'a@127.0.0.1').

all() ->
    [manual_start_stop, simple_subscribe].

init_per_suite(Config) ->
    application:load(partisan),
    application:load(erleans),
    application:set_env(partisan, peer_port, 10200),
    application:set_env(partisan, pid_encoding, false),
    %% lower gossip interval of partisan membership so it triggers more often in tests
    application:set_env(partisan, gossip_interval, 100),
    application:load(lasp),
    application:ensure_all_started(lager),
    application:ensure_all_started(pgo),
    {ok, _} = application:ensure_all_started(erleans),
    start_nodes(),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    application:stop(pgo),
    application:stop(plumtree),
    application:stop(partisan),
    application:stop(lasp_pg),
    application:stop(lasp),
    {ok, _} = ct_slave:stop(?NODE_A),
    ok.

start_nodes() ->
    Nodes = [{?NODE_A, 10201}], %, b, c, d],
    start_nodes(Nodes, []).

start_nodes([], Acc) ->
    Acc;
start_nodes([{Node, PeerPort} | T], Acc) ->
    ErlFlags = case application:get_env(erleans, default_provider, ets) of
                   ets ->
                       "-config ../../../../test/sys.config";
                   postgres ->
                       "-config ../../../../test/db_sys.config"
               end,
    CodePath = code:get_path(),
    {ok, HostNode} = ct_slave:start(Node,
                                    [{kill_if_fail, true},
                                     {monitor_master, true},
                                     {init_timeout, 3000},
                                     {startup_timeout, 3000},
                                     {startup_functions,
                                      [{code, set_path, [CodePath]},
                                       {application, load, [lager]},
                                       {application, set_env,
                                        [lager, handlers,
                                         [{lager_console_backend, [{level, debug}]},
                                          {lager_file_backend,
                                           [{file, "log/ct_console.log"}, {level, debug}]}]]},
                                       {application, ensure_all_started, [lager]},
                                       {application, load, [partisan]},
                                       {application, load, [vonnegut]},
                                       {application, set_env,
                                        [vonnegut, http_port, 8001]},
                                       {application, load, [erleans]},
                                       {application, set_env, [partisan, pid_encoding, false]},
                                       {application, set_env, [partisan, gossip_interval, 100]},
                                       {application, set_env, [partisan, peer_port, PeerPort]},
                                       {application, ensure_all_started, [partisan]},
                                       {application, ensure_all_started, [erleans]},
                                       {application, load, [partisan]}]},
                                     {erl_flags, ErlFlags}]),
    timer:sleep(1000),

    ct:print("\e[32m Node ~p [OK] \e[0m", [HostNode]),
    net_kernel:connect_node(?NODE_A),
    rpc:call(?NODE_A, partisan_peer_service, join, [#{name => ?NODE_CT,
                                                      listen_addrs => [#{ip => {127,0,0,1}, port => 10200}],
                                                      parallelism => 1}]),
    ok = lasp_peer_service:join(#{name => ?NODE_A,
                                  listen_addrs => [#{ip => {127,0,0,1}, port => PeerPort}],
                                  parallelism => 1}),
    start_nodes(T, [HostNode | Acc]).

manual_start_stop(_Config) ->
    Grain1 = erleans:get_grain(test_grain, <<"grain1">>),
    Grain2 = erleans:get_grain(test_grain, <<"grain2">>),

    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, rpc:call(?NODE_A, test_grain, activated_counter, [Grain2])),

    %% ensure we've waited a broadcast interval
    timer:sleep(500),

    %% verify grain1 is on node ct and grain2 is on node a
    ?assertEqual({ok, ?NODE_CT}, test_grain:node(Grain1)),
    ?assertEqual({ok, ?NODE_A}, test_grain:node(Grain2)),

    ?assertEqual({ok, ?NODE_CT}, rpc:call(?NODE_A, test_grain, node, [Grain1])),
    ?assertEqual({ok, 1}, rpc:call(?NODE_A, test_grain, activated_counter, [Grain2])),

    timer:sleep(200),

    ?assertEqual({ok, ?NODE_A}, rpc:call(?NODE_A, test_grain, node, [Grain2])),
    ?assertEqual({ok, ?NODE_A}, test_grain:node(Grain2)),

    ok.

simple_subscribe(_Config) ->
    Grain1 = erleans:get_grain(stream_test_grain, <<"dist-simple-subscribe-grain1">>),
    Topic = <<"dist-simple-topic">>,
    stream_test_grain:subscribe(Grain1, Topic),
    RcrdList = lists:duplicate(5 + rand:uniform(100), <<"repeated simple record">>),
    erleans_test_utils:produce(Topic, RcrdList),
    ?UNTIL(stream_test_grain:records_read(Grain1) >= 3),
    ok.
