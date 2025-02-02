%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(dist_lifecycle_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [manual_start_stop].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_) ->
    application:stop(erleans),
    ok.

manual_start_stop(_Config) ->
    LocalNode = node(),

    Paths = ["-config", "../../../../test/sys.config", "-pa" | code:get_path()],
    {ok, PeerPid, Peer} = ?CT_PEER(Paths),

    ct:print("\e[32m Node ~p [OK] \e[0m", [Peer]),

    erpc:call(Peer, application, load, [gen_cluster]),
    erpc:call(Peer, application, set_env, [gen_cluster, type, {list, []}]),
    erpc:call(Peer, application, ensure_all_started, [erleans]),

    Grain1 = erleans:get_grain(test_grain, <<"grain1">>),
    Grain2 = erleans:get_grain(test_grain, <<"grain2">>),

    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, rpc:call(Peer, test_grain, activated_counter, [Grain2])),

    %% ensure we've waited a broadcast interval
    timer:sleep(500),

    %% verify grain1 is on node ct and grain2 is on node a
    ?assertEqual({ok, LocalNode}, test_grain:node(Grain1)),
    ?assertEqual({ok, Peer}, test_grain:node(Grain2)),

    ?assertEqual({ok, LocalNode}, rpc:call(Peer, test_grain, node, [Grain1])),
    ?assertEqual({ok, 1}, rpc:call(Peer, test_grain, activated_counter, [Grain2])),

    timer:sleep(200),

    ?assertEqual({ok, Peer}, rpc:call(Peer, test_grain, node, [Grain2])),
    ?assertEqual({ok, Peer}, test_grain:node(Grain2)),

    peer:stop(PeerPid),

    ok.
