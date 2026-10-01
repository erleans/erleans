%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% ---------------------------------------------------------------------------
-module(dist_lifecycle_SUITE).

-export([all/0,
         init_per_suite/1,
         end_per_suite/1,
         node_loss_reroutes/1,
         manual_start_stop/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("test_utils.hrl").

all() ->
    [manual_start_stop, node_loss_reroutes].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_) ->
    application:stop(erleans),
    ok.

node_loss_reroutes(_Config) ->
    Paths = ["-config", "../../../../test/sys.config", "-pa" | code:get_path()],
    {ok, PeerPid, Peer} = ?CT_PEER(Paths),
    true = is_pid(PeerPid),
    try
        erpc:call(Peer, application, load, [gen_cluster]),
        erpc:call(Peer, application, set_env, [gen_cluster, type, {list, []}]),
        {ok, _} = erpc:call(Peer, application, ensure_all_started, [erleans]),
        Grain = erleans:get_grain(fault_test_grain, make_ref()),
        {ok, RemotePid} = erpc:call(Peer, erleans_grain_sup, start_child, [Grain]),
        ?UNTIL(RemotePid =:= erleans_grain_registry:whereis_name(Grain)),
        Test = self(),
        Tag = make_ref(),
        {Caller, Monitor} = spawn_monitor(fun() ->
            Result = erleans_grain:call(Grain, {block_on, Peer, Test, Tag}),
            Test ! {Tag, result, Result}
        end),
        receive {Tag, blocked, RemotePid} -> ok
        after 1000 -> ct:fail(remote_call_not_started)
        end,
        %% Halt the node while the call is in flight, before it can reply.
        ok = erpc:cast(Peer, erlang, halt, []),
        NewPid = receive
                     {Tag, result, Pid} when is_pid(Pid) -> Pid;
                     {'DOWN', Monitor, process, Caller, Reason} -> ct:fail({caller_failed, Reason})
                 after 5000 -> ct:fail(call_not_rerouted)
                 end,
        ?assertEqual(node(), node(NewPid)),
        ?assertNotEqual(RemotePid, NewPid),
        ?assertEqual(NewPid, erleans_grain_registry:whereis_name(Grain)),
        receive {'DOWN', Monitor, process, Caller, normal} -> ok
        after 1000 -> ct:fail(caller_failed)
        end
    after
        case is_process_alive(PeerPid) of
            true -> peer:stop(PeerPid);
            false -> ok
        end
    end.

manual_start_stop(_Config) ->
    LocalNode = node(),

    Paths = ["-config", "../../../../test/sys.config", "-pa" | code:get_path()],
    {PeerPid, Peer} = case ?CT_PEER(Paths) of
                         {ok, Pid, Node} when is_pid(Pid) ->
                             {Pid, Node}
                     end,

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
