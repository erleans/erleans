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
         duplicate_activation_reroutes/1,
         deactivation_keeps_registration/1,
         manual_start_stop/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("test_utils.hrl").

all() ->
    [manual_start_stop, node_loss_reroutes, deactivation_keeps_registration,
     duplicate_activation_reroutes].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_) ->
    application:stop(erleans),
    etag_test_grain:clear_test_keys(),
    ok.

duplicate_activation_reroutes(_Config) ->
    Paths = ["-config", "../../../../test/sys.config", "-pa" | code:get_path()],
    %% Control the peer over stdio so the two directories start isolated.
    %% Its name sorts first, making the local activation the conflict loser.
    {ok, PeerPid, Peer} = peer:start_link(#{name => peer:random_name("a-duplicate"),
                                          connection => standard_io,
                                          args => ["-setcookie", atom_to_list(erlang:get_cookie())
                                                   | Paths]}),
    true = is_pid(PeerPid),
    try
        ?assert(Peer < node()),
        peer:call(PeerPid, application, load, [gen_cluster]),
        peer:call(PeerPid, application, set_env, [gen_cluster, type, {list, []}]),
        {ok, _} = peer:call(PeerPid, application, ensure_all_started, [erleans]),
        Id = integer_to_binary(erlang:unique_integer([positive])),
        Grain0 = erleans:get_grain(etag_test_grain, Id),
        Grain = Grain0#{provider => {erleans_provider_ets, in_memory}},
        ?assertEqual(a, erleans_grain:call(Grain, get)),
        Loser = erleans_grain_registry:whereis_name(Grain),
        {module, etag_test_grain} = peer:call(PeerPid, code, ensure_loaded, [etag_test_grain]),
        {ok, Winner} = peer:call(PeerPid, erleans_grain_sup, start_child, [Grain]),
        ?assertNot(lists:member(Peer, nodes())),
        ?assertEqual(Loser, erleans_grain_registry:whereis_name(Grain)),
        ?assertEqual(Winner, peer:call(PeerPid, erleans_grain_registry, whereis_name, [Grain])),
        Monitor = monitor(process, Loser),
        Test = self(),
        Tag = make_ref(),
        Callers = [spawn_monitor(fun() ->
            Test ! {Tag, result, erleans_grain:call(Grain, {pending_on, node(), Test, Tag})}
        end) || _ <- lists:seq(1, 3)],
        [receive {Tag, pending, Loser} -> ok
         after 1000 -> ct:fail(call_not_pending)
         end || _ <- Callers],

        %% Merging the directories must invoke the registered resolver.
        pong = net_adm:ping(Peer),
        ok = global:sync(),
        receive {'DOWN', Monitor, process, Loser, Reason} ->
            ?assertEqual({shutdown, duplicate_activation}, Reason)
        after 5000 -> ct:fail(duplicate_not_stopped)
        end,
        [begin
             receive {Tag, result, Result} -> ?assertEqual(Winner, Result)
             after 1000 -> ct:fail(call_not_rerouted)
             end,
             receive {'DOWN', M, process, P, normal} -> ok
             after 1000 -> ct:fail(caller_failed)
             end
         end || {P, M} <- Callers],
        ?assertEqual(Winner, erleans_grain_registry:whereis_name(Grain)),
        ?assertEqual(Winner, peer:call(PeerPid, erleans_grain_registry, whereis_name, [Grain])),
        %% The loser's deactivate callback would save its dirty in-memory state.
        ?assertEqual({ok, #{value => a}, 1},
                     erleans_provider_ets:read(etag_test_grain, in_memory, Id)),
        ?assertEqual({ok, #{value => a}, 1},
                     peer:call(PeerPid, erleans_provider_ets, read,
                               [etag_test_grain, in_memory, Id])),
        ?assertEqual(a, erleans_grain:call(Grain, get))
    after
        peer:stop(PeerPid)
    end.

deactivation_keeps_registration(_Config) ->
    Paths = ["-config", "../../../../test/sys.config", "-pa" | code:get_path()],
    {ok, PeerPid, Peer} = ?CT_PEER(Paths),
    true = is_pid(PeerPid),
    try
        erpc:call(Peer, application, load, [gen_cluster]),
        erpc:call(Peer, application, set_env, [gen_cluster, type, {list, []}]),
        {ok, _} = erpc:call(Peer, application, ensure_all_started, [erleans]),
        lists:foreach(fun(Kind) -> deactivation_keeps_registration(Peer, Kind) end,
                      [plain, ephemeral, no_save])
    after
        peer:stop(PeerPid)
    end.

deactivation_keeps_registration(Peer, Kind) ->
    Test = self(),
    Tag = make_ref(),
    Id = etag_test_grain:test_key({deactivation_save, Test, Tag}),
    Grain0 = erleans:get_grain(etag_test_grain, Id),
    Grain = Grain0#{provider => {opaque_etag_provider, in_memory}},
    ?assertEqual(a, erleans_grain:call(Grain, get)),
    Pid = erleans_grain_registry:whereis_name(Grain),
    true = is_pid(Pid),
    Monitor = monitor(process, Pid),
    ?UNTIL(Pid =:= erpc:call(Peer, erleans_grain_registry, whereis_name, [Grain])),
    try
        ok = erleans_grain:call(Grain, {deactivate, Test, Tag, Kind}),
        receive {Tag, deactivating, Pid} -> ok
        after 1000 -> ct:fail(deactivation_not_started)
        end,
        assert_registered(Peer, Grain, Pid),
        {Reader, ReaderMonitor} = spawn_monitor(fun() ->
            Test ! {Tag, read, erleans_grain:call(Grain, get)}
        end),
        %% Make the second call arrive while deactivate/1 is still running.
        ?UNTIL(begin
            {messages, Messages} = process_info(Pid, messages),
            lists:any(fun({'$gen_call', _, _}) -> true; (_) -> false end, Messages)
        end),
        Pid ! {Tag, finish_deactivate},
        {Expected, Version} = case Kind of
            no_save -> {a, <<"version:1">>};
            _ ->
                receive {Tag, saving, Pid} -> ok
                after 1000 -> ct:fail(save_not_started)
                end,
                assert_registered(Peer, Grain, Pid),
                ?assertEqual({ok, #{value => a}, <<"version:1">>},
                             opaque_etag_provider:read(etag_test_grain, in_memory, Id)),
                Pid ! {Tag, finish_save},
                {saved, <<"version:2">>}
        end,
        receive {'DOWN', Monitor, process, Pid, {shutdown, deactivated}} -> ok
        after 1000 -> ct:fail(grain_not_deactivated)
        end,
        receive {Tag, read, Value} -> ?assertEqual(Expected, Value)
        after 1000 -> ct:fail(reader_not_rerouted)
        end,
        receive {'DOWN', ReaderMonitor, process, Reader, normal} -> ok
        after 1000 -> ct:fail(reader_failed)
        end,
        ?assertEqual({ok, #{value => Expected}, Version},
                     opaque_etag_provider:read(etag_test_grain, in_memory, Id)),
        %% The replacement activation must have the current ETag on its first save.
        ok = erleans_grain:call(Grain, {set, latest}),
        ?assertEqual(latest, erleans_grain:call(Grain, get)),
        ?assertNotEqual(Pid, erleans_grain_registry:whereis_name(Grain))
    after
        Pid ! {Tag, finish_deactivate},
        Pid ! {Tag, finish_save}
    end.

assert_registered(Peer, Grain, Pid) ->
    ?assertEqual(Pid, erleans_grain_registry:whereis_name(Grain)),
    ?assertEqual(Pid, erpc:call(Peer, erleans_grain_registry, whereis_name, [Grain])),
    ?assertEqual({error, {already_started, Pid}},
                 erpc:call(Peer, erleans_grain_sup, start_child, [Grain])).

node_loss_reroutes(_Config) ->
    Paths = ["-config", "../../../../test/sys.config", "-pa" | code:get_path()],
    {ok, PeerPid, Peer} = ?CT_PEER(Paths),
    true = is_pid(PeerPid),
    try
        erpc:call(Peer, application, load, [gen_cluster]),
        erpc:call(Peer, application, set_env, [gen_cluster, type, {list, []}]),
        {ok, _} = erpc:call(Peer, application, ensure_all_started, [erleans]),
        Grain = erleans:get_grain(fault_test_grain, integer_to_binary(erlang:unique_integer([positive]))),
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
