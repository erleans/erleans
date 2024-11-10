-module(erleans_discovery).

-behaviour(gen_statem).

-export([start_link/0,
         members/0]).

-export([init/1,
         active/3,
         inactive/3,
         callback_mode/0,
         terminate/3,
         code_change/4]).

-record(data, {type             :: none
                                 | manual
                                 | {direct, list()}
                                 | {fqdns, string()}
                                 | {srv, string()}
                                 | {ip, string()},
               nodes            :: list(),
               nodename         :: atom() | undefined,
               refresh_interval :: integer() | infinity}).

-define(SERVER, ?MODULE).

start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

members() ->
    gen_statem:call(?SERVER, members).

callback_mode() ->
    [state_functions].

init([]) ->
    case erleans_config:get(node_discovery, none) of
        none ->
            {ok, active, #data{type=none,
                               nodes=[],
                               refresh_interval=infinity}};
        Type ->
            %% PartisanPort = erleans_config:get(partisan_port, 10200),
            NodeName = erleans_config:get(nodename, nonodename),
            RefreshInterval = erleans_config:get(refresh_interval, 5000),
            Nodes = erleans_config:get(nodes, []),
            {ok, inactive, #data{type=Type,
                                 nodename=NodeName,
                                 nodes=Nodes,
                                 refresh_interval=RefreshInterval}, [{next_event, internal, refresh}]}
    end.

inactive(internal, refresh, Data=#data{refresh_interval=RefreshInterval}) ->
    ok = handle_refresh(Data),
    {next_state, active, Data, [{timeout, RefreshInterval, refresh}]};
inactive(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

active(timeout, refresh, Data=#data{refresh_interval=RefreshInterval}) ->
    ok = handle_refresh(Data),
    {keep_state_and_data, [{timeout, RefreshInterval, refresh}]};
active(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_, _OldState, Data, _) ->
    {ok, Data}.

%% Internal functions

handle_event({call, From}, members, _Data) ->
    %% {ok, MembersList} = partisan_peer_service:members(),
    Nodes = nodes(),
    {keep_state_and_data, [{reply, From, {ok, Nodes}}]}.

handle_refresh(#data{type=none}) ->
    ok;
handle_refresh(#data{type=_Type,
                     nodename=_NodeName,
                     nodes=Nodes}) ->
    %% Nodes = erleans_dns_peers:discover(Type, NodeName, PartisanPort),
    [net_adm:ping(Node) || Node <- Nodes].
    %% ok = partisan_peer_service:update_members(Nodes).
