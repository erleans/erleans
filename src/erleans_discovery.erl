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
               partisan_port    :: integer() | undefined,
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
                               refresh_interval=infinity}};
        Type ->
            PartisanPort = erleans_config:get(partisan_port, 10200),
            NodeName = erleans_config:get(nodename, nonodename),
            RefreshInterval = erleans_config:get(refresh_interval, 5000),
            {ok, inactive, #data{type=Type,
                                 partisan_port=PartisanPort,
                                 nodename=NodeName,
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
    {ok, MembersList} = partisan_peer_service:members(),
    {keep_state_and_data, [{reply, From, {ok, MembersList}}]}.

handle_refresh(#data{type=none}) ->
    ok;
handle_refresh(#data{type=Type,
                     nodename=NodeName,
                     partisan_port=PartisanPort}) ->
    Nodes = erleans_dns_peers:discover(Type, NodeName, PartisanPort),
    ok = partisan_peer_service:update_members(Nodes).
