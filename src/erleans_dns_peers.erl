%%%----------------------------------------------------------------------------
%%% Copyright Space-Time Insight 2017. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%----------------------------------------------------------------------------

-module(erleans_dns_peers).

-export([discover/3,
         leave/0]).

discover(ClusterType, NodeName, Port) ->
    AllNodes = lookup(ClusterType, NodeName, Port),
    sets:fold(fun({Name, Host, PartisanPort}, Acc) ->
                  [erleans_cluster:to_node(Name, Host, PartisanPort) | Acc]
              end, [], AllNodes).

leave() ->
    ok.
    %% partisan_peer_service:leave([]).

lookup(local, _NodeName, _Port) ->
    sets:new();
lookup(manual, _NodeName, _Port) ->
    sets:new();
lookup(none, _NodeName, _Port) ->
    sets:new();
lookup({direct, Nodes}, _NodeName, _Port) ->
    sets:from_list(Nodes);
lookup({ip, DiscoveryDomain}, NodeName, Port) ->
    lists:foldl(fun(Record, NodesAcc) ->
                    H = inet_parse:ntoa(Record),
                    sets:add_element({list_to_atom(string:join([NodeName, H], "@")), H, Port}, NodesAcc)
                end, sets:new(), inet_res:lookup(DiscoveryDomain, in, a));
lookup({fqdns, DiscoveryDomain}, NodeName, Port) ->
    lists:foldl(fun(Record, NodesAcc) ->
                    {ok, {hostent, Host, _, _, _, _}} = inet_res:gethostbyaddr(Record),
                    sets:add_element({list_to_atom(string:join([NodeName, Host], "@")), Host, Port}, NodesAcc)
                end, sets:new(), inet_res:lookup(DiscoveryDomain, in, a));
lookup({srv, DiscoveryDomain}, NodeName, _) ->
    lists:foldl(fun({_, _, PartisanPort, Host}, NodesAcc) ->
                    Node = list_to_atom(atom_to_list(NodeName)++"@"++Host),
                    sets:add_element({Node, Host, PartisanPort}, NodesAcc)
                end, sets:new(), inet_res:lookup(DiscoveryDomain, in, srv)).
