%%%--------------------------------------------------------------------
%%% Copyright Tristan Sloughter 2019. All Rights Reserved.
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
%%%-----------------------------------------------------------------

-module(erleans_cluster).

-export([to_node/2,
         to_node/3,
         leave/0]).

-include_lib("kernel/include/inet.hrl").

to_node(Name, Host) ->
    to_node(Name, Host, 8080).

to_node(Name, Host, PartisanPort) ->
    IP = case inet:parse_address(Host) of
             {error, einval} ->
                 {ok, #hostent{h_addr_list=[IPAddress | _]}} = inet_res:getbyname(Host, a),
                 IPAddress;
             {ok, IPAddress} ->
                 IPAddress
         end,
    list_to_atom(Host ++ "@" ++ IP).
    %% #{name => Name,
    %%   listen_addrs => [#{ip => IP, port => PartisanPort}],
    %%   parallelism => 1}.

leave() ->
    partisan_peer_service:leave([]).
