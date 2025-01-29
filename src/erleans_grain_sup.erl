%%%----------------------------------------------------------------------------
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
%%%----------------------------------------------------------------------------

-module(erleans_grain_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_child/1,
         start_child/2]).

-export([init/1]).

-include_lib("kernel/include/logger.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(erleans:grain_ref()) -> supervisor:startchild_ret().
start_child(GrainRef) ->
    ?LOG_INFO("local_grain=~p", [GrainRef]),
    supervisor:start_child(?MODULE, [GrainRef]).

-spec start_child(node(), erleans:grain_ref()) -> supervisor:startchild_ret().
start_child(Node, GrainRef) ->
    ?LOG_INFO("node=~p grain=~p", [Node, GrainRef]),
    supervisor:start_child({?MODULE, Node}, [GrainRef]).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 5,
                 period => 10},
    ChildSpecs = [#{id => erleans_grain,
                    start => {erleans_grain, start_link, []},
                    restart => temporary,
                    shutdown => 5000}],
    {ok, {SupFlags, ChildSpecs}}.
