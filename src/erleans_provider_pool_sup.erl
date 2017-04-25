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

%%% ---------------------------------------------------------------------------
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_provider_pool_sup).

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Name, Config) ->
    supervisor:start_link(?MODULE, [Name, Config]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Name, Config]) ->
    Size = proplists:get_value(pool_size, Config, 5),
    Module = proplists:get_value(module, Config),
    Options = proplists:get_value(args, Config, []),

    Broker = {erleans_provider_broker, {erleans_provider_pool, start_link, [Name]},
              permanent, 5000, worker, [erleans_provider_pool]},
    ConnManager = {{erleans_conn_manager, Name}, {erleans_conn_manager, start_link, [Module, Name, Size, Options]},
                   permanent, 5000, worker, [nucleus_provider_worker]},


    {ok, {{one_for_one, 5, 10}, [Broker, ConnManager]}}.

%%====================================================================
%% Internal functions
%%====================================================================
