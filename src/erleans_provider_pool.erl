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
-module(erleans_provider_pool).

-export([start_link/1]).

-export([init/1]).

-include("erleans.hrl").

start_link(Name) ->
    sbroker:start_link(?broker(Name), ?MODULE, [], [{read_time_after, 16}]).

init(_) ->
    QueueSpec = {sbroker_timeout_queue, #{out => out,
                                          timeout => 5000,
                                          drop => drop_r,
                                          min => 0,
                                          max => 128}},
    WorkerSpec = {sbroker_drop_queue, #{out => out,
                                        drop => drop,
                                        max => infinity}},
    {ok, {QueueSpec, WorkerSpec, []}}.
