-module(erleans_stateless_broker).

%-behaviour(sbroker).

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
