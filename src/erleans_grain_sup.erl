-module(erleans_grain_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_child/1]).

-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(GrainRef :: erleans:grain_ref()) -> {ok, pid()}.
start_child(GrainRef) ->
    lager:info("sup=~p", [GrainRef]),
    supervisor:start_child(?MODULE, [GrainRef]).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [#{id => erleans_grain,
                    start => {erleans_grain, start_link, []},
                    restart => transient,
                    shutdown => 5000}],
    {ok, {SupFlags, ChildSpecs}}.
