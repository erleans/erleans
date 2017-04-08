-module(erleans_system_grain_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_child/2]).

-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(Node :: node(), GrainRef :: erleans:grain_ref())
                 -> {ok, pid()} | {error, supervisor:startchild_err()}.
start_child(Node, GrainRef) ->
    lager:info("grain=~p", [GrainRef]),
    supervisor:start_child({?MODULE, Node}, [GrainRef]).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 1,
                 period => 5},
    ChildSpecs = [#{id => erleans_grain,
                    start => {erleans_grain, start_link, []},
                    restart => transient,
                    shutdown => 5000}],
    {ok, {SupFlags, ChildSpecs}}.
