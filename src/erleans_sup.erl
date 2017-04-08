%%%-------------------------------------------------------------------
%% @doc erleans top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(erleans_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [#{id => erleans_grain_sup,
                    start => {erleans_grain_sup, start_link, []},
                    restart => permanent,
                    type => supervisor,
                    shutdown => 5000},
                  #{id => erleans_system_grain_sup,
                    start => {erleans_system_grain_sup, start_link, []},
                    restart => permanent,
                    type => supervisor,
                    shutdown => 5000}],
    {ok, {SupFlags, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================
