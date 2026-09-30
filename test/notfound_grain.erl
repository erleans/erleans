%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% ---------------------------------------------------------------------------
-module(notfound_grain).

-moduledoc """
A test grain returns notfound on init
""".

-behaviour(erleans_grain).

-export([placement/0,
         provider/0,
         anything/1]).

-export([activate/2,
         handle_call/3,
         handle_cast/2,
         deactivate/1]).

placement() ->
    prefer_local.

provider() ->
    default.

anything(Ref) ->
    erleans_grain:call(Ref, anything).

activate(_, _) ->
    {error, notfound}.

handle_call(_, From, State) ->
    {ok, State, [{reply, From, ok}]}.

handle_cast(_, State) ->
    {ok, State}.

deactivate(State) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
