%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc A test grain returns notfound on init
%%% @end
%%% ---------------------------------------------------------------------------
-module(notfound_grain).

-behaviour(erleans_grain).

-export([placement/0,
         provider/0,
         anything/1]).

-export([activate/2,
         handle_call/3,
         handle_cast/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    prefer_local.

provider() ->
    erleans_config:get(default_provider).

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
