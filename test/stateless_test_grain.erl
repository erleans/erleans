%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc A test grain that increments a counter everytime it is activated.
%%% @end
%%% ---------------------------------------------------------------------------
-module(stateless_test_grain).

-behaviour(erleans_grain).

-export([placement/0,
         provider/0,
         node/1,
         deactivated_counter/1,
         activated_counter/1]).

-export([state/1,
         activate/2,
         handle_call/3,
         handle_cast/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    {stateless, 3}.

provider() ->
    ets.

deactivated_counter(Ref) ->
    erleans_grain:call(Ref, deactivated_counter).

activated_counter(Ref) ->
    erleans_grain:call(Ref, activated_counter).

node(Ref) ->
    erleans_grain:call(Ref, node).

state(_) ->
    #{activated_counter => 0,
      deactivated_counter => 0}.

activate(_, State=#{activated_counter := Counter}) ->
    {ok, State#{activated_counter => Counter+1}, #{}}.

handle_call(node, From, State) ->
    {ok, State, [{reply, From, {ok, node()}}]};
handle_call(deactivated_counter, From, State=#{deactivated_counter := Counter}) ->
    {ok, State, [{reply, From, {ok, Counter}}]};
handle_call(deactivated_counter, From, State) ->
    {ok, State, [{reply, From, {ok, 0}}]};
handle_call(activated_counter, From, State=#{activated_counter := Counter}) ->
    {ok, State, [{reply, From, {ok, Counter}}]};
handle_call(activated_counter, From, State) ->
    {ok, State, [{reply, From, {ok, 0}}]}.

handle_cast(_, State) ->
    {noreply, State}.

deactivate(State=#{deactivated_counter := D}) ->
    {ok, State#{deactivated_counter => D+1}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
