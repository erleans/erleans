%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc A test grain that shows the use of separating persistent and ephemeral
%%%%     state.
%%% @end
%%% ---------------------------------------------------------------------------
-module(test_ephemeral_state_grain).

-behaviour(erleans_grain).

-export([save/1,
         node/1,
         increment_ephemeral_counter/1,
         ephemeral_counter/1,
         deactivated_counter/1,
         activated_counter/1]).

-export([placement/0,
         provider/0,
         state/1,
         activate/2,
         handle_call/3,
         handle_cast/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    prefer_local.

provider() ->
    ets.

deactivated_counter(Ref) ->
    erleans_grain:call(Ref, deactivated_counter).

activated_counter(Ref) ->
    erleans_grain:call(Ref, activated_counter).

increment_ephemeral_counter(Ref) ->
    erleans_grain:call(Ref, increment_ephemeral_counter).

ephemeral_counter(Ref) ->
    erleans_grain:call(Ref, ephemeral_counter).

save(Ref) ->
    erleans_grain:call(Ref, save).

node(Ref) ->
    erleans_grain:call(Ref, node).

state(_) ->
    #{activated_counter => 0,
      deactivated_counter => 0}.

activate(_, PState=#{activated_counter := Counter}) ->
    {ok, {#{counter => 0}, PState#{activated_counter => Counter+1}}, #{}}.

handle_call(node, From, State) ->
    {ok, State, [{reply, From, {ok, node()}}]};
handle_call(increment_ephemeral_counter, From, {EState=#{counter := Counter}, PState}) ->
    {ok, {EState#{counter => Counter+1}, PState}, [{reply, From, ok}, save_state]};
handle_call(ephemeral_counter, From, State={#{counter := Counter}, _}) ->
    {ok, State, [{reply, From, {ok, Counter}}]};
handle_call(deactivated_counter, From, State={_, #{deactivated_counter := Counter}}) ->
    {ok, State, [{reply, From, {ok, Counter}}]};
handle_call(activated_counter, From, State={_, #{activated_counter := Counter}}) ->
    {ok, State, [{reply, From, {ok, Counter}}]};
handle_call(save, From, State) ->
    {ok, State, [{reply, From, ok}, save_state]}.

handle_cast(_, State) ->
    {ok, State}.

deactivate({EState, PState=#{deactivated_counter := D}}) ->
    {save_state, {EState, PState#{deactivated_counter => D+1}}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
