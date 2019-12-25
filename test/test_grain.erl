%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc A test grain that increments a counter everytime it is activated.
%%% @end
%%% ---------------------------------------------------------------------------
-module(test_grain).

-behaviour(erleans_grain).

-export([placement/0,
         provider/0,
         save/1,
         node/1,
         deactivated_counter/1,
         activated_counter/1,
         call_counter/1]).

-export([state/1,
         activate/2,
         handle_call/3,
         handle_cast/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    prefer_local.

provider() ->
    default.

deactivated_counter(Ref) ->
    erleans_grain:call(Ref, deactivated_counter).

activated_counter(Ref) ->
    erleans_grain:call(Ref, activated_counter).

call_counter(Ref) ->
    erleans_grain:call(Ref, call_counter).

save(Ref) ->
    erleans_grain:call(Ref, save).

node(Ref) ->
    erleans_grain:call(Ref, node).

state(_) ->
    #{activated_counter => 0,
      deactivated_counter => 0,
      call_counter => 0}.

activate(_, State=#{activated_counter := Counter}) ->
    {ok, State#{activated_counter => Counter+1}, #{}}.

handle_call(call_counter, From, State=#{call_counter := CallCounter}) ->
    {ok, State#{call_counter => CallCounter+1}, [{reply, From, {ok, CallCounter}}]};
handle_call(node, From, State=#{call_counter := CallCounter}) ->
    {ok, State#{call_counter => CallCounter+1}, [{reply, From, {ok, node()}}]};
handle_call(deactivated_counter, From, State=#{deactivated_counter := Counter,
                                               call_counter := CallCounter}) ->
    {ok, State#{call_counter => CallCounter+1}, [{reply, From, {ok, Counter}}]};
handle_call(deactivated_counter, From, State=#{call_counter := CallCounter}) ->
    {ok, State#{call_counter => CallCounter+1}, [{reply, From, {ok, 0}}]};
handle_call(activated_counter, From, State=#{activated_counter := Counter,
                                             call_counter := CallCounter}) ->
    {ok, State#{call_counter => CallCounter+1}, [{reply, From, {ok, Counter}}]};
handle_call(activated_counter, From, State=#{call_counter := CallCounter}) ->
    {ok, State#{call_counter => CallCounter+1}, [{reply, From, {ok, 0}}]};
handle_call(save, From, State=#{call_counter := CallCounter}) ->
    {ok, State#{call_counter => CallCounter+1}, [{reply, From, ok}, save_state]}.

handle_cast(_, State) ->
    {ok, State}.

deactivate(State=#{deactivated_counter := D}) ->
    {save_state, State#{deactivated_counter => D+1}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
