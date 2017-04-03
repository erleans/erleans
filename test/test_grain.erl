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
         activated_counter/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         eval_timer/1,
         change_id/1,
         change_id/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    prefer_local.

provider() ->
    ets_provider.

deactivated_counter(Ref) ->
    erleans_grain:call(Ref, deactivated_counter).

activated_counter(Ref) ->
    erleans_grain:call(Ref, activated_counter).

save(Ref) ->
    erleans_grain:call(Ref, save).

node(Ref) ->
    erleans_grain:call(Ref, node).

init(State=#{activated_counter := Counter}) ->
    {ok, State#{activated_counter => Counter+1}, #{life_time => infinity}};
init(State=#{}) ->
    {ok, State#{activated_counter => 1}, #{life_time => infinity}}.

handle_call(node, _From, State) ->
    {reply, {ok, node()}, State};
handle_call(deactivated_counter, _From, State=#{deactivated_counter := Counter}) ->
    {reply, {ok, Counter}, State};
handle_call(deactivated_counter, _From, State) ->
    {reply, {ok, 0}, State};
handle_call(activated_counter, _From, State=#{activated_counter := Counter}) ->
    {reply, {ok, Counter}, State};
handle_call(activated_counter, _From, State) ->
    {reply, {ok, 0}, State};
handle_call(save, _From, State) ->
    {save_reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

eval_timer(State) ->
    {ok, State}.

deactivate(State=#{deactivated_counter := D}) ->
    {save, State#{deactivated_counter => D+1}};
deactivate(State) ->
    {save, State#{deactivated_counter => 1}}.

change_id(#{change_id := ChangeId}) ->
    ChangeId;
change_id(_) ->
    0.

change_id(ChangeId, State=#{}) ->
    State#{change_id => ChangeId}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
