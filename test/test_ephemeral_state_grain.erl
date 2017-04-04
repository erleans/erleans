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

-export([placement/0,
         provider/0,
         save/1,
         node/1,
         increment_ephemeral_counter/1,
         ephemeral_counter/1,
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

-define(INIT_STATE, #{persistent => #{activated_counter => 1,
                                      deactivated_counter => 0},
                      ephemeral => #{counter => 0}}).

placement() ->
    prefer_local.

provider() ->
    ets_provider.

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

init(PState=#{activated_counter := Counter}) ->
    {ok, ?INIT_STATE#{persistent => PState#{activated_counter => Counter+1}}, #{life_time => infinity}};
init(#{}) ->
    {ok, ?INIT_STATE, #{life_time => infinity}}.

handle_call(node, _From, State) ->
    {reply, {ok, node()}, State};
handle_call(increment_ephemeral_counter, _From, State=#{ephemeral := EState=#{counter := Counter}}) ->
    {save_reply, ok, State#{ephemeral => EState#{counter => Counter+1}}};
handle_call(ephemeral_counter, _From, State=#{ephemeral := #{counter := Counter}}) ->
    {reply, {ok, Counter}, State};
handle_call(deactivated_counter, _From, State=#{persistent := #{deactivated_counter := Counter}}) ->
    {reply, {ok, Counter}, State};
handle_call(deactivated_counter, _From, State) ->
    {reply, {ok, 0}, State};
handle_call(activated_counter, _From, State=#{persistent := #{activated_counter := Counter}}) ->
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

deactivate(State=#{persistent := PState=#{deactivated_counter := D}}) ->
    {save, State#{persistent => PState#{deactivated_counter => D+1}}}.

change_id(#{change_id := ChangeId}) ->
    ChangeId;
change_id(_) ->
    0.

change_id(ChangeId, State=#{}) ->
    State#{change_id => ChangeId}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
