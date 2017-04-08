%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2017 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_consumer_grain).

-behaviour(erleans_grain).

-export([placement/0,
         provider/0,
         subscribe/2,
         unsubscribe/2]).

-export([init/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         eval_timer/1,
         deactivate/1]).

-include_lib("stdlib/include/ms_transform.hrl").

-define(INITIAL_FETCH_INTERVAL, 1000).

placement() ->
    prefer_local.

provider() ->
    erleans_config:provider(erleans_consumer_grain).

subscribe(Ref, Grain) ->
    erleans_grain:call(Ref, {subscribe, Grain}).

unsubscribe(Ref, Grain) ->
    erleans_grain:call(Ref, {unsubscribe, Grain}).

init(#{id := {StreamProvider, Topic}}, State) ->
    Subscribers = maps:get(subscribers, State, sets:new()),
    {ok, State#{topic => Topic,
                stream_provider => StreamProvider,
                offset => 0,
                subscribers => Subscribers,
                fetch_interval => ?INITIAL_FETCH_INTERVAL}, #{eval_timeout_interval => ?INITIAL_FETCH_INTERVAL}}.

handle_call({subscribe, Grain}, _From, State=#{subscribers := Subscribers}) ->
    {reply, ok, State#{subscribers => sets:add_element(Grain, Subscribers)}};
handle_call({unsubscribe, Grain}, _From, State=#{subscribers := Subscribers}) ->
    {reply, ok, State#{subscribers => sets:del_element(Grain, Subscribers)}}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

eval_timer(State=#{stream_provider := StreamProvider,
                   subscribers := Subscribers,
                   topic := Topic,
                   offset := Offset}) ->
    case StreamProvider:fetch([{Topic, Offset}]) of
        [] ->
            NewOffset = Offset;
        [{Topic, {NewOffset, RecordSet}}] ->
            ec_plists:foreach(fun(G) ->
                                  erleans_grain:call(G, {stream, Topic, RecordSet})
                              end, sets:to_list(Subscribers))
    end,
    {ok, State#{offset => NewOffset}}.

deactivate(State) ->
    {save, State}.

%%
