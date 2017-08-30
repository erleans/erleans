%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc A test grain that subscribes to a stream.
%%% @end
%%% ---------------------------------------------------------------------------
-module(stream_test_grain).

-behaviour(erleans_grain).

-export([subscribe/2, subscribe/3,
         unsubscribe/2,
         reset/1,
         records_read/1]).

-export([placement/0,
         provider/0,
         activate/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    prefer_local.

provider() ->
    erleans_config:get(default_provider).

subscribe(Ref, Topic) ->
    subscribe(Ref, Topic, 0).

subscribe(Ref, Topic, Offset) ->
    erleans_grain:call(Ref, {subscribe, Topic, Offset}).

unsubscribe(Ref, Topic) ->
    erleans_grain:call(Ref, {unsubscribe, Topic}).

reset(Ref) ->
    erleans_grain:call(Ref, reset).

records_read(Ref) ->
    erleans_grain:call(Ref, records_read).

activate(#{id := _Id}, State=#{}) ->
    ct:pal("starting or waking up? ~p", [State]),
    {ok, State, #{}}.

handle_call({subscribe, Topic, Offset}, From, State) ->
    StreamProvider = erleans_config:get(default_stream_provider),
    Ret = erleans_grain:subscribe(StreamProvider, Topic, Offset),
    {ok, State, [{reply, From, Ret}]};
handle_call({unsubscribe, Topic}, From, State) ->
    StreamProvider = erleans_config:get(default_stream_provider),
    Ret = erleans_grain:unsubscribe(StreamProvider, Topic),
    {ok, State, [{reply, From, Ret}]};
handle_call(records_read, From, State) ->
    RecordsRead = maps:get(records_read, State, 0),
    {ok, State, [{reply, From, RecordsRead}]};
handle_call(reset, From, State) ->
    {ok, State#{records_read => 0}, [{reply, From, ok}]};
handle_call({stream, _Topic, Records}, From, State) ->
    RecordsRead = maps:get(records_read, State, 0),
    ct:pal("got stream message for topic ~p with ~p records, ~p previous",
           [_Topic, length(Records), RecordsRead]),
    ct:pal("records ~p", [Records]),
    Len = length(Records),
    {ok, State#{records_read => RecordsRead + Len}, [{reply, From, ok}]}.

handle_cast(_, State) ->
    {ok, State}.

handle_info(_, State) ->
    {ok, State}.

deactivate(State) ->
    ct:pal("shutting down! ~p", [State]),
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
