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
         init/2,
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

init(#{id := _Id}, State=#{}) ->
    ct:pal("starting or waking up? ~p", [State]),
    {ok, State, #{life_time => infinity}}.

handle_call({subscribe, Topic, Offset}, _From, State) ->
    StreamProvider = erleans_config:get(default_stream_provider),
    Ret = erleans_grain:subscribe(StreamProvider, Topic, Offset),
    {reply, Ret, State};
handle_call({unsubscribe, Topic}, _From, State) ->
    StreamProvider = erleans_config:get(default_stream_provider),
    Ret = erleans_grain:unsubscribe(StreamProvider, Topic),
    {reply, Ret, State};
handle_call(records_read, _From, State) ->
    RecordsRead = maps:get(records_read, State, 0),
    {reply, RecordsRead, State};
handle_call(reset, _From, State) ->
    {reply, ok, State#{records_read => 0}};
handle_call({stream, _Topic, Records}, _, State) ->
    RecordsRead = maps:get(records_read, State, 0),
    ct:pal("got stream message for topic ~p with ~p records, ~p previous",
           [_Topic, length(Records), RecordsRead]),
    ct:pal("records ~p", [Records]),
    Len = length(Records),
    {reply, ok, State#{records_read => RecordsRead + Len}}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

deactivate(State) ->
    ct:pal("shutting down! ~p", [State]),
    {save, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
