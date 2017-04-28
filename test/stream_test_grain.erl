%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc A test grain that subscribes to a stream.
%%% @end
%%% ---------------------------------------------------------------------------
-module(stream_test_grain).

-behaviour(erleans_grain).

-export([records_read/1]).

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

records_read(Ref) ->
    erleans_grain:call(Ref, records_read).

init(#{id := Id}, State=#{}) ->
    Topic = <<Id/binary, "-some-topic">>,
    StreamProvider = test_stream,
    erleans_grain:subscribe(StreamProvider, Topic),
    {ok, State, #{life_time => infinity}}.

handle_call(records_read, _From, State) ->
    RecordsRead = maps:get(records_read, State, 0),
    {reply, RecordsRead, State};
handle_call({stream, _Topic, Records}, _, State) ->
    RecordsRead = maps:get(records_read, State, 0),
    Len = length(Records),
    {reply, ok, State#{records_read => RecordsRead + Len}}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

deactivate(State) ->
    {save, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
