%%%----------------------------------------------------------------------------
%%% Copyright Space-Time Insight 2017. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%----------------------------------------------------------------------------

%%% ---------------------------------------------------------------------------
%%% @doc Unlike grains a streams process (activation) must be continuous. It
%%%      must be reactivated on a new node if the one it currently lives on
%%%      leaves the cluster for whatever reason. This gen_server is responsible
%%%      for figuring out what streams this node should be handling based on
%%%      all nodes currently members of the cluster.
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_stream_manager).

-behaviour(gen_server).

-export([start_link/0,
         next/2,
         subscribe/2,
         update_streams/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% can remove these when bug in sbroker specs is fixed
-dialyzer({nowarn_function, handle_down_agent/3}).
-dialyzer({nowarn_function, enqueue_stream/2}).

-include("erleans.hrl").

-define(TIMEOUT, 10000). %% check what streams to be running after 10 seconds of a node change

-record(state,
        {streams  :: #{erleans:stream_ref() => {sets:set(erleans:grain_ref()), integer(), integer()}},
         monitors :: #{reference() => erleans:stream_ref()},
         provider :: {module(), atom()}
        }).

-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

next(StreamRef, SequenceToken) ->
    gen_server:call(?MODULE, {next, StreamRef, SequenceToken}).

subscribe(StreamRef, GrainRef) ->
    {Partition, Node} = erleans_partitions:find_node(StreamRef),
    gen_server:call({?MODULE, Node}, {subscribe, StreamRef, Partition, GrainRef}).

update_streams(Range) ->
    gen_server:call(?MODULE, {update_streams, Range}).

init([]) ->
    %% storage provider for stream metadata
    Provider = erleans_stream:provider(),
    ProviderOptions = proplists:get_value(Provider, erleans_config:get(providers, [])),
    Module = proplists:get_value(module, ProviderOptions),

    erleans_partitions:add_handler(?MODULE, ?MODULE),

    {ok, #state{streams=#{},
                provider={Module, Provider},
                monitors=#{}}, 0}.

handle_call({next, StreamRef, SequenceToken}, _From={FromPid, _Tag}, State=#state{monitors=Monitors,
                                                                                  provider=Provider,
                                                                                  streams=Streams}) ->
    lager:info("at=next stream_ref=~p sequence_token=~p", [StreamRef, SequenceToken]),
    Stream1 = StreamRef#{sequence_token => SequenceToken},
    case maps:get(StreamRef, Streams, undefined) of
        {CurrentSubs, Partition, ETag} ->
            NewETag = save(StreamRef, Partition, CurrentSubs, ETag, Provider),
            Streams1 = Streams#{Stream1 => {CurrentSubs, Partition, NewETag}},

            %% demonitor process that was handling this stream
            %% TODO: shouldn't have to be O(N)
            Monitors1 = maps:filter(fun(MonitorRef, Pid) when Pid =:= FromPid ->
                                        erlang:demonitor(MonitorRef, [flush]),
                                        false;
                                       (_, _) ->
                                        true
                                    end, Monitors),
            {reply, ok, State#state{streams=Streams1,
                                    monitors=Monitors1}};
        undefined ->
            %% weird, we got a next but aren't responsible for this stream
            {reply, {error, not_my_responsibility}, State}
    end;
handle_call({subscribe, StreamRef, Partition, Grain}, _From, State=#state{streams=Streams,
                                                                          provider=Provider}) ->
    lager:info("at=subscribe stream_ref=~p grain_ref=~p", [StreamRef, Grain]),
    {NewSubscribers, OldETag} = case maps:get(StreamRef, Streams, undefined) of
                                    undefined ->
                                        {sets:add_element(Grain, sets:new()), undefined};
                                    {Subscribers, _, ETag} ->
                                        {sets:add_element(Grain, Subscribers), ETag}
                                end,
    enqueue_stream(StreamRef, NewSubscribers),
    NewETag = save(StreamRef, Partition, NewSubscribers, OldETag, Provider),
    Streams1 = maps:put(StreamRef, {NewSubscribers, Partition, NewETag}, Streams),
    {reply, ok, State#state{streams=Streams1}};
handle_call({unsubscribe, StreamRef, Grain}, _From, State=#state{streams=Streams,
                                                                 provider=Provider}) ->
    lager:info("at=unsubscribe stream_ref=~p grain_ref=~p", [StreamRef, Grain]),
    case maps:get(StreamRef, Streams, undefined) of
        undefined ->
            %% no longer our responsibility?
            {reply, ok, State};
        {Subscribers, Partition, ETag} ->
            Subscribers1 = sets:del_element(Grain, Subscribers),
            NewETag = save(StreamRef, Partition, Subscribers1, ETag, Provider),
            Streams1 = maps:put(StreamRef, {Subscribers1, NewETag}, Streams),
            {reply, ok, State#state{streams=Streams1}}
    end;
handle_call({update_streams, Range}, _From, State=#state{provider=Provider}) ->
    MyStreams = enqueue_streams(Provider, Range),
    {reply, ok, State#state{streams = MyStreams}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, _, _, _}, State=#state{monitors=Monitors,
                                                        streams=Streams}) ->
    %% an agent crashed while handling a stream
    %% find out what stream it is and enqueue it again
    Monitors1 = handle_down_agent(MonitorRef, Monitors, Streams),
    {noreply, State#state{monitors=Monitors1}};
handle_info({StreamRef, {go, _Ref, Pid, _RelativeTime, _SojournTime}}, State=#state{monitors=Monitors}) ->
    Ref = erlang:monitor(process, Pid),
    {noreply, State#state{monitors = Monitors#{Ref => StreamRef}}};
handle_info({_Stream, {drop, _SojournTime}}, State) ->
    %% should never happen... we have an infinite timeout
    {noreply, State};
handle_info({update_streams, Range}, State=#state{provider=Provider}) ->
    lager:info("at=update_streams", []),
    %sbroker:dirty_cancel(?STREAM_BROKER, ?STREAM_TAG),
    MyStreams = enqueue_streams(Provider, Range),
    {noreply, State#state{streams = MyStreams}};
handle_info(timeout, State) ->
    Range = erleans_partitions:get_range(),
    self() ! {update_streams, Range},
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal functions

enqueue_streams({ProviderModule, Provider}, {Start, Stop}) ->
    lists:foldl(fun(Partition, Acc) ->
                    %% should eventually use the range Hash>=Start AND Hash<=Stop in provider
                    {ok, Streams} = ProviderModule:read_by_hash(erleans_stream, Provider, Partition),
                    lists:foldl(fun({StreamRef, erleans_stream, ETag, Subscribers}, Acc1) ->
                                    enqueue_stream(StreamRef, Subscribers),
                                    Acc1#{StreamRef => {Subscribers, Partition, ETag}}
                                end, Acc, Streams)
                end, #{}, lists:seq(Start, Stop)).

handle_down_agent(MonitorRef, Monitors, Streams) ->
    {Stream, Monitors1} = maps:take(MonitorRef, Monitors),
    {Subscribers, _ETag} = maps:get(Stream, Streams),
    lager:info("at=DOWN stream_ref=~p", [Stream]),
    enqueue_stream(Stream, Subscribers),
    Monitors1.

-spec enqueue_stream(erleans:stream_ref(), sets:set(erleans:grain_ref())) -> {await, any(), pid()} | {drop, 0}.
enqueue_stream(StreamRef, Subscribers) ->
    sbroker:async_ask(?STREAM_BROKER, {StreamRef, Subscribers}, {self(), StreamRef}).

save(Id, Partition, Value, undefined, {ProviderModule, ProviderName}) ->
    ETag = erlang:phash2(Value),
    ProviderModule:insert(erleans_stream, ProviderName, Id, Partition, Value, ETag),
    ETag;
save(Id, Partition, Value, OldETag, {ProviderModule, ProviderName}) ->
    ETag = erlang:phash2(Value),
    ProviderModule:replace(erleans_stream, ProviderName, Id, Partition, Value, OldETag, ETag),
    ETag.
