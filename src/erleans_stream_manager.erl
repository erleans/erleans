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
         next/3,
         subscribe/3,
         unsubscribe/2,
         update_streams/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include_lib("kernel/include/logger.hrl").

%% can remove these when bug in sbroker specs is fixed
-dialyzer({nowarn_function, handle_down_agent/3}).
-dialyzer({nowarn_function, enqueue_streams/2}).
-dialyzer({nowarn_function, enqueue_stream/4}).

-include("erleans.hrl").

-define(TIMEOUT, 10000). %% check what streams to be running after 10 seconds of a node change

-record(substream,
        {
          grains = sets:new() :: sets:set(),
          etag = undefined :: pos_integer() | undefined,
          seq_token :: any(),
          fetch_timer :: timer:tref() | undefined,
          partition :: non_neg_integer(),
          persist_ctr = 0 :: non_neg_integer()
        }).

-record(state,
        {streams  :: #{erleans:stream_ref() => [{reference(), #substream{}}]},
         monitors :: #{reference() => {erleans:stream_ref(), reference()}},
         provider :: {module(), atom()}
        }).

-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

next(StreamRef, SubRef, SequenceToken) ->
    gen_server:call(?MODULE, {next, StreamRef, SubRef, SequenceToken}).

subscribe(StreamRef, GrainRef, SequenceToken) ->
    {Partition, Node} = erleans_partitions:find_node(StreamRef),
    gen_server:call({?MODULE, Node},
                    {subscribe, StreamRef, Partition, GrainRef, SequenceToken}).

unsubscribe(StreamRef, GrainRef) ->
    {_Partition, Node} = erleans_partitions:find_node(StreamRef),
    gen_server:call({?MODULE, Node},
                    {unsubscribe, StreamRef, GrainRef}).

update_streams(Range) ->
    gen_server:call(?MODULE, {update_streams, Range}).

init([]) ->
    %% storage provider for stream metadata
    Provider = erleans_stream_provider:metadata_provider(),
    ProviderOptions = proplists:get_value(Provider, erleans_config:get(providers, [])),
    Module = proplists:get_value(module, ProviderOptions),

    erleans_partitions:add_handler(?MODULE, ?MODULE),

    start_gc_timer(),
    {ok, #state{streams=#{},
                provider={Module, Provider},
                monitors=#{}}}.

handle_call({next, StreamRef, SubRef, NewSequenceToken}, _From={FromPid, _Tag},
            State=#state{monitors=Monitors,
                         provider=Provider,
                         streams=Streams}) ->
    ?LOG_INFO("at=next stream_ref=~p sequence_token=~p",
               [StreamRef, NewSequenceToken]),
    case maps:get(StreamRef, Streams, undefined) of
        undefined ->
            %% weird, we got a next but aren't responsible for this stream
            {reply, {error, not_my_responsibility}, State};
        SubStreams ->
            {_, SubStream = #substream{grains = Grains,
                                       partition = Partition,
                                       etag = ETag,
                                       persist_ctr = Persist}} =
                lists:keyfind(SubRef, 1, SubStreams),
            Save = erleans_config:get(stream_metadata_save_interval, 20),
            SubStream1 =
                case (Persist rem Save) of
                    0 ->
                        NewETag = save({StreamRef, SubRef}, Partition,
                                       {Grains, NewSequenceToken}, ETag, Provider),
                        SubStream#substream{seq_token = NewSequenceToken,
                                            persist_ctr = Persist + 1,
                                            etag = NewETag};
                    _ ->
                        SubStream#substream{seq_token = NewSequenceToken,
                                            persist_ctr = Persist + 1}
                end,

            SubStreams1 = lists:keyreplace(SubRef, 1, SubStreams,
                                           {SubRef, SubStream1}),

            Streams1 = Streams#{StreamRef => SubStreams1},

            %% demonitor process that was handling this stream
            %% TODO: shouldn't have to be O(N)
            Monitors1 = maps:filter(fun(MonitorRef, Pid) when Pid =:= FromPid ->
                                        erlang:demonitor(MonitorRef, [flush]),
                                        false;
                                       (_, _) ->
                                        true
                                    end, Monitors),
            {reply, ok, State#state{streams=Streams1,
                                    monitors=Monitors1}}
    end;
handle_call({subscribe, StreamRef, Partition, Grain, SequenceToken}, _From,
            State=#state{streams=Streams, provider=Provider}) ->
    ?LOG_INFO("at=subscribe stream_ref=~p grain_ref=~p", [StreamRef, Grain]),
    %% note that we don't care about existing streams here on
    %% subscribe!  anything else with the same sequence token could be
    %% getting fetched elsewhere concurrently, so we need to add our
    %% stream independently and wait for GC to take care of merging
    %% the streams later.
    Grains = sets:add_element(Grain, sets:new()),
    SubStream = #substream{grains = Grains,
                           seq_token = SequenceToken,
                           partition = Partition},
    SubRef = make_ref(),
    #{fetch_interval := FetchInterval} = StreamRef,
    {ok, FetchTimer} = timer:send_interval(FetchInterval,
                                           {trigger_fetch, StreamRef, SubRef}),
    NewETag = save({StreamRef, SubRef}, Partition, {Grains, SequenceToken},
                   undefined, Provider),
    SubStream1 = SubStream#substream{etag = NewETag, fetch_timer = FetchTimer},
    enqueue_stream(StreamRef, SubRef, Grains, SequenceToken),
    Streams1 =
        case Streams of
            #{StreamRef := SubStreams} ->
                maps:put(StreamRef, [{SubRef, SubStream1} | SubStreams], Streams);
            _ ->
                maps:put(StreamRef, [{SubRef, SubStream1}], Streams)
        end,
    {reply, ok, State#state{streams=Streams1}};
handle_call({unsubscribe, StreamRef, Grain}, _From, State=#state{streams=Streams,
                                                                 provider=Provider}) ->
    ?LOG_INFO("at=unsubscribe stream_ref=~p grain_ref=~p", [StreamRef, Grain]),
    case maps:get(StreamRef, Streams, undefined) of
        undefined ->
            %% no longer our responsibility?
            {reply, ok, State};
        SubStreams ->
            case lists:filter(fun({_Ref, #substream{grains = Grains}}) ->
                                      sets:is_element(Grain, Grains)
                              end, SubStreams) of
                %% already removed somehow I guess? is this an error?
                [] ->
                    {reply, ok, State};
                %% not even sure how to handle the case where we have more than one
                [{SubRef, SubStream =
                      #substream{grains = Grains,
                                 partition = Partition,
                                 seq_token = SeqToken,
                                 etag = ETag,
                                 fetch_timer = FetchTimer}}] ->
                    Grains1 = sets:del_element(Grain, Grains),
                    SubStream1 = SubStream#substream{grains = Grains1},
                    Streams1 =
                        case {sets:size(Grains1), length(SubStreams) - 1} of
                            %% no more subscribers, this was the only grain in the subsequence
                            {0, 0} ->
                                timer:cancel(FetchTimer),
                                delete({StreamRef, SubRef}, Provider),
                                maps:remove(StreamRef, Streams);
                            %% stream needs to be removed from subseqs and deleted
                            {0, _} ->
                                timer:cancel(FetchTimer),
                                delete({StreamRef, SubRef}, Provider),
                                SubStreams1 = lists:keydelete(SubRef, 1, SubStreams),
                                maps:put(StreamRef, SubStreams1, Streams);
                            %% stream still has subscribers, update it
                            {_, _} ->
                                NewETag = save({StreamRef, SubRef}, Partition,
                                               {Grains1, SeqToken}, ETag, Provider),
                                SubStream2 = SubStream1#substream{etag = NewETag},
                                SubStreams1 = lists:keyreplace(SubRef, 1, SubStreams,
                                                               {SubRef, SubStream2}),
                                maps:put(StreamRef, SubStreams1, Streams)
                        end,
                    {reply, ok, State#state{streams=Streams1}}
                end
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
handle_info({Refs = {_StreamRef, _SubRef}, {go, _Ref, Pid, _RelativeTime, _SojournTime}},
            State=#state{monitors=Monitors}) ->
    Ref = erlang:monitor(process, Pid),
    {noreply, State#state{monitors = Monitors#{Ref => Refs}}};
handle_info({_Stream, {drop, _SojournTime}}, State) ->
    %% should never happen... we have an infinite timeout
    {noreply, State};
handle_info({update_streams, Range}, State=#state{provider=Provider}) ->
    ?LOG_INFO("at=update_streams", []),
    %sbroker:dirty_cancel(?STREAM_BROKER, ?STREAM_TAG),
    MyStreams = enqueue_streams(Provider, Range),
    {noreply, State#state{streams = MyStreams}};
handle_info({trigger_fetch, StreamRef, SubRef}, State) ->
    Substreams = maps:get(StreamRef, State#state.streams),
    {_, #substream{grains = Subscribers,
                   seq_token = SequenceToken}} =
        lists:keyfind(SubRef, 1, Substreams),
    enqueue_stream(StreamRef, SubRef, Subscribers, SequenceToken),
    {noreply, State};
handle_info(gc_streams, State) ->
    %% we should actually do something here, really.

    %% - the primary concern is that streams don't actively merge,
    %%   since the stream ref includes the sequence token, so we need
    %%   to iterate through all of them and merge them when we can.
    start_gc_timer(),
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
                    lists:foldl(fun({{StreamRef, SubRef}, erleans_stream, ETag,
                                     {Subscribers, SequenceToken}}, Acc1) ->
                                    enqueue_stream(StreamRef, SubRef, Subscribers, SequenceToken),
                                    #{fetch_interval := FetchInterval} = StreamRef,
                                    {ok, FetchTimer} = timer:send_interval(FetchInterval,
                                                                           {trigger_fetch, StreamRef, SubRef}),
                                    SubStream = #substream{grains = Subscribers,
                                                           seq_token = SequenceToken,
                                                           etag = ETag,
                                                           fetch_timer = FetchTimer,
                                                           partition = Partition},
                                    case Acc1 of
                                        #{StreamRef := SubStreams} ->
                                            Acc1#{StreamRef := [{SubRef, SubStream} | SubStreams]};
                                        _ ->
                                            Acc1#{StreamRef => [{SubRef, SubStream}]}
                                    end
                                end, Acc, Streams)
                end, #{}, lists:seq(Start, Stop)).

handle_down_agent(MonitorRef, Monitors, Streams) ->
    {{Stream, SubRef}, Monitors1} = maps:take(MonitorRef, Monitors),
    ?LOG_INFO("at=DOWN stream_ref=~p", [{Stream, SubRef}]),
    SubStreams = maps:get(Stream, Streams),
    {_, #substream{seq_token = SeqToken, grains = Subscribers}}
        = lists:keyfind(SubRef, 1, SubStreams),
    enqueue_stream(Stream, SubRef, Subscribers, SeqToken),
    Monitors1.

-spec enqueue_stream(erleans:stream_ref(), reference(), sets:set(erleans:grain_ref()), term()) ->
                            {await, any(), pid()} | {drop, 0}.
enqueue_stream(StreamRef, SubRef, Subscribers, SequenceToken) ->
    sbroker:async_ask(?STREAM_BROKER, {StreamRef, SubRef, Subscribers, SequenceToken},
                      {self(), {StreamRef, SubRef}}).

save(Id, Partition, Value, undefined, {ProviderModule, ProviderName}) ->
    ETag = erlang:phash2(Value),
    ProviderModule:insert(erleans_stream, ProviderName, Id, Partition, Value, ETag),
    ETag;
save(Id, Partition, Value, OldETag, {ProviderModule, ProviderName}) ->
    ETag = erlang:phash2(Value),
    ProviderModule:update(erleans_stream, ProviderName, Id, Partition, Value, OldETag, ETag),
    ETag.

delete(Id, {ProviderModule, ProviderName}) ->
    ProviderModule:delete(erleans_stream, ProviderName, Id).

start_gc_timer() ->
    GCInterval = erleans_config:get(stream_gc_interval, timer:seconds(120)),
    erlang:send_after(GCInterval, self(), gc_streams).
