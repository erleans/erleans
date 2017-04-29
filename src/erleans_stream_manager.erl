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
         find_node/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% can remove these when bug in sbroker specs is fixed
-dialyzer({nowarn_function, handle_down_agent/3}).
-dialyzer({nowarn_function, enqueue_if_node/5}).
-dialyzer({nowarn_function, enqueue_stream/2}).

-include("erleans.hrl").

-define(TIMEOUT, 10000). %% check what streams to be running after 10 seconds of a node change

-record(state,
        {ring     :: hash_ring:ring() | undefined,
         streams  :: #{erleans:stream_ref() => sets:set(erleans:grain_ref())},
         monitors :: #{reference() => erleans:stream_ref()},
         provider :: {module(), atom()},
         tref     :: reference() | undefined
        }).

-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

next(StreamRef, SequenceToken) ->
    gen_server:call(?MODULE, {next, StreamRef, SequenceToken}).

subscribe(StreamRef, GrainRef) ->
    {ok, Node} = find_node(StreamRef),
    gen_server:call({?MODULE, Node}, {subscribe, StreamRef, GrainRef}).

find_node(Stream) ->
    gen_server:call(?MODULE, {find_node, Stream}).

init([]) ->
    %% callback for changes to cluster membership
    Self = self(),
    partisan_peer_service_events:add_sup_callback(fun(Members) ->
                                                      Self ! {update, Members}
                                                  end),

    %% storage provider for stream metadata
    Provider = erleans_stream:provider(),
    ProviderOptions = proplists:get_value(Provider, erleans_config:get(providers, [])),
    Module = proplists:get_value(module, ProviderOptions),

    {ok, #state{streams=#{},
                provider={Module, Provider},
                monitors=#{}}, 0}.

handle_call({next, StreamRef, SequenceToken}, _From={FromPid, _Tag}, State=#state{monitors=Monitors,
                                                                                  provider=Provider,
                                                                                  streams=Streams}) ->
    lager:info("at=next stream_ref=~p sequence_token=~p", [StreamRef, SequenceToken]),
    Stream1 = StreamRef#{sequence_token => SequenceToken},
    {CurrentSubs, ETag} = maps:get(StreamRef, Streams, {sets:new(), undefined}),
    NewETag = save(StreamRef, CurrentSubs, ETag, Provider),
    Streams1 = Streams#{Stream1 => {CurrentSubs, NewETag}},

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
handle_call({subscribe, StreamRef, Grain}, _From, State=#state{streams=Streams,
                                                               provider=Provider}) ->
    lager:info("at=subscribe stream_ref=~p grain_ref=~p", [StreamRef, Grain]),
    {NewSubscribers, OldETag} = case maps:get(StreamRef, Streams, undefined) of
                                    undefined ->
                                        {sets:add_element(Grain, sets:new()), undefined};
                                    {Subscribers, ETag} ->
                                        {sets:add_element(Grain, Subscribers), ETag}
                                end,
    NewETag = save(StreamRef, NewSubscribers, OldETag, Provider),
    Streams1 = maps:put(StreamRef, {NewSubscribers, NewETag}, Streams),
    {reply, ok, State#state{streams=Streams1}};
handle_call({unsubscribe, StreamRef, Grain}, _From, State=#state{streams=Streams,
                                                                 provider=Provider}) ->
    lager:info("at=unsubscribe stream_ref=~p grain_ref=~p", [StreamRef, Grain]),
    case maps:get(StreamRef, Streams, undefined) of
        undefined ->
            %% no longer our responsibility?
            {reply, ok, State};
        {Subscribers, ETag} ->
            Subscribers1 = sets:del_element(Grain, Subscribers),
            NewETag = save(StreamRef, Subscribers1, ETag, Provider),
            Streams1 = maps:put(StreamRef, {Subscribers1, NewETag}, Streams),
            {reply, ok, State#state{streams=Streams1}}
    end;
handle_call({find_node, StreamRef}, From, State=#state{ring=Ring}) ->
    spawn(fun() ->
              case hash_ring:find_node(StreamRef, Ring) of
                  {ok, RingNode} ->
                      Node = hash_ring_node:get_key(RingNode),
                      gen_server:reply(From, {ok, Node});
                  error ->
                      gen_server:reply(From, error)
              end
          end),
    {noreply, State}.

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
handle_info(update_streams, State=#state{ring=Ring,
                                         provider={ProviderModule, Provider}}) ->
    lager:info("at=update_streams", []),
    {ok, Streams} = ProviderModule:all(erleans_stream, Provider),

    %% TODO: oh, so inefficient
    %sbroker:dirty_cancel(?STREAM_BROKER, ?STREAM_TAG),
    MyStreams = lists:foldl(fun({StreamRef, erleans_stream, ETag, Subscribers}, Acc) ->
                                enqueue_if_node(StreamRef, Subscribers, ETag, Ring, Acc)
                            end, #{}, Streams),
    {noreply, State#state{streams = MyStreams,
                          tref = undefined}};
handle_info({update, Membership}, State=#state{tref=TRef}) ->
    cancel_timer(TRef),
    TRef1 = erlang:send_after(?TIMEOUT, self(), update_streams, []),
    %% convert partisan membership to list of erlang nodes
    Members = [P || {P, _, _} <- sets:to_list(state_orset:query(Membership))],
    NewRing = make_ring(Members),
    {noreply, State#state{ring = NewRing,
                          tref = TRef1}};
handle_info(timeout, State) ->
    TRef = erlang:send_after(?TIMEOUT, self(), update_streams, []),
    Ring = make_ring(),
    {noreply, State#state{ring = Ring,
                          tref = TRef}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal functions

cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    erlang:cancel_timer(TRef, []).

make_ring() ->
    {ok, Nodes} = partisan_peer_service:members(),
    make_ring(Nodes).

make_ring(Nodes) ->
    lager:info("current_members=~p", [Nodes]),
    Members = hash_ring:list_to_nodes(Nodes),
    hash_ring:make(Members, [{module, hash_ring_static}]).

handle_down_agent(MonitorRef, Monitors, Streams) ->
    {Stream, Monitors1} = maps:take(MonitorRef, Monitors),
    {Subscribers, _ETag} = maps:get(Stream, Streams),
    lager:info("at=DOWN stream_ref=~p", [Stream]),
    enqueue_stream(Stream, Subscribers),
    Monitors1.

enqueue_if_node(StreamRef, Subscribers, ETag, Ring, Acc) ->
    case hash_ring:find_node(StreamRef, Ring) of
        {ok, RingNode} ->
            case hash_ring_node:get_key(RingNode) of
                Key when Key =:= node() ->
                    enqueue_stream(StreamRef, Subscribers),
                    Acc#{StreamRef => {Subscribers, ETag}};
                _ ->
                    Acc
            end;
        _ ->
            Acc
    end.

-spec enqueue_stream(erleans:stream_ref(), sets:set(erleans:grain_ref())) -> {await, any(), pid()} | {drop, 0}.
enqueue_stream(StreamRef, Subscribers) ->
    sbroker:async_ask(?STREAM_BROKER, {StreamRef, Subscribers}, {self(), StreamRef}).

save(Id, Value, undefined, {ProviderModule, ProviderName}) ->
    ETag = erlang:phash2(Value),
    ProviderModule:insert(erleans_stream, ProviderName, Id, Value, ETag),
    ETag;
save(Id, Value, OldETag, {ProviderModule, ProviderName}) ->
    ETag = erlang:phash2(Value),
    ProviderModule:replace(erleans_stream, ProviderName, Id, Value, OldETag, ETag),
    ETag.
