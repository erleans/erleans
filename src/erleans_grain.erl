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
%%% @doc The Grain behavior provides a process that handles registering
%%%      the grain in the cluster, loading grain state and saving grain state.
%%%
%%%      If a provider is specified in the configuration the corresponding ref
%%%      state is loaded using the provider configured data store.
%%%
%%%      A single_activation grain will attempt to have only one living process in the
%%%      cluster at any given time, while a stateless grain is meant as a read
%%%      only cache process and may spawn many instances for the same ref
%%%      depending on incoming requests and configuration.
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_grain).

-behaviour(gen_statem).

-export([start_link/1,
         call/2,
         call/3,
         cast/2,
         subscribe/2,
         subscribe/3,
         unsubscribe/2]).

-export([init/1,
         callback_mode/0,
         active/3,
         deactivating/3,
         terminate/3,
         code_change/4]).

-include("erleans.hrl").

-define(DEFAULT_TIMEOUT, 5000).
-define(NO_PROVIDER_ERROR, no_provider_configured).

-type cb_state() :: term() | #{persistent := term(),
                               ephemeral  := term()}.

-type action() :: save_state | {save, any()}.

-callback activate(Ref :: erleans:grain_ref(), Arg :: term()) ->
    {ok, Data :: cb_state(), opts()} |
    ignore |
    {stop, Reason :: term()}.

-callback provider() -> module().

-callback placement() -> erleans:grain_placement().

-callback handle_call(Msg :: term(), From :: pid(), CbData :: cb_state()) ->
    {reply, Reply :: term(), CbData :: cb_state()} |
    {reply, Reply :: term(), CbData :: cb_state(), [action()]} |
    {stop, Reason :: term(), Reply :: term(), CbData :: cb_state()}.

-callback handle_cast(Msg :: term(), CbData :: cb_state()) ->
    {noreply, CbData :: cb_state()} |
    {save, CbData :: cb_state()} |
    {save, Updates :: maps:map(), CbData :: cb_state()}.

-callback handle_info(Msg :: term(), CbData :: cb_state()) ->
    {noreply, CbData :: cb_state()} |
    {save, CbData :: cb_state()} |
    {save, Updates :: maps:map(), CbData :: cb_state()}.

-callback deactivate(CbData :: cb_state()) ->
    {ok, Data :: cb_state()} |
    {save, Data :: cb_state()} |
    {save, Updates :: maps:map(), CbData :: cb_state()}.

-optional_callbacks([provider/0,
                     placement/0]).

-record(data,
       { cb_module            :: module(),
         cb_state             :: cb_state(),

         id                   :: term(),
         etag                 :: integer(),
         provider             :: term(),
         ref                  :: erleans:grain_ref(),
         create_time          :: non_neg_integer(),
         deactivate_after     :: non_neg_integer() | infinity,
         deactivating = false :: boolean()
       }).

-type opts() :: #{ref    => binary(),
                  etag   => integer(),

                  deactivate_after => non_neg_integer() | infinity
                 }.

-export_types([opts/0]).

-spec start_link(GrainRef :: erleans:grain_ref()) -> {ok, pid()} | {error, any()}.
start_link(GrainRef = #{placement := {stateless, _N}}) ->
    {ok, Pid} = gen_statem:start_link(?MODULE, [GrainRef], []),
    gproc:reg(?stateless_counter(GrainRef)),
    gproc:reg_other(?stateless(GrainRef), Pid),
    {ok, Pid};
start_link(GrainRef) ->
    gen_statem:start_link({via, erleans_pm, GrainRef}, ?MODULE, [GrainRef], []).

subscribe(StreamProvider, Topic) ->
    %% 0 is going to be the most common initial token, but subscribe/3
    %% allows the token to be overridden when it is not 0, or not an
    %% integer.
    subscribe(StreamProvider, Topic, 0).

subscribe(StreamProvider, Topic, SequenceToken) ->
    StreamRef = erleans:get_stream(StreamProvider, Topic),
    MyGrain = get(grain_ref),
    erleans_stream_manager:subscribe(StreamRef, MyGrain, SequenceToken).

unsubscribe(StreamProvider, Topic) ->
    StreamRef = erleans:get_stream(StreamProvider, Topic),
    MyGrain = get(grain_ref),
    erleans_stream_manager:unsubscribe(StreamRef, MyGrain).

-spec call(GrainRef :: erleans:grain_ref(), Request :: term()) -> Reply :: term().
call(GrainRef, Request) ->
    call(GrainRef, Request, ?DEFAULT_TIMEOUT).

-spec call(GrainRef :: erleans:grain_ref(), Request :: term(), non_neg_integer() | infinity) -> Reply :: term().
call(GrainRef, Request, Timeout) ->
    ReqType = req_type(),
    do_for_ref(GrainRef, fun(Pid) ->
                             try
                                 gen_statem:call(Pid, {ReqType, Request}, Timeout)
                             catch
                                 exit:{bad_etag, _} ->
                                     lager:error("at=grain_exit reason=bad_etag", []),
                                     {exit, saved_etag_changed}
                             end
                         end).

-spec cast(GrainRef :: erleans:grain_ref(), Request :: term()) -> Reply :: term().
cast(GrainRef, Request) ->
    ReqType = req_type(),
    do_for_ref(GrainRef, fun(Pid) -> gen_statem:cast(Pid, {ReqType, Request}) end).

req_type() ->
    case get(req_type) of
        undefined ->
            refresh_timer;
        ReqType ->
            ReqType
    end.

do_for_ref(GrainPid, Fun) when is_pid(GrainPid) ->
    Fun(GrainPid);
do_for_ref(GrainRef=#{placement := {stateless, _N}}, Fun) ->
    case erleans_stateless:pick_grain(GrainRef) of
        {ok, Pid} when is_pid(Pid) ->
            Fun(Pid);
        _ ->
            exit(timeout)
    end;
do_for_ref(GrainRef, Fun) ->
    try
        case erleans_pm:whereis_name(GrainRef) of
            Pid when is_pid(Pid) ->
                Fun(Pid);
            undefined ->
                lager:info("start=~p", [GrainRef]),
                case activate_grain(GrainRef) of
                    {ok, Pid} ->
                        Fun(Pid);
                    {error, {already_started, Pid}} ->
                        Fun(Pid);
                    {error, Error} ->
                        exit({noproc, Error})
                end
        end
    catch
        %% Retry only if the process deactivated
        exit:{Reason, _} when Reason =:= {shutdown, deactivated}
                            ; Reason =:= normal ->
            do_for_ref(GrainRef, Fun)
    end.

activate_grain(GrainRef=#{placement := Placement}) ->
    case Placement of
        {stateless, N} ->
            activate_stateless(GrainRef, N);
        stateless ->
            activate_stateless(GrainRef, erleans_config:get(max_stateless));
        prefer_local ->
            activate_local(GrainRef);
        random ->
            activate_random(GrainRef)
        %% load ->
        %%  load placement
    end.

%% Stateless are always activated on the local node if <N exist already on the node
activate_stateless(GrainRef, _N) ->
    erleans_grain_sup:start_child(node(), GrainRef).

%% Activate on the local node
activate_local(GrainRef) ->
    erleans_grain_sup:start_child(node(), GrainRef).

%% Activate the grain on a random node in the cluster
activate_random(GrainRef) ->
    {ok, Members} = partisan_peer_service:members(),
    Size = erlang:length(Members),
    Nth = rand:uniform(Size),
    Node = lists:nth(Nth, Members),
    erleans_grain_sup:start_child(Node, GrainRef).

init([GrainRef=#{id := Id,
                 implementing_module := CbModule}]) ->
    put(grain_ref, GrainRef),
    process_flag(trap_exit, true),
    {CbData, ETag} = case maps:find(provider, GrainRef) of
                          {ok, Provider={ProviderModule, ProviderName}} ->
                              case ProviderModule:read(CbModule, ProviderName, Id) of
                                  {ok, SavedData, E} ->
                                      {SavedData, E};
                                  _ ->
                                      {#{}, undefined}
                              end;
                          {ok, undefined} ->
                              Provider = undefined,
                              {#{}, undefined};
                          error ->
                              Provider = undefined,
                              {#{}, undefined}
                      end,

    maybe_enqueue_grain(GrainRef),

    case CbData of
        notfound ->
            {stop, notfound};
        _ ->
            case erlang:function_exported(CbModule, activate, 2) of
                false ->
                    CbData1 = CbData,
                    GrainOpts = #{},
                    init_(GrainRef, CbModule, Id, Provider, ETag, GrainOpts, CbData1);
                true ->
                    case CbModule:activate(GrainRef, CbData) of
                        {ok, CbData1, GrainOpts} ->
                            init_(GrainRef, CbModule, Id, Provider, ETag, GrainOpts, CbData1);
                        {stop, Reason} ->
                            {stop, Reason}
                    end

            end
    end.

init_(GrainRef, CbModule, Id, Provider, ETag, GrainOpts, CbData1) ->
    {CbData2, ETag1} = verify_etag(CbModule, Id, Provider, ETag, CbData1),
    CreateTime = maps:get(create_time, GrainOpts, erlang:system_time(seconds)),
    DeactivateAfter = maps:get(deactivate_after, GrainOpts, erleans_config:get(deactivate_after)),
    Data = #data{cb_module        = CbModule,
                 cb_state         = CbData2,

                 id               = Id,
                 etag             = ETag1,
                 provider         = Provider,
                 ref              = GrainRef,
                 create_time      = CreateTime,
                 deactivate_after = case DeactivateAfter of 0 -> infinity; _ -> DeactivateAfter end
                },
    {ok, active, Data}.

callback_mode() ->
    state_functions.

active({call, From}, {ReqType, Msg}, Data=#data{cb_module=CbModule,
                                                cb_state=CbData,
                                                deactivate_after=DeactivateAfter}) ->
    handle_reply(CbModule:handle_call(Msg, From, CbData), From, Data, upd_timer(ReqType, DeactivateAfter));
active(cast, {ReqType, Msg}, Data=#data{cb_module=CbModule,
                                        cb_state=CbData,
                                        deactivate_after=DeactivateAfter}) ->
    handle_reply(CbModule:handle_cast(Msg, CbData), Data, upd_timer(ReqType, DeactivateAfter));
active(EventType, Event, Data) ->
    handle_event(EventType, Event, active, Data).

deactivating(_EventType, {refresh_timer, _}, Data) ->
    erleans_timer:recover(),
    {next_state, active, Data, [postpone]};
deactivating(EventType, Event, Data) ->
    handle_event(EventType, Event, deactivating, Data).

handle_event(state_timeout, activation_expiry, _, Data) ->
    finalize_and_stop(Data, true);
handle_event(_, check_timers, active, _) ->
    keep_state_and_data;
handle_event(_, check_timers, _, Data) ->
    case erleans_timer:check() of
        finished ->
            finalize_and_stop(Data, false);
        pending ->
            %% we can't get here with a reply pending
            erlang:send_after(50, self(), check_timers),
            keep_state_and_data
    end;
handle_event(_, {erleans_grain_tag, {go, _, _, _, _}}, _, _Data) ->
    %% checked out
    keep_state_and_data;
handle_event(_, {erleans_grain_tag, {drop, _}}, _, _Data) ->
    %% dropped from queue
    keep_state_and_data;
handle_event(_, {cancel_timer, _Pid, _TimeLeft}, _, _Data) ->
    %% filter these out
    keep_state_and_data;
handle_event(_, Message, _, Data=#data{cb_module=CbModule,
                                       cb_state=CbData}) ->
    handle_reply(CbModule:handle_info(Message, CbData), Data, []).

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate({shutdown, deactivated}, _State, _Data) ->
    ok;
terminate(?NO_PROVIDER_ERROR, _State, #data{cb_module=CbModule,
                                            id=Id}) ->
    lager:error("attempted to save without storage provider configured: id=~p cb_module=~p", [Id, CbModule]),
    %% We do not want to call the deactivate callback here because this
    %% is not a deactivation, it is a hard crash.
    ok;
terminate(Reason, _State, Data) ->
    lager:info("at=terminate reason=~p", [Reason]),
    %% supervisor is terminating, node is probably shutting down.
    %% deactivate the grain so it can clean up and save if needed
    _ = finalize_and_stop(Data, false),
    ok.

%% Internal functions

upd_timer(leave_timer, _) ->
    [];
upd_timer(refresh_timer, DeactivateAfter) ->
    [{state_timeout, DeactivateAfter, activation_expiry}].

%% If a recoverable stop (meaning we are stopping because the activation expired)
%% first cancel timers and wait until all currently running callbacks have completed
%%
%% Note: a grain returning `stop` is treated as non-recoverable. Because grains are
%% automatically started when referenced stopping doesn't make much sense in the
%% traditional sense. Maybe it should be removed or at least renamed and maybe act as
%% recoverable?
finalize_and_stop(Data, _Recoverable=true) ->
    %% first, cancel all timers, see if anything is ticking, if no,
    %% then we need to wait to see if we can shut down
    erleans_timer:recoverable_cancel(),
    case erleans_timer:check() of
        finished ->
            finalize_and_stop(Data, false);
        pending ->
            %% we can't get here with a reply pending
            erlang:send_after(50, self(), check_timers),
            {next_state, deactivating, Data}
    end;
finalize_and_stop(Data=#data{cb_module=CbModule,
                             id=Id,
                             ref=Ref,
                             provider=Provider,
                             cb_state=CbData,
                             etag=ETag}, false) ->
    %% Save to or delete from backing storage.
    erleans_pm:unregister_name(Ref, self()),
    case CbModule:deactivate(CbData) of
        {save, NewCbData} ->
            NewETag = replace_state(CbModule, Provider, Id, NewCbData, ETag),
            {stop, {shutdown, deactivated}, Data#data{cb_state=NewCbData,
                                                      etag=NewETag}};
        {ok, NewCbData} ->
            {stop, {shutdown, deactivated}, Data#data{cb_state=NewCbData}}
    end.

handle_reply(Reply, Data=#data{ref=GrainRef}, Actions) ->
    maybe_enqueue_grain(GrainRef),
    handle_reply_(Reply, Data, Actions).

handle_reply(Reply, From, Data=#data{ref=GrainRef}, Actions) ->
    maybe_enqueue_grain(GrainRef),
    handle_reply_(Reply, From, Data, Actions).

handle_reply_({reply, Reply, NewCbData}, From, Data, Actions) ->
    {keep_state, Data#data{cb_state=NewCbData}, [{reply, From, Reply} | Actions]};
handle_reply_({save_reply, Reply, Updates, NewCbData}, From, Data=#data{id=Id,
                                                                        cb_module=CbModule,
                                                                        provider=Provider,
                                                                        etag=ETag}, Actions)
  when is_map(Updates) ->
    %% Saving requires an etag so it can verify no other process has updated the row before us.
    %% The actor needs to crash in the case that the etag found in the table has chnaged.
    NewETag = update_state(CbModule, Provider, Id, Updates, NewCbData, ETag),
    {keep_state, Data#data{cb_state=NewCbData, etag=NewETag}, [{reply, From, Reply} | Actions]};
handle_reply_({save_reply, Reply, NewCbData}, From, Data=#data{id=Id,
                                                               cb_module=CbModule,
                                                               provider=Provider,
                                                               etag=ETag}, Actions) ->
    NewETag = replace_state(CbModule, Provider, Id, NewCbData, ETag),
    {keep_state, Data#data{cb_state=NewCbData, etag=NewETag}, [{reply, From, Reply} | Actions]};
handle_reply_({stop, _Reason, Reply, NewCbData}, From, Data, Actions) ->
    {stop, Reason, NewData} = finalize_and_stop(Data#data{cb_state=NewCbData}, false),
    {stop_and_reply, Reason, [{reply, From, Reply} | Actions], NewData};
handle_reply_({stop, NewCbData}, _From, Data, _Actions) ->
    finalize_and_stop(Data#data{cb_state=NewCbData}, false).

handle_reply_({noreply, NewCbData}, Data, Actions) ->
    {keep_state, Data#data{cb_state=NewCbData}, Actions};
handle_reply_({stop, NewCbData}, Data, _Actions) ->
    finalize_and_stop(Data#data{cb_state=NewCbData}, false);
handle_reply_({save, Updates, NewCbData}, Data=#data{id=Id,
                                                     cb_module=CbModule,
                                                     provider=Provider,
                                                     etag=ETag}, Actions) when is_map(Updates) ->
    NewETag = update_state(CbModule, Provider, Id, Updates, NewCbData, ETag),
    {keep_state, Data#data{cb_state=NewCbData, etag=NewETag}, Actions};
handle_reply_({save, NewCbData}, Data=#data{id=Id,
                                            cb_module=CbModule,
                                            provider=Provider,
                                            etag=ETag}, Actions) ->
    NewETag = replace_state(CbModule, Provider, Id, NewCbData, ETag),
    {keep_state, Data#data{cb_state=NewCbData, etag=NewETag}, Actions}.

%% Saving requires an etag so it can verify no other process has updated the row before us.
%% The actor needs to crash in the case that the etag found in the table has changed.
update_state(_CbModule, undefined, _Id, _Updates, _CbData, _ETag) ->
    exit(?NO_PROVIDER_ERROR);
update_state(CbModule, Provider, Id, Updates, #{persistent := PData,
                                                ephemeral  := _}, ETag) ->
    NewETag = etag(PData),
    update_state_(CbModule, Provider, Id, Updates, ETag, NewETag);
update_state(CbModule, Provider, Id, Updates, CbData, ETag) ->
    NewETag = etag(CbData),
    update_state_(CbModule, Provider, Id, Updates, ETag, NewETag).

update_state_(CbModule, {Provider, ProviderName}, Id, Updates, ETag, NewETag) ->
    case Provider:update(CbModule, ProviderName, Id, Updates, ETag, NewETag) of
        ok ->
            NewETag;
        {error, Reason} ->
            exit(Reason)
    end.

replace_state(_CbModule, undefined, _Id, _CbData, _ETag) ->
    exit(?NO_PROVIDER_ERROR);
replace_state(CbModule, Provider, Id, #{persistent := PData,
                                        ephemeral  := _}, ETag) ->
    NewETag = etag(PData),
    replace_state(CbModule, Provider, Id, PData, ETag, NewETag);
replace_state(CbModule, Provider, Id, CbData, ETag) ->
    NewETag = etag(CbData),
    replace_state(CbModule, Provider, Id, CbData, ETag, NewETag).

replace_state(CbModule, {Provider, ProviderName}, Id, Data, ETag, NewETag) ->
    case Provider:replace(CbModule, ProviderName, Id, Data, ETag, NewETag) of
        ok ->
            NewETag;
        {error, Reason} ->
            exit(Reason)
    end.

maybe_enqueue_grain(GrainRef = #{placement := {stateless, _}}) ->
    erleans_stateless:enqueue_grain(GrainRef, self());
maybe_enqueue_grain(_) ->
    ok.

verify_etag(CbModule, Id, {Provider, ProviderName}, undefined, CbData=#{persistent := PData,
                                                                        ephemeral  := _}) ->
    ETag = etag(PData),
    Provider:insert(CbModule, ProviderName, Id, PData, ETag),
    {CbData, ETag};
verify_etag(CbModule, Id, {Provider, ProviderName}, undefined, CbData) ->
    ETag = etag(CbData),
    Provider:insert(CbModule, ProviderName, Id, CbData, ETag),
    {CbData, ETag};
verify_etag(_, _, _, ETag, CbData) ->
    {CbData, ETag}.

etag(Data) ->
    erlang:phash2(Data).
