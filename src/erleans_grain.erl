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

-behaviour(gen_server).

-export([start_link/1,
         call/2,
         call/3,
         cast/2,
         subscribe/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("erleans.hrl").

-define(DEFAULT_TIMEOUT, 5000).
-define(NO_PROVIDER_ERROR, no_provider_configured).

-type cb_state() :: term() | #{persistent := term(),
                               ephemeral  := term()}.

-callback init(Ref :: erleans:grain_ref(), Arg :: term()) ->
    {ok, State :: cb_state(), opts()} |
    ignore |
    {stop, Reason :: term()}.

-callback provider() -> module().

-callback placement() -> erleans:grain_placement().

-callback handle_call(Msg :: term(), From :: pid(), CbState :: cb_state()) ->
    {reply, Reply :: term(), CbState :: cb_state()} |
    {stop, Reason :: term(), Reply :: term(), CbState :: cb_state()}.

-callback handle_cast(Msg :: term(), CbState :: cb_state()) ->
    {noreply, CbState :: cb_state()} |
    {save, CbState :: cb_state()} |
    {save, Updates :: maps:map(), CbState :: cb_state()}.

-callback handle_info(Msg :: term(), CbState :: cb_state()) ->
    {noreply, CbState :: cb_state()} |
    {save, CbState :: cb_state()} |
    {save, Updates :: maps:map(), CbState :: cb_state()}.

-callback deactivate(CbState :: cb_state()) ->
    {ok, State :: cb_state()} |
    {save, State :: cb_state()} |
    {save, Updates :: maps:map(), CbState :: cb_state()}.

-optional_callbacks([provider/0,
                     placement/0]).

-record(state,
       { cb_module             :: module(),
         cb_state              :: cb_state(),

         id                    :: term(),
         etag                  :: integer(),
         provider              :: term(),
         ref                   :: erleans:grain_ref(),
         create_time           :: non_neg_integer(),
         lease_time            :: non_neg_integer() | infinity
       }).

-type opts() :: #{ref    => binary(),
                  etag   => integer(),

                  lease_time => non_neg_integer() | infinity,
                  life_time => non_neg_integer() | infinity
                 }.

-export_types([opts/0]).

-spec start_link(GrainRef :: erleans:grain_ref()) -> {ok, pid()} | {error, any()}.
start_link(GrainRef = #{placement := {stateless, _N}}) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [GrainRef], []),
    gproc:reg(?stateless_counter(GrainRef)),
    gproc:reg_other(?stateless(GrainRef), Pid),
    {ok, Pid};
start_link(GrainRef) ->
    gen_server:start_link({via, erleans_pm, GrainRef}, ?MODULE, [GrainRef], []).

subscribe(StreamProvider, Topic) ->
    StreamRef = erleans:get_stream(StreamProvider, Topic),
    MyGrain = get(grain_ref),
    erleans_stream_manager:subscribe(StreamRef, MyGrain).

-spec call(GrainRef :: erleans:grain_ref(), Request :: term()) -> Reply :: term().
call(GrainRef, Request) ->
    call(GrainRef, Request, ?DEFAULT_TIMEOUT).

-spec call(GrainRef :: erleans:grain_ref(), Request :: term(), non_neg_integer() | infinity) -> Reply :: term().
call(GrainRef, Request, Timeout) ->
    do_for_ref(GrainRef, fun(Pid) ->
                             try
                                 gen_server:call(Pid, Request, Timeout)
                             catch
                                 exit:{bad_etag, _} ->
                                     lager:error("at=grain_exit reason=bad_etag", []),
                                     {exit, saved_etag_changed}
                             end
                         end).

-spec cast(GrainRef :: erleans:grain_ref(), Request :: term()) -> Reply :: term().
cast(GrainRef, Request) ->
    do_for_ref(GrainRef, fun(Pid) -> gen_server:cast(Pid, Request) end).

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
                    Err ->
                        exit({noproc, Err})
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
    {CbState, ETag} = case maps:find(provider, GrainRef) of
                          {ok, Provider={ProviderModule, ProviderName}} ->
                              case ProviderModule:read(CbModule, ProviderName, Id) of
                                  {ok, SavedState, E} ->
                                      {SavedState, E};
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

    case CbState of
        notfound ->
            {stop, notfound};
        _ ->
            case erlang:function_exported(CbModule, init, 2) of
                false ->
                    CbState1 = CbState,
                    GrainOpts = #{};
                true ->
                    {ok, CbState1, GrainOpts} = CbModule:init(GrainRef, CbState)
            end,
            {CbState2, ETag1} = verify_etag(CbModule, Id, Provider, ETag, CbState1),
            CreateTime = maps:get(create_time, GrainOpts, erlang:system_time(seconds)),
            LeaseTime = maps:get(lease_time, GrainOpts, erleans_config:get(default_lease_time)),
            State = #state{cb_module   = CbModule,
                           cb_state    = CbState2,

                           id          = Id,
                           etag        = ETag1,
                           provider    = Provider,
                           ref         = GrainRef,
                           create_time = CreateTime,
                           lease_time  = LeaseTime
                          },
            {ok, State, lease_time(LeaseTime)}
    end.

handle_call(Msg, From, State=#state{cb_module=CbModule,
                                    cb_state=CbState}) ->
    handle_reply(CbModule:handle_call(Msg, From, CbState), State).

handle_cast(Msg, State=#state{cb_module=CbModule,
                              cb_state=CbState}) ->
    handle_reply(CbModule:handle_cast(Msg, CbState), State).

handle_info(timeout, State) ->
    %% Lease expired
    finalize_and_stop(State);
handle_info({erleans_grain_tag, {go, _, _, _, _}}, State) ->
    %% checked out
    {noreply, State};
handle_info({erleans_grain_tag, {drop, _}}, State) ->
    %% dropped from queue
    {noreply, State};
handle_info(Message, State=#state{cb_module=CbModule,
                                  cb_state=CbState}) ->
    handle_reply(CbModule:handle_info(Message, CbState), State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate({shutdown, deactivated}, _State) ->
    ok;
terminate(?NO_PROVIDER_ERROR, #state{cb_module=CbModule,
                                     id=Id}) ->
    lager:error("attempted to save without storage provider configured: id=~p cb_module=~p", [Id, CbModule]),
    %% We do not want to call the deactivate callback here because this
    %% is not a deactivation, it is a hard crash.
    ok;
terminate(Reason, State) ->
    lager:info("at=terminate reason=~p", [Reason]),
    %% supervisor is terminating, node is probably shutting down.
    %% deactivate the grain so it can clean up and save if needed
    _ = finalize_and_stop(State),
    ok.

%% Internal functions

finalize_and_stop(State=#state{cb_module=CbModule,
                               id=Id,
                               ref=Ref,
                               provider=Provider,
                               cb_state=CbState,
                               etag=ETag}) ->
    %% Save to or delete from backing storage.
    erleans_pm:unregister_name(Ref, self()),
    case CbModule:deactivate(CbState) of
        {save, NewCbState} ->
            NewETag = replace_state(CbModule, Provider, Id, NewCbState, ETag),
            {stop, {shutdown, deactivated}, State#state{cb_state=NewCbState,
                                                        etag=NewETag}};
        {ok, NewCbState} ->
            {stop, {shutdown, deactivated}, State#state{cb_state=NewCbState}}
    end.

handle_reply(Reply, State=#state{ref=GrainRef}) ->
    maybe_enqueue_grain(GrainRef),
    handle_reply_(Reply, State).

handle_reply_({stop, _Reason, Reply, NewCbState}, State) ->
    {stop, Reason, NewState} = finalize_and_stop(State#state{cb_state=NewCbState}),
    {stop, Reason, Reply, NewState};
handle_reply_({stop, NewCbState}, State) ->
    finalize_and_stop(State#state{cb_state=NewCbState});
handle_reply_({reply, Reply, NewCbState}, State=#state{lease_time=LeaseTime}) ->
    {reply, Reply, State#state{cb_state=NewCbState}, lease_time(LeaseTime)};
handle_reply_({noreply, NewCbState}, State=#state{lease_time=LeaseTime}) ->
    {noreply, State#state{cb_state=NewCbState}, lease_time(LeaseTime)};
handle_reply_({save_reply, Reply, Updates, NewCbState}, State=#state{id=Id,
                                                                     cb_module=CbModule,
                                                                     lease_time=LeaseTime,
                                                                     provider=Provider,
                                                                     etag=ETag})
  when is_map(Updates) ->
    %% Saving requires an etag so it can verify no other process has updated the row before us.
    %% The actor needs to crash in the case that the etag found in the table has chnaged.
    NewETag = update_state(CbModule, Provider, Id, Updates, NewCbState, ETag),
    {reply, Reply, State#state{cb_state=NewCbState,
                               etag=NewETag}, lease_time(LeaseTime)};
handle_reply_({save_reply, Reply, NewCbState}, State=#state{id=Id,
                                                            cb_module=CbModule,
                                                            provider=Provider,
                                                            lease_time=LeaseTime,
                                                            etag=ETag}) ->
    NewETag = replace_state(CbModule, Provider, Id, NewCbState, ETag),
    {reply, Reply, State#state{cb_state=NewCbState,
                               etag=NewETag}, lease_time(LeaseTime)};
handle_reply_({save, Updates, NewCbState}, State=#state{id=Id,
                                                        cb_module=CbModule,
                                                        lease_time=LeaseTime,
                                                        provider=Provider,
                                                        etag=ETag}) when is_map(Updates) ->
    NewETag = update_state(CbModule, Provider, Id, Updates, NewCbState, ETag),
    {noreply, State#state{cb_state=NewCbState,
                          etag=NewETag}, lease_time(LeaseTime)};
handle_reply_({save, NewCbState}, State=#state{id=Id,
                                               cb_module=CbModule,
                                               provider=Provider,
                                               lease_time=LeaseTime,
                                               etag=ETag}) ->
    NewETag = replace_state(CbModule, Provider, Id, NewCbState, ETag),
    {noreply, State#state{cb_state=NewCbState,
                          etag=NewETag}, lease_time(LeaseTime)}.

%% Saving requires an etag so it can verify no other process has updated the row before us.
%% The actor needs to crash in the case that the etag found in the table has changed.
update_state(_CbModule, undefined, _Id, _Updates, _CbState, _ETag) ->
    exit(?NO_PROVIDER_ERROR);
update_state(CbModule, Provider, Id, Updates, #{persistent := PState,
                                                ephemeral  := _}, ETag) ->
    NewETag = etag(PState),
    update_state_(CbModule, Provider, Id, Updates, ETag, NewETag);
update_state(CbModule, Provider, Id, Updates, CbState, ETag) ->
    NewETag = etag(CbState),
    update_state_(CbModule, Provider, Id, Updates, ETag, NewETag).

update_state_(CbModule, {Provider, ProviderName}, Id, Updates, ETag, NewETag) ->
    case Provider:update(CbModule, ProviderName, Id, Updates, ETag, NewETag) of
        ok ->
            NewETag;
        {error, Reason} ->
            exit(Reason)
    end.

replace_state(_CbModule, undefined, _Id, _CbState, _ETag) ->
    exit(?NO_PROVIDER_ERROR);
replace_state(CbModule, Provider, Id, #{persistent := PState,
                                        ephemeral  := _}, ETag) ->
    NewETag = etag(PState),
    replace_state(CbModule, Provider, Id, PState, ETag, NewETag);
replace_state(CbModule, Provider, Id, CbState, ETag) ->
    NewETag = etag(CbState),
    replace_state(CbModule, Provider, Id, CbState, ETag, NewETag).

replace_state(CbModule, {Provider, ProviderName}, Id, State, ETag, NewETag) ->
    case Provider:replace(CbModule, ProviderName, Id, State, ETag, NewETag) of
        ok ->
            NewETag;
        {error, Reason} ->
            exit(Reason)
    end.

lease_time(0) -> infinity;
lease_time(X) -> X.

maybe_enqueue_grain(GrainRef = #{placement := {stateless, _}}) ->
    erleans_stateless:enqueue_grain(GrainRef, self());
maybe_enqueue_grain(_) ->
    ok.

verify_etag(CbModule, Id, {Provider, ProviderName}, undefined, CbState=#{persistent := PState,
                                                                         ephemeral  := _}) ->
    ETag = etag(PState),
    Provider:insert(CbModule, ProviderName, Id, PState, ETag),
    {CbState, ETag};
verify_etag(CbModule, Id, {Provider, ProviderName}, undefined, CbState) ->
    ETag = etag(CbState),
    Provider:insert(CbModule, ProviderName, Id, CbState, ETag),
    {CbState, ETag};
verify_etag(_, _, _, ETag, CbState) ->
    {CbState, ETag}.

etag(Data) ->
    erlang:phash2(Data).
