%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
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
         cast/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("erleans.hrl").

-define(DEFAULT_TIMEOUT, 5000).

-type cb_state() :: term() | #{persistent := term(),
                               ephemeral  := term()}.

-callback init(Arg :: term()) ->
    {ok, State :: cb_state()} |
    {ok, State :: cb_state(), timeout() | hibernate} |
    ignore |
    {stop, Reason :: term()}.

-callback provider() -> module().

-callback placement() -> erleans:grain_placement().

-callback handle_call(Msg :: term(), From :: pid(), CbState :: cb_state()) ->
    {reply, Reply :: term(), CbState :: cb_state()} |
    {stop, Reply :: term(), Reason :: term()}.

-callback handle_cast(Msg :: term(), CbState :: cb_state()) ->
    {noreply, CbState :: cb_state()} |
    {save, CbState :: cb_state()} |
    {save, Updates :: maps:map(), CbState :: cb_state()}.

-callback handle_info(Msg :: term(), CbState :: cb_state()) ->
    {noreply, CbState :: cb_state()} |
    {save, CbState :: cb_state()} |
    {save, Updates :: maps:map(), CbState :: cb_state()}.

-callback eval_timer(CbState :: cb_state()) ->
    {ok, State :: cb_state()} |
    {stop, Reason :: term()}.

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
         tref                  :: reference(),
         create_time           :: non_neg_integer(),
         lease_time            :: non_neg_integer() | infinity,
         life_time             :: non_neg_integer() | infinity,
         eval_timeout_interval :: non_neg_integer()
       }).

%% -type grain_opts() :: #{ref    := binary(),
%%                         etag   := integer(),

%%                         lease_time => non_neg_integer() | infinity,
%%                         life_time => non_neg_integer() | infinity,
%%                         eval_timeout_interval => non_neg_integer() | infinity}.

-spec start_link(GrainRef :: erleans:grain_ref()) -> {ok, pid()} | {error, any()}.
start_link(GrainRef = #{placement := {stateless, _N}}) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [GrainRef], []),
    gproc:reg(?stateless_counter(GrainRef)),
    gproc:reg_other(?stateless(GrainRef), Pid),
    {ok, Pid};
start_link(GrainRef) ->
    gen_server:start_link({via, erleans_pm, GrainRef}, ?MODULE, [GrainRef], []).

-spec call(GrainRef :: erleans:grain_ref(), Request :: term()) -> Reply :: term().
call(GrainRef, Request) ->
    call(GrainRef, Request, ?DEFAULT_TIMEOUT).

-spec call(GrainRef :: erleans:grain_ref(), Request :: term(), non_neg_integer() | infinity) -> Reply :: term().
call(GrainRef, Request, Timeout) ->
    do_for_ref(GrainRef, fun(Pid) ->
                             try
                                 gen_server:call(Pid, Request, Timeout)
                             catch
                                 exit:{{bad_etag, E1, E2}, _} ->
                                     lager:error("at=grain_exit reason=bad_etag initial_etag=~p saved_etag=~p",
                                                 [E1, E2]),
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
    case erleans_pm:whereis_name(GrainRef) of
        Pid when is_pid(Pid) ->
            Fun(Pid);
        undefined ->
            lager:info("start=~p", [GrainRef]),
            case activate_grain(GrainRef) of
                {ok, Pid} ->
                    Fun(Pid);
                _ ->
                    exit(noproc)
            end
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
    process_flag(trap_exit, true),
    {CbState, ETag} = case maps:find(provider, GrainRef) of
                          {ok, Provider} ->
                              case Provider:read(Id) of
                                  {ok, SavedState, E} ->
                                      {SavedState, E};
                                  _ ->
                                      {#{}, undefined}
                              end;
                          error ->
                              Provider = undefined,
                              {#{}, undefined}
                      end,

    maybe_enqueue_grain(GrainRef),

    case CbState of
        notfound ->
            {stop, notfound};
        _ ->
            case erlang:function_exported(CbModule, init, 1) of
                false ->
                    CbState1 = CbState,
                    GrainOpts = #{};
                true ->
                    {ok, CbState1, GrainOpts} = CbModule:init(CbState)
            end,

            {CbState2, ETag1} = verify_etag(Id, Provider, ETag, CbState1),
            CreateTime = maps:get(create_time, GrainOpts, erlang:system_time(seconds)),
            LeaseTime = maps:get(lease_time, GrainOpts, erleans_config:get(default_lease_time)),
            LifeTime = maps:get(life_time, GrainOpts, erleans_config:get(default_life_time)),
            EvalTimeoutInterval = maps:get(eval_timeout_interval, GrainOpts, erleans_config:get(default_eval_interval)),
            TRef = erlang:start_timer(EvalTimeoutInterval, self(), eval_timeout),
            State = #state{cb_module   = CbModule,
                           cb_state    = CbState2,

                           id          = Id,
                           etag        = ETag1,
                           provider    = Provider,
                           ref         = GrainRef,
                           tref        = TRef,
                           create_time = CreateTime,
                           lease_time  = LeaseTime,
                           life_time   = LifeTime,
                           eval_timeout_interval=EvalTimeoutInterval},
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
handle_info({timeout, _TRef, eval_timeout}, State=#state{cb_module=CbModule,
                                                         cb_state=CbState,
                                                         eval_timeout_interval=EvalTimeoutInterval,
                                                         lease_time=LeaseTime,
                                                         life_time=LifeTime,
                                                         create_time=CreateTime,
                                                         tref=TRef}) ->
    erlang:cancel_timer(TRef),
    case CbModule:eval_timer(CbState) of
        {ok, NewCbState} when LifeTime =:= 0 ->
            NewTRef = erlang:start_timer(EvalTimeoutInterval, self(), eval_timeout),
            {noreply, State#state{cb_state=NewCbState,
                                  tref=NewTRef}, lease_time(LeaseTime, EvalTimeoutInterval)};
        {ok, NewCbState} ->
            Now = erlang:system_time(seconds),
            case Now - CreateTime of
                N when N >= LifeTime ->
                    finalize_and_stop(State#state{cb_state=NewCbState});
                _ ->
                    NewTRef = erlang:start_timer(EvalTimeoutInterval, self(), eval_timeout),
                    {noreply, State#state{cb_state=NewCbState,
                                          tref=NewTRef}, lease_time(LeaseTime, EvalTimeoutInterval)}
            end;
        {stop, NewCbState} ->
            finalize_and_stop(State#state{cb_state=NewCbState})
    end;
handle_info(Message, State=#state{cb_module=CbModule,
                                  cb_state=CbState}) ->
    handle_reply(CbModule:handle_info(Message, CbState), State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, _State) ->
    lager:info("at=terminate reason=~p", [Reason]),
    ok.

%% Internal functions

finalize_and_stop(State=#state{cb_module=CbModule,
                               id=Id,
                               ref=Ref,
                               provider=Provider,
                               cb_state=CbState,
                               etag=ETag}) ->
    %% Save to or delete from backing storage.
    %% erleans_pm:unregister_name()
    case CbModule:deactivate(CbState) of
        {save, NewCbState} ->
            NewETag = replace_state(Provider, Id, NewCbState, ETag),
            erleans_pm:unregister_name(Ref, self()),
            {stop, normal, State#state{cb_state=NewCbState,
                                       etag=NewETag}};
        {ok, NewCbState} ->
            {stop, normal, State#state{cb_state=NewCbState}}
    end.

handle_reply(Reply, State=#state{ref=GrainRef}) ->
    maybe_enqueue_grain(GrainRef),
    handle_reply_(Reply, State).

handle_reply_({stop, Reply, NewCbState}, State) ->
    {stop, Reason, NewState} = finalize_and_stop(State#state{cb_state=NewCbState}),
    {stop, Reason, Reply, NewState};
handle_reply_({stop, NewCbState}, State) ->
    finalize_and_stop(State#state{cb_state=NewCbState});
handle_reply_({reply, Reply, NewCbState}, State=#state{lease_time=LeaseTime}) ->
    {reply, Reply, State#state{cb_state=NewCbState}, lease_time(LeaseTime)};
handle_reply_({noreply, NewCbState}, State=#state{lease_time=LeaseTime}) ->
    {noreply, State#state{cb_state=NewCbState}, lease_time(LeaseTime)};
handle_reply_({save_reply, Reply, Updates, NewCbState}, State=#state{id=Id,
                                                                     lease_time=LeaseTime,
                                                                     provider=Provider,
                                                                     etag=ETag})
  when is_map(Updates) ->
    %% Saving requires an etag so it can verify no other process has updated the row before us.
    %% The actor needs to crash in the case that the etag found in the table has chnaged.
    NewETag = update_state(Provider, Id, Updates, NewCbState, ETag),
    {reply, Reply, State#state{cb_state=NewCbState,
                               etag=NewETag}, lease_time(LeaseTime)};
handle_reply_({save_reply, Reply, NewCbState}, State=#state{id=Id,
                                                            provider=Provider,
                                                            lease_time=LeaseTime,
                                                            etag=ETag}) ->
    NewETag = replace_state(Provider, Id, NewCbState, ETag),
    {reply, Reply, State#state{cb_state=NewCbState,
                               etag=NewETag}, lease_time(LeaseTime)};
handle_reply_({save, Updates, NewCbState}, State=#state{id=Id,
                                                        lease_time=LeaseTime,
                                                        provider=Provider,
                                                        etag=ETag}) when is_map(Updates) ->
    NewETag = update_state(Provider, Id, Updates, NewCbState, ETag),
    {noreply, State#state{cb_state=NewCbState,
                          etag=NewETag}, lease_time(LeaseTime)};
handle_reply_({save, NewCbState}, State=#state{id=Id,
                                               provider=Provider,
                                               lease_time=LeaseTime,
                                               etag=ETag}) ->
    NewETag = replace_state(Provider, Id, NewCbState, ETag),
    {noreply, State#state{cb_state=NewCbState,
                          etag=NewETag}, lease_time(LeaseTime)}.

%% Saving requires an etag so it can verify no other process has updated the row before us.
%% The actor needs to crash in the case that the etag found in the table has changed.
update_state(Provider, Id, Updates, #{persistent := PState,
                                      ephemeral  := _}, ETag) ->
    NewETag = etag(PState),
    update_state_(Provider, Id, Updates, ETag, NewETag);
update_state(Provider, Id, Updates, CbState, ETag) ->
    NewETag = etag(CbState),
    update_state_(Provider, Id, Updates, ETag, NewETag).

update_state_(Provider, Id, Updates, ETag, NewETag) ->
    case Provider:update(Id, Updates, ETag, NewETag) of
        ok ->
            NewETag;
        {error, Reason} ->
            exit(Reason)
    end.

replace_state(Provider, Id, #{persistent := PState,
                              ephemeral  := _}, ETag) ->
    NewETag = etag(PState),
    replace_state(Provider, Id, PState, ETag, NewETag);
replace_state(Provider, Id, CbState, ETag) ->
    NewETag = etag(CbState),
    replace_state(Provider, Id, CbState, ETag, NewETag).

replace_state(Provider, Id, State, ETag, NewETag) ->
    case Provider:replace(Id, State, ETag, NewETag) of
        ok ->
            NewETag;
        {error, Reason} ->
            exit(Reason)
    end.

lease_time(0) -> infinity;
lease_time(X) -> X.

lease_time(0, _) -> infinity;
lease_time(L, I) -> L - I.

maybe_enqueue_grain(GrainRef = #{placement := {stateless, _}}) ->
    erleans_stateless:enqueue_grain(GrainRef, self());
maybe_enqueue_grain(_) ->
    ok.

verify_etag(Id, Provider, undefined, CbState=#{persistent := PState,
                                               ephemeral  := _}) ->
    ETag = etag(PState),
    Provider:insert(Id, PState, ETag),
    {CbState, ETag};
verify_etag(Id, Provider, undefined, CbState) ->
    ETag = etag(CbState),
    Provider:insert(Id, CbState, ETag),
    {CbState, ETag};
verify_etag(_, _, ETag, CbState) ->
    {CbState, ETag}.

etag(Data) ->
    erlang:phash2(Data).
