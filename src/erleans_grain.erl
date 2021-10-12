%%%----------------------------------------------------------------------------
%%% Copyright Tristan Sloughter 2019. All Rights Reserved.
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
         cast/2]).

-export([init/1,
         init/2,
         callback_mode/0,
         active/3,
         deactivating/3,
         terminate/3,
         code_change/4]).

-include("erleans.hrl").
-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_TIMEOUT, 5000).
-define(NO_PROVIDER_ERROR, no_provider_configured).

-type cb_state() :: {EphemeralState :: term(), PersistentState :: term()} | term().

-type from() :: {pid(), term()}.

-type action() :: {reply, From :: from(), Reply :: term()} |
                  {cast, Msg :: term()} |
                  {info, Msg :: term()} |
                  save_state.

-callback provider() -> module().

-callback placement() -> erleans:grain_placement().

-callback state(Id :: term()) -> term().

-callback activate(Ref :: erleans:grain_ref(), Arg :: term()) -> {ok, Data :: cb_state(), opts()} |
                                                                 {error, Reason :: term()}.

-type callback_result() :: {ok, CbData :: cb_state()} |
                           {ok, CbData :: cb_state(), [action()]} |
                           {deactivate, CbData :: cb_state()} |
                           {deactivate, CbData :: cb_state(), [action()]}.

-callback handle_call(Msg :: term(), From :: from(), CbData :: cb_state()) -> callback_result().

-callback handle_cast(Msg :: term(), CbData :: cb_state()) -> callback_result().

-callback handle_info(Msg :: term(), CbData :: cb_state()) -> callback_result().

-callback deactivate(CbData :: cb_state()) -> {ok, CbData :: cb_state()} |
                                              {save_state, CbData :: cb_state()}.

-optional_callbacks([activate/2,
                     provider/0,
                     placement/0,
                     state/1,
                     handle_info/2,
                     deactivate/1]).

-record(data,
       { cb_module            :: module(),
         cb_state             :: cb_state(),

         id                   :: term(),
         etag                 :: integer(),
         provider             :: term(),
         ref                  :: erleans:grain_ref(),
         create_time          :: non_neg_integer(),
         deactivate_after     :: non_neg_integer() | infinity
       }).

-type opts() :: #{ref    => binary(),
                  etag   => integer(),

                  deactivate_after => non_neg_integer() | infinity
                 }.

-export_types([opts/0]).

-spec start_link(GrainRef :: erleans:grain_ref()) -> {ok, pid() | undefined} | {error, any()}.
start_link(GrainRef) ->
    proc_lib:start_link(?MODULE, init, [self(), GrainRef]).

-spec call(GrainRef :: erleans:grain_ref(), Request :: term()) -> Reply :: term().
call(GrainRef, Request) ->
    call(GrainRef, Request, ?DEFAULT_TIMEOUT).

-spec call(GrainRef :: erleans:grain_ref(), Request :: term(), non_neg_integer() | infinity) -> Reply :: term().
call(GrainRef, Request, Timeout) ->
    ReqType = req_type(),
    do_for_ref(GrainRef, fun(_, Pid) ->
                                 try
                                     gen_statem:call(Pid, {otel_tracer:current_span_ctx(), ReqType, Request}, Timeout)
                                 catch
                                     exit:{bad_etag, _} ->
                                         ?LOG_ERROR("at=grain_exit reason=bad_etag", []),
                                         {exit, saved_etag_changed}
                                 end
                         end).

-spec cast(GrainRef :: erleans:grain_ref(), Request :: term()) -> Reply :: term().
cast(GrainRef, Request) ->
    ReqType = req_type(),
    do_for_ref(GrainRef, fun(_, Pid) -> gen_statem:cast(Pid, {otel_tracer:current_span_ctx(), ReqType, Request}) end).

req_type() ->
    case get(req_type) of
        undefined ->
            refresh_timer;
        ReqType ->
            ReqType
    end.

do_for_ref(GrainPid, Fun) when is_pid(GrainPid) ->
    Fun(noname, GrainPid);
do_for_ref(GrainRef=#{placement := {stateless, _N}}, Fun) ->
    case erleans_stateless:pick_grain(GrainRef, Fun) of
        {ok, Res} ->
            Res;
        _ ->
            exit(timeout)
    end;
do_for_ref(GrainRef, Fun) ->
    try
        case erleans_pm:whereis_name(GrainRef) of
            Pid when is_pid(Pid) ->
                Fun(noname, Pid);
            undefined ->
                ?LOG_INFO("start=~p", [GrainRef]),
                case activate_grain(GrainRef) of
                    {ok, undefined} ->
                        %% the only way the Pid could be `undefined`
                        %% is an ignore from the statem, which can
                        %% only happen for `{error, notfound}`
                        exit({noproc, notfound});
                    {ok, Pid} ->
                        Fun(noname, Pid);
                    {error, {already_started, Pid}} ->
                        Fun(noname, Pid);
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
    erleans_grain_sup:start_child(GrainRef).

%% Activate on the local node
activate_local(GrainRef) ->
    erleans_grain_sup:start_child(GrainRef).

%% Activate the grain on a random node in the cluster
activate_random(GrainRef) ->
    {ok, Members} = partisan_peer_service:members(),
    Size = erlang:length(Members),
    Nth = rand:uniform(Size),
    Node = lists:nth(Nth, Members),
    erleans_grain_sup:start_child(Node, GrainRef).

%% not used but required by the behaviour definition
init(Args) -> erlang:error(not_implemented, [Args]).

init(Parent, GrainRef) ->
    put(grain_ref, GrainRef),
    process_flag(trap_exit, true),

    case GrainRef of
        #{placement := {stateless, _N}} ->
            init_(Parent, GrainRef);
        _->
            Self = self(),
            %% use a node local gproc registration to ensure no duplicates can
            %% be made on a single node.
            case gproc:reg_or_locate(?stateful(GrainRef), Self) of
                {_, Pid} when Pid =/= Self ->
                    proc_lib:init_ack(Parent, {error, {already_started, Pid}});
                {_, _Pid} ->
                    erleans_pm:register_name(GrainRef, Self),
                    init_(Parent, GrainRef)
            end
    end.

init_(Parent, GrainRef=#{id := Id,
                         implementing_module := CbModule}) ->
    {CbData, ETag} = case maps:find(provider, GrainRef) of
                          {ok, Provider={ProviderModule, ProviderName}} ->
                              case ProviderModule:read(CbModule, ProviderName, Id) of
                                  {ok, SavedData, E} ->
                                      {SavedData, E};
                                  _ ->
                                      new_state(CbModule, Id)
                              end;
                         {ok, undefined} ->
                             Provider = undefined,
                             new_state(CbModule, Id);
                         error ->
                             Provider = undefined,
                             new_state(CbModule, Id)
                      end,

    maybe_add_worker(GrainRef),

    case CbData of
        notfound ->
            proc_lib:init_ack(Parent, ignore);
        _ ->
            case erleans_utils:fun_or_default(CbModule, activate, 2, [GrainRef, CbData], {ok, CbData, #{}}) of
                {ok, CbData1, GrainOpts} ->
                    verify_and_enter_loop(Parent, GrainRef, CbModule, Id, Provider, ETag, GrainOpts, CbData1);
                {error, notfound} ->
                    %% activate returning {error, notfound} is given special treatment and
                    %% results in an ignore from the statem and an `exit({noproc, notfound})`
                    %% from `erleans_grain`
                    proc_lib:init_ack(Parent, ignore);
                {error, Reason} ->
                    proc_lib:init_ack(Parent, {error, Reason})
            end
    end.

new_state(CbModule, Id) ->
    case erlang:function_exported(CbModule, state, 1) of
        true ->
            {CbModule:state(Id), undefined};
        false ->
            {#{}, undefined}
    end.

verify_and_enter_loop(Parent, GrainRef, CbModule, Id, Provider, ETag, GrainOpts, CbData1) ->
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
    proc_lib:init_ack(Parent, {ok, self()}),
    gen_statem:enter_loop(?MODULE, [], active, Data).

callback_mode() ->
    [state_functions, state_enter].

active(enter, _OldState, Data=#data{deactivate_after=DeactivateAfter}) ->
    {keep_state, Data, [{state_timeout, DeactivateAfter, activation_expiry}]};
active({call, From}, {undefined, ReqType, Msg}, Data=#data{cb_module=CbModule,
                                                           cb_state=CbData,
                                                           deactivate_after=DeactivateAfter}) ->
    handle_result(CbModule:handle_call(Msg, From, CbData), Data, upd_timer(ReqType, DeactivateAfter));
active({call, From}, {SpanCtx, ReqType, Msg}, Data=#data{cb_module=CbModule,
                                                         cb_state=CbData,
                                                         deactivate_after=DeactivateAfter}) ->
    otel_tracer:start_span(opentelemetry:get_application_tracer(?MODULE),span_name(Msg), #{parent => SpanCtx}),
    otel_span:set_attribute(otel_tracer:current_span_ctx(), <<"grain_msg">>, io_lib:format("~p", [Msg])),
    try handle_result(CbModule:handle_call(Msg, From, CbData), Data, upd_timer(ReqType, DeactivateAfter))
    after
        otel_span:end_span(otel_tracer:current_span_ctx(), opentelemetry:timestamp())
    end;
active(cast, {undefined, ReqType, Msg}, Data=#data{cb_module=CbModule,
                                                   cb_state=CbData,
                                                   deactivate_after=DeactivateAfter}) ->
    handle_result(CbModule:handle_cast(Msg, CbData), Data, upd_timer(ReqType, DeactivateAfter));
active(cast, {SpanCtx, ReqType, Msg}, Data=#data{cb_module=CbModule,
                                                 cb_state=CbData,
                                                 deactivate_after=DeactivateAfter}) ->
    otel_tracer:start_span(opentelemetry:get_application_tracer(?MODULE),span_name(Msg), #{parent => SpanCtx}),
    otel_span:set_attribute(otel_tracer:current_span_ctx(), <<"grain_msg">>, io_lib:format("~p", [Msg])),
    try handle_result(CbModule:handle_cast(Msg, CbData), Data, upd_timer(ReqType, DeactivateAfter))
    after
        otel_span:end_span(otel_tracer:current_span_ctx(), opentelemetry:timestamp())
    end;
active(state_timeout, activation_expiry, Data) ->
    {next_state, deactivating, Data};
active(EventType, Event, Data) ->
    handle_event(EventType, Event, active, Data).

deactivating(enter, _OldState, Data) ->
    %% cancel all timers, then maybe stop
    erleans_timer:recoverable_cancel(),
    timer_check(Data);
deactivating(state_timeout, check_timers, Data) ->
    timer_check(Data);
deactivating(_EventType, {_, refresh_timer, _}, Data) ->
    %% restart the timers
    erleans_timer:recover(),
    %% this event refreshes the time, which means we are active again
    %% so simply postpone the event to be handled in the active state
    {next_state, active, Data, [postpone]};
deactivating(EventType, Event, Data) ->
    handle_event(EventType, Event, deactivating, Data).

% see if anything is ticking, if yes, then we need to wait to see if we can shut down
timer_check(Data) ->
    case erleans_timer:check() of
        finished ->
            finalize_and_stop(Data);
        pending ->
            {keep_state, Data, [{state_timeout, 50, check_timers}]}
    end.

handle_event(_, {cancel_timer, _Pid, _TimeLeft}, _, _Data) ->
    %% filter these out
    keep_state_and_data;
handle_event(info, {'EXIT', _, Reason}, _, Data) ->
    {stop, {shutdown, Reason}, Data};
handle_event(_, Message, _, Data=#data{cb_module=CbModule,
                                       cb_state=CbData}) ->
    Reply = erleans_utils:fun_or_default(CbModule, handle_info, 2, [Message, CbData], {ok, CbData}),
    handle_result(Reply, Data, []).

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate({shutdown, deactivated}, _State, #data{ref=GrainRef}) ->
    maybe_remove_worker(GrainRef),
    ok;
terminate(?NO_PROVIDER_ERROR, _State, #data{cb_module=CbModule,
                                            id=Id,
                                            ref=GrainRef}) ->
    maybe_remove_worker(GrainRef),
    ?LOG_ERROR("attempted to save without storage provider configured: id=~p cb_module=~p", [Id, CbModule]),
    %% We do not want to call the deactivate callback here because this
    %% is not a deactivation, it is a hard crash.
    ok;
terminate(Reason, _State, Data=#data{ref=GrainRef}) ->
    maybe_remove_worker(GrainRef),
    ?LOG_INFO("at=terminate reason=~p", [Reason]),
    %% supervisor is terminating, node is probably shutting down.
    %% deactivate the grain so it can clean up and save if needed
    _ = finalize_and_stop(Data),
    ok.

%% Internal functions

maybe_add_worker(GrainRef=#{placement := {stateless, _}}) ->
    gproc_pool:add_worker(?pool(GrainRef), self()),
    gproc_pool:connect_worker(?pool(GrainRef), self());
maybe_add_worker(_) ->
    ok.

maybe_remove_worker(GrainRef=#{placement := {stateless, _}}) ->
    gproc_pool:disconnect_worker(?pool(GrainRef), self()),
    gproc_pool:remove_worker(?pool(GrainRef), self());
maybe_remove_worker(_) ->
    ok.

maybe_unregister(#{placement := {stateless, _}}) ->
    ok;
maybe_unregister(GrainRef) ->
    erleans_pm:unregister_name(GrainRef, self()),
    gproc:unreg(?stateful(GrainRef)).

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
finalize_and_stop(Data=#data{cb_module=CbModule,
                             id=Id,
                             ref=Ref,
                             provider=Provider,
                             cb_state=CbData,
                             etag=ETag}) ->
    %% Save to or delete from backing storage.
    maybe_unregister(Ref),

    case erleans_utils:fun_or_default(CbModule, deactivate, 1, [CbData], {ok, CbData}) of
        {save_state, NewCbData={_, PersistentState}} ->
            NewETag = update_state(CbModule, Provider, Id, PersistentState, ETag),
            {stop, {shutdown, deactivated}, Data#data{cb_state=NewCbData,
                                                      etag=NewETag}};
        {save_state, NewCbData} ->
            NewETag = update_state(CbModule, Provider, Id, NewCbData, ETag),
            {stop, {shutdown, deactivated}, Data#data{cb_state=NewCbData,
                                                      etag=NewETag}};
        {ok, NewCbData} ->
            {stop, {shutdown, deactivated}, Data#data{cb_state=NewCbData}}
    end.

handle_result({ok, NewCbData}, Data=#data{ref=_GrainRef}, Actions) ->
    {keep_state, Data#data{cb_state=NewCbData}, Actions};
handle_result({ok, NewCbData, CbActions}, Data=#data{ref=_GrainRef}, Actions) ->
    {Actions1, Data1} = handle_actions(CbActions, Actions, NewCbData, Data),
    {keep_state, Data1#data{cb_state=NewCbData}, Actions1};
handle_result({deactivate, NewCbData}, Data, _) ->
    {next_state, deactivating, Data#data{cb_state=NewCbData}, []};
handle_result({deactivate, NewCbData, CbActions}, Data, _) ->
    {Actions1, Data1} = handle_actions(CbActions, [], NewCbData, Data),
    {next_state, deactivating, Data1#data{cb_state=NewCbData}, Actions1}.

%% Drops unrecognized actions and converts cast actions to next_events that
%% do not reset the activation timer
handle_actions([], ActionsAcc, _CbData, Data) ->
    {lists:reverse(ActionsAcc), Data};
handle_actions([{cast, Msg} | Rest], ActionsAcc, CbData, Data) ->
    handle_actions(Rest, [{next_event, cast, {leave_timer, Msg}} | ActionsAcc], CbData, Data);
handle_actions([{info, Msg} | Rest], ActionsAcc, CbData, Data) ->
    handle_actions(Rest, [{next_event, info, {leave_timer, Msg}} | ActionsAcc], CbData, Data);
handle_actions([R={reply, _, _} | Rest], ActionsAcc, CbData, Data) ->
    handle_actions(Rest, [R | ActionsAcc], CbData, Data);
handle_actions([save_state | Rest], ActionsAcc, CbData, Data) ->
    NewETag = update_state(CbData, Data),
    handle_actions(Rest, ActionsAcc, CbData, Data#data{etag=NewETag});
handle_actions([A | _Rest], _ActionsAcc, _CbData, _Data) ->
    %% unknown action, exit with reason bad_action
    exit({bad_action, A}).

update_state({_Ephemeral, Persistent}, Data) ->
    update_state(Persistent, Data);
update_state(CbData, #data{id=Id,
                           cb_module=CbModule,
                           provider=Provider,
                           etag=ETag}) ->
    update_state(CbModule, Provider, Id, CbData, ETag).

update_state(_CbModule, undefined, _Id, _CbData, _ETag) ->
    exit(?NO_PROVIDER_ERROR);
update_state(CbModule, Provider, Id, CbData, ETag) ->
    NewETag = etag(CbData),
    update_state(CbModule, Provider, Id, CbData, ETag, NewETag).

update_state(CbModule, {Provider, ProviderName}, Id, Data, ETag, NewETag) ->
    case Provider:update(CbModule, ProviderName, Id, Data, ETag, NewETag) of
        ok ->
            NewETag;
        {error, Reason} ->
            exit(Reason)
    end.

verify_etag(CbModule, Id, {Provider, ProviderName}, undefined, D={_, CbData}) ->
    ETag = etag(CbData),
    Provider:insert(CbModule, ProviderName, Id, CbData, ETag),
    {D, ETag};
verify_etag(CbModule, Id, {Provider, ProviderName}, undefined, CbData) ->
    ETag = etag(CbData),
    Provider:insert(CbModule, ProviderName, Id, CbData, ETag),
    {CbData, ETag};
verify_etag(_, _, _, ETag, CbData) ->
    {CbData, ETag}.

etag(Data) ->
    erlang:phash2(Data).

span_name(Msg) when is_tuple(Msg) ->
    element(1, Msg);
span_name(Msg) ->
    Msg.
