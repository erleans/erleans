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
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_conn_manager).

-behaviour(gen_server).

-export([start_link/4,
         get_connection/1,
         done/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-dialyzer({nowarn_function, enqueue_connection/3}).
-dialyzer({nowarn_function, connect/5}).

-record(state, {mod       :: module(),
                conn_args :: list(),
                conns     :: #{pid() => reference() | undefined},
                monitors  :: #{reference() => pid()},
                broker    :: sbroker:broker(),
                pool_size :: integer(),
                backoff   :: backoff:backoff()}).

-include("erleans.hrl").

-spec start_link(module(), atom(), integer(), list()) -> {ok, pid()} | {error, any()}.
start_link(Mod, Broker, Size, Args) ->
    gen_server:start_link({local, Broker}, ?MODULE, [Mod, Broker, Size, Args], []).

get_connection(Broker) ->
    case sbroker:ask(?broker(Broker)) of
        {go, _Ref, Pid, _RelativeTime, _SojournTime} ->
            Pid;
        {drop, _N} ->
            {error, timeout}
    end.


done(Broker, Pid) ->
    enqueue_connection(Broker, Pid, whereis(Broker)),
    gen_server:cast(Broker, {done, Pid}).

init([Mod, Broker, Size, Args]) ->
    erlang:process_flag(trap_exit, true),
    B = backoff:init(1000, 10000, self(), backoff_fired),
    {ok, #state{mod=Mod,
                conns=#{},
                monitors=#{},
                broker=Broker,
                conn_args=Args,
                pool_size=Size,
                backoff=B}, 0}.

handle_call(_, _From, State) ->
    {noreply, State}.

handle_cast({done, Pid}, State=#state{mod=Mod,
                                      conns=Conns}) ->
    case maps:get(Pid, Conns, undefined) of
        undefined ->
            %% should never happen. just close this maybe open connection
            catch Mod:close(Pid),
            {noreply, State#state{conns=Conns#{Pid => undefined}}};
        Monitor ->
            erlang:demonitor(Monitor, [flush]),
            {noreply, State#state{conns=Conns#{Pid => undefined}}}
    end.

handle_info({Conn, {go, _Ref, Pid, _, _}}, State=#state{conns=Conns,
                                                        monitors=Monitors}) ->
    Mon = erlang:monitor(process, Pid),
    {noreply, State#state{conns=Conns#{Conn => Mon},
                          monitors=Monitors#{Mon => Conn}}};
handle_info({Conn, {drop, _}}, State=#state{broker=Broker,
                                           conns=Conns}) ->
    enqueue_connection(Broker, Conn, self()),
    {noreply, State#state{conns=Conns#{Conn => undefined}}};
handle_info({'DOWN', Ref, process, _Pid, _}, State=#state{mod=Mod,
                                                          conns=Conns,
                                                          monitors=Monitors}) ->
    erlang:demonitor(Ref, [flush]),
    {Conn, Monitors1} = maps:take(Ref, Monitors),
    %% closing will trigger the handle_info EXIT call to handle reconnecting
    Mod:close(Conn),
    {noreply, State#state{conns=maps:remove(Conn, Conns),
                          monitors=Monitors1}};
handle_info({'EXIT', Pid, _}, State=#state{mod=Mod,
                                           conn_args=Args,
                                           conns=Conns,
                                           broker=Broker,
                                           backoff=B}) ->
    cancel(Broker, Pid),
    {B1, Conns1} = connect(Mod, Args, Broker, Conns, B),
    {noreply, State#state{conns=Conns1,
                          backoff=B1}};
handle_info(timeout, State=#state{mod=Mod,
                                  broker=Broker,
                                  conn_args=Args,
                                  conns=Conns,
                                  pool_size=Size,
                                  backoff=B}) ->
    {B2, Conns1} = lists:foldl(fun(_, {BAcc, ConnsAcc}) ->
                                   connect(Mod, Args, Broker, ConnsAcc, BAcc)
                              end, {B, Conns}, lists:seq(1, Size)),
    {noreply, State#state{conns=Conns1,
                          backoff=B2}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%%%

connect(Mod, Args, Broker, Conns, B) ->
    case Mod:connect(Args) of
        {ok, Pid} ->
            erlang:link(Pid),
            {await, _, _} = enqueue_connection(Broker, Pid, self()),
            {_, B1} = backoff:succeed(B),
            {B1, Conns#{Pid => undefined}};
        _ ->
            _ = backoff:fire(B),
            {_, B1} = backoff:fail(B),
            {B1, Conns}
    end.

cancel(Broker, Tag) ->
    sbroker:dirty_cancel(?broker(Broker), Tag).

enqueue_connection(Broker, Pid, ToPid) ->
    sbroker:async_ask_r(?broker(Broker), Pid, {ToPid, Pid}).
