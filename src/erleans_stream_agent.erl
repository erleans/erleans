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
-module(erleans_stream_agent).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-include("erleans.hrl").

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    sbroker:async_ask_r(?STREAM_BROKER),
    {ok, {}}.

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info( {_, {go, _Ref, {Stream=#{stream_module   := StreamModule,
                                      topic           := Topic}, SubRef, Subscribers, Offset},
                  _RelativeTime, _SojournTime}}, State) ->
    lager:info("at=fetch_start topic=~p stream_mod=~p offset=~p",
               [Topic, StreamModule, Offset]),
    case StreamModule:fetch([{Topic, Offset}]) of
        %% I'm not sure this is a good idea, but eases races?
        [{error, not_found}] ->
            lager:warning("error=nonexistent_topic"),
            NewOffset = Offset;
        [] ->
            lager:info("no new records"),
            NewOffset = Offset;
        [{Topic, {NewOffset, []}}] ->
            lager:info("no new records");
        [{Topic, {NewOffset, RecordSet}}] ->
            ec_plists:foreach(fun(G) ->
                                  erleans_grain:call(G, {stream, Topic, RecordSet})
                              end, sets:to_list(Subscribers))
    end,

    erleans_stream_manager:next(Stream, SubRef, NewOffset),
    lager:info("at=fetch_finished"),
    sbroker:async_ask_r(?STREAM_BROKER),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.


%%
