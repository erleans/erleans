%%%----------------------------------------------------------------------------
%%% Copyright Tristan Sloughter 2024. All Rights Reserved.
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
-module(erleans_grain_registry).

-moduledoc """
Erleans Grain registry.
""".

-include("erleans.hrl").

-export([register_name/2,
         resolve_name/3,
         unregister_name/1,
         unregister_name/2,
         whereis_name/1,
         send/2]).

-callback register_name(erleans:grain_ref(), pid()) -> yes | no.
-callback unregister_name(erleans:grain_ref()) -> ok.
-callback unregister_name(erleans:grain_ref(), pid()) -> ok.
-callback whereis_name(erleans:grain_ref()) -> pid() | undefined.

-spec register_name(Name :: erleans:grain_ref(), Pid :: pid()) -> yes | no.
register_name(Name, Pid) when is_pid(Pid) ->
    global:register_name(Name, Pid, fun ?MODULE:resolve_name/3).

-spec resolve_name(term(), pid(), pid()) -> pid().
resolve_name(_Name, Pid, Pid) ->
    Pid;
resolve_name(_Name, Pid1, Pid2) ->
    %% Choose the same owner regardless of argument order. Use an external
    %% resolver fun so global does not retain an old version of this module.
    {Winner, Loser} = case {node(Pid1), Pid1} < {node(Pid2), Pid2} of
                         true -> {Pid1, Pid2};
                         false -> {Pid2, Pid1}
                     end,
    exit(Loser, {shutdown, duplicate_activation}),
    Winner.

-spec unregister_name(Name :: erleans:grain_ref()) -> ok.
unregister_name(Name) ->
    case ?MODULE:whereis_name(Name) of
        Pid when is_pid(Pid) ->
            unregister_name(Name, Pid);
        undefined ->
            ok
    end.

-spec unregister_name(Name :: erleans:grain_ref(), Pid :: pid()) -> ok.
unregister_name(Name, _Pid) ->
    _ = global:unregister_name(Name),
    ok.

-spec whereis_name(GrainRef :: erleans:grain_ref()) -> pid() | undefined.
whereis_name(GrainRef=#{placement := stateless}) ->
    whereis_stateless(GrainRef);
whereis_name(GrainRef=#{placement := {stateless, _}}) ->
    whereis_stateless(GrainRef);
whereis_name(GrainRef) ->
    global:whereis_name(GrainRef).

-spec send(Name :: erleans:grain_ref(), Message :: term()) -> term().
send(Name, Message) ->
    case whereis_name(Name) of
        Pid when is_pid(Pid) ->
            Pid ! Message;
        undefined ->
            error({badarg, Name})
    end.

whereis_stateless(GrainRef) ->
    %% Stateless pools use claim, which pick_worker/1 does not support.
    %% Registry lookup only locates a worker; calls claim it in erleans_stateless.
    case gproc_pool:active_workers(?pool(GrainRef)) of
        [] ->
            undefined;
        [{_Name, Pid} | _] ->
            Pid
    end.
