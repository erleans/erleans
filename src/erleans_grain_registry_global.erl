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
%%% @doc Erleans Grain registry backed by Erlang's global.
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_grain_registry_global).

-export([register_name/2,
         unregister_name/1,
         unregister_name/2,
         whereis_name/1,
         send/2]).

-include("erleans.hrl").

-dialyzer({nowarn_function, register_name/2}).
-dialyzer({nowarn_function, unregister_name/2}).

-spec register_name(Name :: erleans:grain_ref(), Pid :: pid()) -> yes | no.
register_name(Name, Pid) when is_pid(Pid) ->
    case global:register_name(Name, Pid) of
        yes ->
            yes;
        _ ->
            no
    end.

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
    case gproc:where(?stateful(GrainRef)) of
        Pid when is_pid(Pid) ->
            Pid;
        _ ->
            global:whereis_name(GrainRef)
    end.

-spec send(Name :: erleans:grain_ref(), Message :: term()) -> term().
send(Name, Message) ->
    case whereis_name(Name) of
        Pid when is_pid(Pid) ->
            Pid ! Message;
        undefined ->
            error({badarg, Name})
    end.

whereis_stateless(GrainRef) ->
    case gproc_pool:pick_worker(GrainRef) of
        false ->
            undefined;
        Pid ->
            Pid
    end.
