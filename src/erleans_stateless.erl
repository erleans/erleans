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

-module(erleans_stateless).

-export([pick_grain/2]).

-include("erleans.hrl").

-define(STATELESS_WAIT, 1000).

-spec pick_grain(erleans:grain_ref(), function()) -> {ok, term()} | {error, timeout}.
pick_grain(GrainRef = #{placement := {stateless, N}}, Fun) ->
    try gproc_pool:claim(?pool(GrainRef), Fun, nowait) of
        {true, Res} ->
            {ok, Res};
        false ->
            %% check if the number of activations is less than the max allowed
            case gproc_pool:active_workers(?pool(GrainRef)) of
                W when length(W) < N ->
                    %% fewer grains activated than the max allowed
                    %% so create a new one
                    {ok, _Pid} = erleans_grain_sup:start_child(node(), GrainRef),
                    claim_with_wait(GrainRef, Fun);
                _ ->
                    claim_with_wait(GrainRef, Fun)
            end
    catch
        error:badarg ->
            %% no pool started for these grains, let's start one
            gproc_pool:new(?pool(GrainRef), claim, [{autosize, true}]),
            %% does not support going over N during bursts
            %% TODO: revisit burst support
            %% spawn a new activation and use it
            {ok, _Pid} = erleans_grain_sup:start_child(node(), GrainRef),
            claim_with_wait(GrainRef, Fun)
    end.

claim_with_wait(GrainRef, Fun) ->
    case gproc_pool:claim(?pool(GrainRef), Fun, {busy_wait, ?STATELESS_WAIT}) of
        {true, Res} ->
            {ok, Res};
        false ->
            {error, timeout}
    end.
