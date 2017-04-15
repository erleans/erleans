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

-export([pick_grain/1,
         enqueue_grain/2]).

-include("erleans.hrl").

-dialyzer({nowarn_function, enqueue_grain/2}).

-spec pick_grain(erleans:grain_ref()) -> {ok, pid()} | {error, timeout}.
pick_grain(GrainRef = #{placement := {stateless, N}}) ->
    try
        %% ask but don't enqueue the ask
        case sbroker:nb_ask(?broker(GrainRef)) of
            {go, _Ref, Value, _RelativeTime, _SojournTime} ->
                {ok, Value};
            {drop, _SojournTime} ->
                %% check if the number of activations is less than the max allowed
                case gproc:get_value({rc,l,GrainRef}) of
                    V when V < N ->
                        %% fewer grains activated than the max allowed
                        %% so create a new one
                        erleans_grain_sup:start_child(node(), GrainRef);
                    _ ->
                        %% enqueue the ask
                        case sbroker:ask(?broker(GrainRef)) of
                            {go, _Ref, Value, _RelativeTime, _SojournTime} ->
                                {ok, Value};
                            {drop, _} ->
                                %% give up
                                {error, timeout}
                        end
                end
        end
    catch
        exit:{noproc, _} ->
            %% no pool started for these grains, let's start one
            erleans_stateless_broker:start_link(GrainRef),
            %% spawn a new activation as well and use it.
            %% Note: a large burst could mean we end up with more than the max
            %% activations allowed if they happen to have a noproc thrown
            %% before one of them can spawn the broker
            erleans_grain_sup:start_child(node(), GrainRef)
    end.

-spec enqueue_grain(erleans:grain_ref(), pid()) -> {await, reference(), pid()} | {drop, 0}.
enqueue_grain(GrainRef, Pid) ->
    erleans_stateless_broker:start_link(GrainRef),
    sbroker:async_ask_r(?broker(GrainRef), Pid, {Pid, erleans_grain_tag}).
