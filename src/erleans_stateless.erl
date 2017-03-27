-module(erleans_stateless).

-export([pick_grain/1,
         enqueue_grain/2]).

-include("erleans.hrl").

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
            
enqueue_grain(GrainRef, Pid) ->
    erleans_stateless_broker:start_link(GrainRef),
    sbroker:async_ask_r(?broker(GrainRef), Pid, {Pid, erleans_grain_tag}).
