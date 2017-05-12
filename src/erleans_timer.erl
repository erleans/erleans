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
%%% @doc Timers are simple periodic or aperiodic timers that will
%%% invoke a callback when they fire.  While they do not have direct
%%% access to grain state or identity, their fun can contain anything,
%%% so they can easily send a message to a grain.
%%%
%%% `cancel_timer/0` is supplied as a helper for simple situations
%%% where a grain only has one timer going.  In the case that there
%%% many timers going at once, `cancel_timer/1` must be used
%%% explicitly on each.
%%%
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_timer).


%% define all passed messages here to get compiler help with atom typos
-define(key, '$erleans_timer_ref').
-define(tick, '$erleans_timer_tick').
-define(cancel, '$erleans_timer_cancel').

-export([start/3,
         start/4,
         cancel/0,
         cancel/1]).


start(Callback, Args, StartTime) ->
    start(Callback, Args, StartTime, never).

start(Callback, Args, StartTime, Period) ->
    case get(grain_ref) of
        undefined ->
            {error, called_outside_of_grain_context};
        GrainRef ->
            Grain = self(),
            Pid =
                spawn(fun() ->
                              link(Grain),
                              Now = erlang:monotonic_time(milli_seconds),
                              FirstFire = Now + StartTime,
                              erlang:send_after(FirstFire, self(), ?tick,
                                                [{abs, true}]),
                              loop(FirstFire, Callback, GrainRef, Args,
                                   Period, Grain)
                      end),
            put(?key, Pid),
            {ok, Pid}
    end.

cancel() ->
    case get(?key) of
        undefined ->
            {error, no_timer_to_cancel};
        Pid when is_pid(Pid) ->
            Pid ! ?cancel,
            erlang:erase(?key),
            ok
    end.

cancel(Pid) when is_pid(Pid) ->
    Pid ! ?cancel,
    erase(?key),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal functions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

loop(FireTime, Callback, GrainRef, Args, Period, Grain) ->
    receive
        ?cancel ->
            unlink(Grain),
            ok;
        ?tick ->
            try Callback(GrainRef, Args) of
                _ ->
                    case Period of
                        never ->
                            unlink(Grain),
                            ok;
                        _ ->
                            NextFire = FireTime + Period,
                            erlang:send_after(NextFire, self(), ?tick,
                                              [{abs, true}]),
                            loop(NextFire, Callback, GrainRef, Args,
                                 Period, Grain)
                    end
            catch Class:Error ->
                    Grain ! {erleans_timer_error, Class, Error},
                    unlink(Grain)
            end;
        Msg ->
            Grain ! {erleans_timer_unexpected_msg, Msg}
    end.
