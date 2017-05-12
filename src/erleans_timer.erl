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

-include("include/erleans_timer.hrl").

-export([start/3,
         start/4,
         cancel/0,
         cancel/1,
         resumable_cancel/0,
         check/0,
         recover/0]).

-record(timer,
        {
          grain :: pid(),
          grain_ref :: map(),
          callback :: function(),
          args :: term(),
          period :: pos_integer()
        }).

-opaque timer() :: #timer{}.
-export_type([timer/0]).

start(Callback, Args, StartTime) ->
    start(Callback, Args, StartTime, never).

start(Callback, Args, StartTime, Period) ->
    case get(grain_ref) of
        undefined ->
            {error, called_outside_of_grain_context};
        GrainRef ->
            Grain = self(),
            Timer =
                #timer{grain = Grain,
                       grain_ref = GrainRef,
                       callback = Callback,
                       args = Args,
                       period = Period},
            Pid = start_timer(StartTime, Timer),
            case get(?key) of
                undefined ->
                    put(?key, #{Pid => Timer});
                Map ->
                    put(?key, Map#{Pid => Timer})
            end,
            {ok, Pid}
    end.

cancel() ->
    case get(?key) of
        undefined ->
            {error, no_timer_to_cancel};
        Map when is_map(Map) ->
            maps:fold(fun(Pid, _V, Acc) ->
                              Pid ! ?cancel,
                              Acc
                      end, beep, Map),
            erlang:erase(?key),
            ok
    end.

cancel(Pid) when is_pid(Pid) ->
    case get(?key) of
        #{Pid := _} = Map ->
            Pid ! ?cancel,
            put(?key, maps:remove(Pid, Map)),
            ok;
        _ ->
            {error, no_timer_to_cancel}
    end.

%%% in order to be able to recover the list of timers when the grain
%%% starts to go down and then is reactivated before all of the
%%% in-process timer ticks can complete, we need to have a slightly
%%% different form of cancel, which leaves the timer information
%%% structure intact so we can restart them if we need to.
resumable_cancel() ->
    case get(?key) of
        undefined ->
            {error, no_timer_to_cancel};
        Map when is_map(Map) ->
            maps:fold(fun(Pid, _V, Acc) ->
                              Pid ! ?cancel,
                              Acc
                      end, beep, Map),
            erase(?key),
            put(?recovery, Map)
    end.

check() ->
    case get(?recovery) of
        undefined ->
            finished;
        Map when is_map(Map) ->
            AnythingAlive =
                maps:fold(fun(_Pid, _V, false) ->
                                  false;
                             (Pid, _V, true) ->
                                  case is_process_alive(Pid) of
                                      true -> false;
                                      false -> true
                                  end
                          end, true, Map),
            case AnythingAlive of
                false ->
                    finished;
                _ ->
                    pending
            end
    end.

recover() ->
    case get(?recovery) of
        undefined ->
            ok;
        Map when is_map(Map) ->
            NewMap = maps:fold(fun(_Pid, Timer, Acc) ->
                                       NewPid = start_timer(Timer#timer.period, Timer),
                                       Acc#{NewPid => Timer}
                               end, #{}, Map),
            erase(?recovery),
            put(?key, NewMap)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal functions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

loop(FireTime, #timer{grain = Grain,
                      grain_ref = GrainRef,
                      callback = Callback,
                      args = Args,
                      period = Period} = Timer) ->
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
                            loop(NextFire, Timer)
                    end
            catch Class:Error ->
                    Grain ! {erleans_timer_error, Class, Error},
                    unlink(Grain)
            end;
        Msg ->
            Grain ! {erleans_timer_unexpected_msg, Msg}
    end.

start_timer(StartTime, #timer{grain = Grain} = Timer) ->
    spawn(fun() ->
                  link(Grain),
                  Now = erlang:monotonic_time(milli_seconds),
                  FirstFire = Now + StartTime,
                  erlang:send_after(FirstFire, self(), ?tick,
                                    [{abs, true}]),
                  loop(FirstFire, Timer)
          end).
