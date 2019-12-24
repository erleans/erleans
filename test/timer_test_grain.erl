%%% ---------------------------------------------------------------------------
%%% %%% @copyright 2017 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc A test grain that sets up some timers and then accumulates
%%% the results of messages sent to it.
%%% @end
%%% ---------------------------------------------------------------------------
-module(timer_test_grain).

-behaviour(erleans_grain).

-export([placement/0,
         provider/0,
         save/1,
         node/1,
         stop/1,
         start_timers/1,
         cancel_timers/1,
         crashy_timer/1,
         long_timer/1,
         start_one_timer/1,
         cancel_one_timer/1,
         accumulate/2,
         clear/1]).

-export([activate/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    prefer_local.

provider() ->
    erleans_config:default_provider().

start_timers(Ref) ->
    erleans_grain:call(Ref, start_timers).

cancel_timers(Ref) ->
    erleans_grain:call(Ref, cancel_timers).

crashy_timer(Ref) ->
    erleans_grain:call(Ref, crashy_timer).

long_timer(Ref) ->
    erleans_grain:call(Ref, long_timer).

start_one_timer(Ref) ->
    erleans_grain:call(Ref, start_one_timer).

cancel_one_timer(Ref) ->
    erleans_grain:call(Ref, cancel_one_timer).

accumulate(Ref, Thing) ->
    erleans_grain:call(Ref, {accumulate, Thing}).

clear(Ref) ->
    erleans_grain:call(Ref, clear).

save(Ref) ->
    erleans_grain:call(Ref, save).

node(Ref) ->
    erleans_grain:call(Ref, node).

stop(Ref) ->
    erleans_grain:call(Ref, stop).

activate(_, State=#{}) ->
    {ok, State#{acc => []}, #{}}.

handle_call(node, From, State) ->
    {ok, State, [{reply, From, {ok, node()}}]};

handle_call(clear, From, State = #{acc := Acc}) ->
    {ok, State#{acc => []}, [{reply, From, {ok, Acc}}]};
handle_call({accumulate, Thing}, From, State=#{acc := Acc}) ->
    {ok, State#{acc => [Thing | Acc]}, [{reply, From, ok}]};

handle_call(start_timers, From, State) ->
    %% pattern should be [b, a, c, b, c, b, ...]
    {ok, One} = erleans_timer:start(fun acc/2, a, 10, never),
    {ok, Two} = erleans_timer:start(fun acc/2, b, 5, 10),
    {ok, Three} = erleans_timer:start(fun acc/2, c, 12, 10),
    {ok, State#{timers => [One, Two, Three]}, [{reply, From, ok}]};
handle_call(cancel_timers, From, State = #{timers := Timers}) ->
    [erleans_timer:cancel(Timer) || Timer <- Timers],
    {ok, State, [{reply, From, ok}]};

handle_call(crashy_timer, From, State) ->
    F = fun(Ref, Arg) ->
                case get(crash) of
                    5 -> exit(boom);
                    undefined ->
                        put(crash, 1);
                    N when is_integer(N) ->
                        put(crash, N+1)
                end,
                timer_test_grain:accumulate(Ref, Arg)
         end,
    erleans_timer:start(F, a, 5, 5),
    {ok, State, [{reply, From, ok}]};

handle_call(long_timer, From, State) ->
    F1 = fun(Ref, Arg) ->
                 timer:sleep(90),
                 timer_test_grain:accumulate(Ref, {Arg, self()})
         end,
    F2 = fun(Ref, Arg) ->
                 timer_test_grain:accumulate(Ref, {Arg, self()})
         end,
    erleans_timer:start(F1, long, 1, 1000),
    erleans_timer:start(F2, short, 5, 15),
    {ok, State, [{reply, From, ok}]};

handle_call(start_one_timer, From, State) ->
    erleans_timer:start(fun acc/2, a, 5, 10),
    {ok, State, [{reply, From, ok}]};
handle_call(cancel_one_timer, From, State) ->
    erleans_timer:cancel(),
    {ok, State, [{reply, From, ok}]};

handle_call(stop, From, State) ->
    {deactivate, State, [{reply, From, ok}]};

handle_call(save, From, State) ->
    {ok, State, [{reply, From, ok}, save_state]}.

handle_cast(_, State) ->
    {ok, State}.

handle_info(Msg, State = #{acc := Acc}) ->
    {ok, State#{acc => [Msg | Acc]}};
handle_info(_, State) ->
    {ok, State}.

deactivate(State) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

acc(Ref, Arg) ->
    timer_test_grain:accumulate(Ref, Arg).
