%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc A grain that doesn't use any storage provider.
%%% @end
%%% ---------------------------------------------------------------------------
-module(no_provider_test_grain).

-behaviour(erleans_grain).

-export([placement/0,
         hello/1,
         save/1]).

-export([activate/2,
         handle_call/3,
         handle_cast/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    prefer_local.

hello(Ref) ->
    erleans_grain:call(Ref, hello).

save(Ref) ->
    erleans_grain:call(Ref, save).

activate(_, State) ->
    {ok, State, #{}}.

handle_call(hello, From, State) ->
    {ok, State, [{reply, From, hello}]};
handle_call(save, From, State) ->
    %% will throw an exception
    {ok, State, [{reply, From, ok}, save_state]}.

handle_cast(_, State) ->
    {ok, State}.

deactivate(State) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
