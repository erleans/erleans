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

-export([init/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         deactivate/1]).

-include("erleans.hrl").

placement() ->
    prefer_local.

hello(Ref) ->
    erleans_grain:call(Ref, hello).

save(Ref) ->
    erleans_grain:call(Ref, save).

init(_, State) ->
    {ok, State, #{}}.

handle_call(hello, _From, State) ->
    {reply, hello, State};
handle_call(save, _From, State) ->
    %% will throw an exception
    {save_reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

deactivate(State) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
