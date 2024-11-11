%%%--------------------------------------------------------------------
%%% Copyright Tristan Sloughter 2019. All Rights Reserved.
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
%%%-----------------------------------------------------------------

%%% ---------------------------------------------------------------------------
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_config).

-behaviour(gen_server).

-export([start_link/1,
         get/1,
         get/2,
         set/2]).

-export([init/1,
         deactivate_after/0,
         handle_call/3,
         handle_cast/2]).

-define(TABLE, ?MODULE).

-record(state, {config :: list(),
                tid :: ets:table()}).

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

-spec get(atom()) -> any().
get(Key) ->
    ?MODULE:get(Key, undefined).

deactivate_after() ->
    case ?MODULE:get(deactivate_after) of
        DeactivateAfter when DeactivateAfter =:= infinity
                             orelse (is_integer(DeactivateAfter)
                                     andalso DeactivateAfter >= 0) ->
            DeactivateAfter;
        undefined ->
            2700000
    end.

-spec get(atom(), dynamic()) -> dynamic().
get(Key, Default) ->
    try
        ets:lookup_element(?TABLE, Key, 2)
    catch
        error:badarg ->
            Default
    end.

set(Key, Value) ->
    gen_server:call(?MODULE, {set, Key, Value}).

init(Config) ->
    Tid = ets:new(?TABLE, [protected,
                           named_table,
                           set,
                           {read_concurrency, true}]),
    setup_config(Config),
    {ok, #state{config=Config,
                tid=Tid}}.

handle_call({set, Key, Value}, _From, State) ->
    ets:insert(?TABLE, {Key, Value}),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%

setup_config(Config) ->
    [ets:insert(?TABLE, {Key, Value}) || {Key, Value} <- Config].
