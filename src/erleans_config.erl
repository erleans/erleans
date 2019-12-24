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
         default_provider/0,
         provider/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2]).

-include_lib("kernel/include/logger.hrl").

-define(TABLE, ?MODULE).

-record(state, {config :: list(),
                tid :: ets:tid()}).

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

-spec get(atom()) -> any().
get(Key) ->
    ?MODULE:get(Key, undefined).

-spec get(atom(), any()) -> any().
get(Key, Default) ->
    try
        ets:lookup_element(?TABLE, Key, 2)
    catch
        error:badarg ->
            Default
    end.

default_provider() ->
    persistent_term:get({?MODULE, default_provider}).

providers_mapping() ->
    persistent_term:get({?MODULE, providers_mapping}).

provider(GrainType) ->
    Mapping = providers_mapping(),
    case proplists:get_value(GrainType, Mapping, undefined) of
        undefined ->
            default_provider();
        Provider ->
            Provider
    end.

init(Config) ->
    Tid = ets:new(?TABLE, [protected, named_table, set, {read_concurrency, true}]),
    setup_config(Config),
    {ok, #state{config=Config,
                tid=Tid}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%

setup_config(Config) ->
    setup_provider_configuration(Config),
    [ets:insert(?TABLE, {Key, Value}) || {Key, Value} <- Config].

setup_provider_configuration(Config) ->
    case proplists:get_value(default_provider, Config, undefined) of
        undefined ->
            ?LOG_ERROR("error=no_default_provider"),
            erlang:error(no_default_provider);
        DefaultProvider ->
            persistent_term:put({?MODULE, default_provider}, DefaultProvider),
            ProvidersMapping = proplists:get_value(providers_mapping, Config, undefined),
            persistent_term:put({?MODULE, providers_mapping}, ProvidersMapping)
    end.
