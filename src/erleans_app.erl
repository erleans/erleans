%%%--------------------------------------------------------------------
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
%%%-----------------------------------------------------------------

%%%-------------------------------------------------------------------
%% @doc erleans public API
%% @end
%%%-------------------------------------------------------------------

-module(erleans_app).

-behaviour(application).

-export([start/2,
         stop/1]).

start(_StartType, _StartArgs) ->
    Specs = init_providers(),
    {ok, Pid} = erleans_sup:start_link(Specs),
    post_init_providers(),

    %% streams manager needs the providers fully initialized first
    %% so we have to do it after post_init_providers
    erleans_sup:start_partitions_sup(),

    {ok, Pid}.

stop(_State) ->
    erleans_cluster:leave(),
    ok.

%% Internal functions

init_providers() ->
    Providers = erleans_config:get(providers, []) ++
        erleans_config:get(stream_providers, []),
    lists:foldl(fun({ProviderName, Args}, Acc) ->
                    case init_provider(ProviderName, Args) of
                        ok ->
                            Acc;
                        {ok, ChildSpec} ->
                            [ChildSpec | Acc];
                        {error, Reason} ->
                            lager:error("failed to initialize provider ~s: reason=~p", [ProviderName, Reason]),
                            Acc
                    end
                end, [], Providers).

init_provider(ProviderName, Config) ->
    Module = proplists:get_value(module, Config),
    Module:init(ProviderName, Config).

post_init_providers() ->
    Providers = erleans_config:get(providers, []),
    lists:foreach(fun({ProviderName, Config}) ->
                      Module = proplists:get_value(module, Config),
                      Module:post_init(ProviderName, Config)
                  end, Providers).
