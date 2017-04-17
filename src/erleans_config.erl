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

%%% ---------------------------------------------------------------------------
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_config).

-export([get/1,
         get/2,
         provider/1]).

-spec get(atom()) -> any().
get(Key) ->
    {ok, Value} = application:get_env(erleans, Key),
    Value.

-spec get(atom(), any()) -> any().
get(Key, Default) ->
    application:get_env(erleans, Key, Default).

provider(Grain) ->
    Value = application:get_env(erleans, providers_mapping, []),
    case proplists:get_value(Grain, Value, undefined) of
        undefined ->
            case application:get_env(erleans, default_provider) of
                {ok, Provider} ->
                    Provider;
                _ ->
                    lager:error("error=no_default_provider"),
                    erlang:error(no_default_provider)
            end;
        Provider ->
            Provider
    end.
