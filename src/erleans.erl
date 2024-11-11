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
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans).

-export([get_grain/2]).

-include("erleans.hrl").

-type provider() :: {module(), atom()}.

-type grain_ref() :: #{implementing_module := module(),
                       id                  := term(),
                       placement           := grain_placement(),
                       provider            => provider() | undefined}.

-type grain_placement() :: random |
                           prefer_local |
                           stateless |
                           {stateless, integer()} |
                           system_grain. %% | load

-define(IS_GRAIN_PLACEMENT(X),
        X =:= random orelse
        X =:= prefer_local orelse
        X =:= stateless orelse
        element(1, X) =:= stateless orelse
        X =:= system_grain).

-type etag() :: integer().

-export_type([grain_ref/0,
              grain_placement/0,
              provider/0,
              etag/0]).

-spec get_grain(module(), any()) -> grain_ref().
get_grain(ImplementingModule, Id) ->
    Placement = placement(ImplementingModule),
    BaseGrainRef = #{implementing_module => ImplementingModule,
                     placement => Placement,
                     id => Id},
    case Placement of
        {stateless, _} ->
            BaseGrainRef#{provider => undefined};
        _ ->
            BaseGrainRef#{provider => provider(ImplementingModule)}
    end.

-spec provider(module()) -> provider() | undefined.
provider(CbModule) ->
    case erleans_utils:fun_or_default(CbModule, provider, undefined) of
        undefined ->
            undefined;
        Name when is_atom(Name) ->
            find_provider_config(Name)
    end.

-spec find_provider_config(atom()) -> provider().
find_provider_config(default) ->
    %% throw an exception if default_provider is set to default,
    %% which would cause an infinite loop
    case erleans_providers:default() of
        default ->
            error(bad_default_provider_config);
        DefaultProvider ->
            find_provider_config(DefaultProvider)
    end;
find_provider_config(Name) ->
    case erleans_providers:provider(Name) of
        undefined ->
            throw({missing_provider_config, Name});
        Module ->
            {Module, Name}
    end.

-spec placement(module()) -> grain_placement().
placement(Module) ->
    case erleans_utils:fun_or_default(Module, placement, ?DEFAULT_PLACEMENT) of
        stateless ->
            {stateless, erleans_config:get(default_stateless_max, 5)};
        Placement when ?IS_GRAIN_PLACEMENT(Placement) ->
            Placement
    end.
