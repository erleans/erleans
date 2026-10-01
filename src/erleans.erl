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
%%% ---------------------------------------------------------------------------
-module(erleans).

-export([get_grain/2, key_type/1, identity/1]).

-include("erleans.hrl").

-type grain_key() :: erleans_grain_key:key().
-type grain_key_type() :: erleans_grain_key:key_type().

-type provider() :: {module(), atom()}.

-type grain_ref() :: #{implementing_module := module(),
                       id                  := grain_key(),
                       placement           := normalized_placement(),
                       provider            => provider() | undefined}.

-type normalized_placement() :: random | prefer_local | {stateless, integer()}.
-type grain_placement() :: stateless | normalized_placement().

%% An opaque concurrency token owned by the storage provider.
%% undefined is reserved for state which has not been read or inserted.
-type etag() :: term().

-export_type([grain_ref/0, grain_key/0, grain_key_type/0,
              grain_placement/0,
              provider/0,
              etag/0]).

-spec get_grain(module(), grain_key()) -> grain_ref().
get_grain(ImplementingModule, Id) ->
    ok = erleans_grain_key:validate(key_type(ImplementingModule), Id),
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

%% The key type is a durable contract for this grain module.
-spec key_type(module()) -> grain_key_type().
key_type(Module) -> erleans_grain_key:type(Module).

%% Reference metadata must not create another identity for the same grain.
-spec identity(grain_ref()) -> {erleans_grain, module(), grain_key()}.
identity(#{implementing_module := Module, id := Id}) ->
    {erleans_grain, Module, Id}.

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

-spec placement(module()) -> normalized_placement().
placement(Module) ->
    case erleans_utils:fun_or_default(Module, placement, ?DEFAULT_PLACEMENT) of
        stateless ->
            {stateless, erleans_config:get(default_stateless_max, 5)};
        Placement = {stateless, Max} when is_integer(Max) ->
            Placement;
        Placement when Placement =:= random;
                       Placement =:= prefer_local ->
            Placement;
        Placement ->
            error({invalid_placement, Placement})
    end.
