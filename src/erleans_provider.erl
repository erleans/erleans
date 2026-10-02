%%%----------------------------------------------------------------------------
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
-module(erleans_provider).

-export([start_link/2]).

%% Providers generate ETags and atomically check them when writing. A successful
%% write returns a fresh token even when the payload has not changed. undefined
%% is reserved for a missing row and must not be returned as a stored ETag.
%% Conflicts may use bad_etag or {bad_etag, ExpectedETag, StoredETag} as the
%% error reason. Grains normalize these to saved_etag_changed. A save continuation
%% may reload after a conflict; otherwise the activation exits with that reason
%% and waiting calls receive {exit, saved_etag_changed}.
-type write_result() :: {ok, erleans:etag()} | {error, term()}.

-callback start_link(ProviderName :: atom(), Args :: list()) -> {ok, pid()}.

-callback all(Type :: module(), ProviderName :: atom()) ->
    {ok, [{Id :: erleans:grain_key(), Type :: module(), Hash :: integer(),
           ETag :: erleans:etag(), State :: term()}]} | {error, term()}.

-callback read(Type :: module(), ProviderName :: atom(), Id :: erleans:grain_key()) ->
    {ok, State :: any(), ETag :: erleans:etag()} |
    {error, Reason :: term()}.

-callback read_by_hash(Type :: module(), ProviderName :: atom(), Hash :: integer()) ->
    {ok,  [{Id :: erleans:grain_key(), Type :: module(), ETag :: erleans:etag(), State :: any()}]} |
    {error, not_found}.

%% Insert only if no row with this type and id exists; otherwise return bad_etag.
-callback insert(Type :: module(), ProviderName :: atom(), Id :: erleans:grain_key(), State :: any()) -> write_result().

-callback insert(Type :: module(), ProviderName :: atom(), Id :: erleans:grain_key(), Hash :: integer(),
                 State :: any()) -> write_result().

-callback update(Type :: module(), ProviderName :: atom(), Id :: erleans:grain_key(), State :: any(),
                  ETag :: erleans:etag()) -> write_result().

-callback update(Type :: module(), ProviderName :: atom(), Id :: erleans:grain_key(), Hash :: integer(),
                  State :: any(), ETag :: erleans:etag()) -> write_result().

start_link(Name, #{module := Module,
                   args   := Args}) ->
    Module:start_link(Name, Args).
