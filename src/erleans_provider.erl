%%%----------------------------------------------------------------------------
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
%%%----------------------------------------------------------------------------

%%% ---------------------------------------------------------------------------
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_provider).

-callback init(ProviderName :: atom(), Args :: list()) -> ok | {ok, supervisor:child_spec()}.

-callback post_init(ProviderName :: atom(), Args :: list()) -> ok.

-callback all(Type :: module(), ProviderName :: atom()) -> {ok, [any()]} | {error, any()}.

-callback read(Type :: module(), ProviderName :: atom(), GrainRef :: erleans:grain_ref()) ->
    {ok, State :: any(), ETag :: erleans:etag()} |
    {error, not_found}.

-callback read_by_hash(Type :: module(), ProviderName :: atom(), GrainRef :: erleans:grain_ref()) ->
    {ok,  [{GrainRef :: erleans:grain_ref(), Type :: module(), ETag :: erleans:etag(), State :: any()}]} |
    {error, not_found}.

-callback insert(Type :: module(), ProviderName :: atom(), Id :: any(), State :: any(), ETag :: erleans:etag()) -> ok.

-callback insert(Type :: module(), ProviderName :: atom(), Id :: any(), Hash :: integer(),
                 State :: any(), ETag :: erleans:etag()) -> ok.

-callback update(Type :: module(), ProviderName :: atom(), Id :: any(), State :: any(),
                  ETag :: erleans:etag(), NewETag :: erleans:etag()) ->
    ok |
    {error, {bad_etag, erleans:etag(), erleans:etag()}} |
    {error, not_found}.

-callback update(Type :: module(), ProviderName :: atom(), Id :: any(), Hash :: integer(),
                  State :: any(), ETag :: erleans:etag(), NewETag :: erleans:etag()) ->
    ok |
    {error, {bad_etag, erleans:etag(), erleans:etag()}} |
    {error, not_found}.
