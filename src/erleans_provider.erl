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

-export([all/1,
         insert/4,
         replace/5]).

-callback init(list()) -> ok.

-callback all(Type :: module()) -> {ok, [any()]} | {error, any()}.

-callback read(Type :: module(), GrainRef :: erleans:grain_ref()) -> {ok, State :: any(), ETag :: erleans:etag()} |
                                                                     {error, not_found}.

-callback insert(Type :: module(), Id :: any(), State :: any(), ETag :: erleans:etag()) -> ok.

-callback update(Type :: module(), Id :: any(), Update :: list(), ETag :: erleans:etag(), NewETag :: erleans:etag()) ->
    ok |
    {error, {bad_etag, erleans:etag(), erleans:etag()}} |
    {error, not_found}.

-callback replace(Type :: module(), Id :: any(), State :: any(), ETag :: erleans:etag(), NewETag :: erleans:etag()) ->
    ok |
    {error, {bad_etag, erleans:etag(), erleans:etag()}} |
    {error, not_found}.

all(Type) ->
    (Type:provider()):all(Type).

insert(Type, Id, State, ETag) ->
    (Type:provider()):insert(Type, Id, State, ETag).

replace(Type, Id, State, ETag, NewETag) ->
    (Type:provider()):replace(Type, Id, State, ETag, NewETag).
