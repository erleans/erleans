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
-module(erleans_stream).

-export([provider/0]).

-callback init(Args :: list()) ->
    ok | {error, Reason :: term()}.

-callback fetch(TopicsOffsets :: [{Topic :: term(), Offset :: integer()}]) ->
    [{Topic :: term(), {NewOffset :: integer(), Records :: list()} | {error, Reason :: term()}}].

-callback produce(TopicsRecordSets :: [{Topic :: term(), RecordSet :: list()}]) ->
    [{Topic :: term(), Offset :: integer() | {error, Reason :: term()}}].

provider() ->
    erleans_config:get(default_provider).
