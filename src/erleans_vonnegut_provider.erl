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
-module(erleans_vonnegut_provider).

-behaviour(erleans_stream_provider).

-export([init/2,
         fetch/1,
         produce/1]).

-include("erleans.hrl").

init(_Name, _Args) ->  %% TODO: fix before PR to actually honor these
    ok = vg_client_pool:start().

fetch(TopicOffsets) ->
    [case vg_client:fetch(Topic, Offset) of
         {ok, #{record_set := Sets,
                high_water_mark := HWM}} ->
             {Topic,
              {HWM, [decode_fetch(Set) || Set <- Sets]}};
         {error, _} = E ->
             E
     end
     || {Topic, Offset} <- TopicOffsets].

produce(TopicRecordSets) ->
    [case vg_client:produce(Topic, RecordSet) of
         {ok, TopicOffset} ->
             TopicOffset;
         {error, _} = E ->
             E
     end
     || {Topic, RecordSet} <- TopicRecordSets].

%%%% internal


decode_fetch(#{crc := _CRC,
               id := _ID,
               record := Record}) ->
    Record.
