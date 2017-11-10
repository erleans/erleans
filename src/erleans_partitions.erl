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
%%% @doc Each erleans node is responsible for a subset of partitions used to
%%%      distribute responsibility for streams and reminders in the cluster.
%%%      This server is notified by partisan when the cluster membership
%%%      change and recalculates the range of partitions to handle.
%%%
%%%      Ranges are closed intervals, [Start, End]. So searching for streams
%%%      or reminders within the interval is to include the Start and End
%%%      partitions, i.e. Partition >=Start andalso Partition =< End.
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_partitions).

-behaviour(gen_server).

-export([start_link/0,
         find_node/1,
         add_handler/2,
         get_range/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("erleans.hrl").

-type range() :: {integer(), integer()}.

-record(state, {range          :: range() | undefined,
                num_partitions :: integer(),
                node_ranges    :: [{range(), node()}],
                to_notify      :: #{atom() => pid() | atom()}}).

-define(CH(Item, Partitions), jch:ch(Item, Partitions)).

-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec find_node(term()) -> {integer(), node()}.
find_node(Item) ->
    gen_server:call(?MODULE, {find_node, Item}).

-spec get_range() -> range().
get_range() ->
    gen_server:call(?MODULE, get_range).

-spec add_handler(atom(), pid() | atom()) -> ok.
add_handler(Name, Pid) ->
    gen_server:call(?MODULE, {add_handler, Name, Pid}).


init([]) ->
    %% callback for changes to cluster membership
    Self = self(),
    ok = partisan_peer_service_events:add_sup_callback(fun(Membership) ->
                                                           Self ! {update, Membership}
                                                       end),
    NumPartitions = erleans_config:get(num_partitions),
    {ok, #state{num_partitions=NumPartitions,
                node_ranges=[],
                to_notify=#{}}}.

handle_call({find_node, Item}, From, State=#state{num_partitions=NumPartitions,
                                                  node_ranges=Ranges}) ->
    spawn(fun() ->
              Partition = ?CH(erlang:phash2(Item), NumPartitions),
              {_, Node} = ec_lists:fetch(fun({{Start, End}, _Node}) when Partition >= Start
                                                                       , Partition =< End ->
                                             true;
                                            (_) ->
                                             false
                                         end, Ranges),
              gen_server:reply(From, {Partition, Node})
          end),
    {noreply, State};
handle_call(get_range, _From, State=#state{range=Range}) ->
    {reply, Range, State};
handle_call({add_handler, Name, Pid}, _From, State=#state{to_notify=ToNotify}) ->
    {reply, ok, State#state{to_notify=ToNotify#{Name => Pid}}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({update, Membership}, State=#state{num_partitions=NumPartitions,
                                               to_notify=ToNotify}) ->
    MembersList = lists:usort(sets:to_list(state_orset:query(Membership))),
    {Range, NodeRanges} = update_ranges(MembersList, NumPartitions, ToNotify),
    {noreply, State#state{range=Range,
                          node_ranges=NodeRanges}};
handle_info({gen_event_EXIT, _, _}, State) ->
    %% there is no reason the event handler should be removed
    %% so if we receive this message attempt to add it back
    Self = self(),
    ok = partisan_peer_service_events:add_sup_callback(fun(Membership) ->
                                                           Self ! {update, Membership}
                                                       end),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal functions

update_ranges(MembersList, NumPartitions, ToNotify) ->
    Length = length(MembersList),
    {_, NodeRanges} = lists:foldl(fun(#{name := Node}, {Pos, Acc}) ->
                                      Range = calc_partition_range(Pos, Length, NumPartitions),
                                      {Pos+1, [{Range, Node} | Acc]};
                                     (Node, {Pos, Acc}) ->
                                      Range = calc_partition_range(Pos, Length, NumPartitions),
                                      {Pos+1, [{Range, Node} | Acc]}
                                  end, {0, []}, MembersList),

    {Range, _} = lists:keyfind(node(), 2, NodeRanges),

    maps:map(fun(_, Pid) ->
                 Pid ! {update_streams, Range}
             end, ToNotify),

    {Range, NodeRanges}.

%% Find the range of partitions this node position is responsible for
-spec calc_partition_range(integer(), integer(), integer()) -> {integer(), integer()}.
calc_partition_range(Pos, NumMembers, NumPartitions) ->
    Size = NumPartitions div NumMembers,
    Start = Pos * Size,
    case Start + Size of
        End when NumPartitions < (End + Size)  ->
            {Start, NumPartitions};
        End ->
            {Start, End-1}
    end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

partition_range_test() ->
    ?assertEqual({0,15}, calc_partition_range(0, 4, 64)),
    ?assertEqual({16,31}, calc_partition_range(1, 4, 64)),
    ?assertEqual({32,47}, calc_partition_range(2, 4, 64)),
    ?assertEqual({48,64}, calc_partition_range(3, 4, 64)).

-endif.
