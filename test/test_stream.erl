-module(test_stream).

-behaviour(erleans_stream_provider).

-export([init/2,
         fetch/1,
         produce/1]).

init(_Name, _Args) ->
    ets:new(test_stream, [public, named_table]),
    ok.

fetch(TopicOffsets) ->
    [case ets:lookup(test_stream, Topic) of
         [] ->
             {error, not_found};
         [{_, Records}] ->
             Tail = lists:nthtail(Offset, Records),
             LTail = length(Tail),
             {Topic, {LTail + Offset, Tail}}
     end
     || {Topic, Offset} <- TopicOffsets].

produce(TopicRecords) ->
    [case ets:lookup(test_stream, Topic) of
         [] ->
             ets:insert(test_stream, {Topic, Records});
         [{_, Existing}] ->
             ets:insert(test_stream, {Topic, Existing ++ Records})
     end
     || {Topic, Records} <- TopicRecords].
