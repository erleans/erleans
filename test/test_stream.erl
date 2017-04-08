-module(test_stream).

-behaviour(erleans_stream).

-export([init/1,
         fetch/1,
         produce/1]).

init(_Args) ->
    ok.

fetch([{T, O} | _]) when O < 9 ->
    [{T, {O+3, [some_record, another_record, cool_record_bro]}}];
fetch([{T, O} | _]) ->
    [{T, {O, []}}].

produce(_) ->
    [].
