%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2017 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_stream).

-callback init(Args :: list()) ->
    ok | {error, Reason :: term()}.

-callback fetch(TopicsOffsets :: [{Topic :: term(), Offset :: integer()}]) ->
    [{Topic :: term(), {NewOffset :: integer(), Records :: list()} | {error, Reason :: term()}}].

-callback produce(TopicsRecordSets :: [{Topic :: term(), RecordSet :: list()}]) ->
    [{Topic :: term(), Offset :: integer() | {error, Reason :: term()}}].
