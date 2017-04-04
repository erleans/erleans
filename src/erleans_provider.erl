%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_provider).

-export([]).

-callback init() -> ok.

-callback read(Id :: any()) -> {ok, State :: any(), ETag :: erleans:etag()} |
                               {error, not_found}.

-callback insert(Id :: any(), State :: any(), ETag :: erleans:etag()) -> ok.

-callback update(Id :: any(), Update :: list(), ETag :: erleans:etag(), NewETag :: erleans:etag()) ->
    ok |
    {error, {bad_etag, erleans:etag(), erleans:etag()}} |
    {error, not_found}.

-callback replace(Id :: any(), State :: any(), ETag :: erleans:etag(), NewETag :: erleans:etag()) ->
    ok |
    {error, {bad_etag, erleans:etag(), erleans:etag()}} |
    {error, not_found}.
