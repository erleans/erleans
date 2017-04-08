%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_provider).

-export([]).

-callback init(list()) -> ok.

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
