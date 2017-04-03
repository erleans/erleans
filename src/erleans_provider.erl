%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_provider).

-export([]).

-callback update(Id :: any(), State :: any(), ETag :: erleans:etag(), NewETag :: erleans:etag()) -> 
    ok |
    {error, {bad_etag, erleans:etag(), erleans:etag()}} |
    {error, not_found}.                                                  
    
