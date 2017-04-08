-module(erleans_cluster).

-export([join/3,
         leave/0]).

join(Name, Host, PartisanPort) ->
    partisan_peer_service:join({Name, Host, PartisanPort}).

leave() ->
    partisan_peer_service:leave([]).
