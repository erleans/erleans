%% Try for 5 seconds
-define(UNTIL(X), (fun Until(100) ->
                           erlang:error({fail, X});
                       Until(I) ->
                           case X of true -> ok;
                               false ->
                                   timer:sleep(200),
                                   Until(I+1)
                           end
                   end)(0)).

-define(until_match(Guard, Expr, Seconds),
        (fun Until(I) when I =:= (Seconds * 5) ->
                 ?assertMatch(Guard, Expr);
             Until(I) ->
                 try
                     ?assertMatch(Guard, Expr)
                 catch error:_ ->
                         timer:sleep(200),
                         Until(I+1)
                 end
         end)(0)).
