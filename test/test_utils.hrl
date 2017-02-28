%% Try for 5 seconds
-define(UNTIL(X), (fun Until(I) when I =:= 10 -> erlang:error(fail);
                       Until(I) -> case X of true -> ok; false -> timer:sleep(2000), Until(I+1) end end)(0)).
