-define(SINGLE_ACTIVATION, single_activation).
-define(STATELESS, stateless).

-define(grain_call(Ref, Request), erleans_grain:call(erleans:get_grain(?MODULE, Ref), Request)).
-define(grain_call(Ref, Request, Timeout), erleans_grain:call(erleans:get_grain(?MODULE, Ref), Request, Timeout)).

-define(grain_cast(Ref, Request), erleans_grain:cast(erleans:get_grain(?MODULE, Ref), Request)).
-define(grain_cast(Ref, Request, Timeout), erleans_grain:cast(erleans:get_grain(?MODULE, Ref), Request, Timeout)).
