-define(SINGLE_ACTIVATION, single_activation).
-define(STATELESS, stateless).

-define(broker(Name), {via, gproc, {n,l,{broker,Name}}}).
-define(stateless(GrainRef), {r,l,GrainRef}).
-define(stateful(GrainRef), {n,l,GrainRef}).
-define(stateless_counter(GrainRef), {rc,l,GrainRef}).

-define(DEFAULT_PLACEMENT, random).

-define(STREAM_BROKER, erleans_stream_broker).
-define(STREAM_TAG, erleans_stream_tag).

-define(INITIAL_FETCH_INTERVAL, 1000).
