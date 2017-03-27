-define(SINGLE_ACTIVATION, single_activation).
-define(STATELESS, stateless).

-define(broker(Name), {via, gproc, {n,l,Name}}).
-define(stateless(GrainRef), {r,l,GrainRef}).
-define(stateless_counter(GrainRef), {rc,l,GrainRef}).

-define(DEFAULT_PLACEMENT, random).
