-define(SINGLE_ACTIVATION, single_activation).
-define(STATELESS, stateless).

-define(pool(Name), {pool,Name}).
-define(stateless(GrainRef), {r,l,GrainRef}).
-define(stateful(GrainRef), {n,l,GrainRef}).
-define(stateless_counter(GrainRef), {rc,l,GrainRef}).

-define(DEFAULT_PLACEMENT, random).
