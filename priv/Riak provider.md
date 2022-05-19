Riak provider for erleans
=====

erleans_provider_riak is a prototype riak persistent storage layer for erleans.
It was tested with riak 3.0.3 with the `leveled` riak backend and the performances on a 5 nodes cluster are pretty good.

## Usage

Just add the riak client (protocol buffer) to the dependencies of your project in `rebar.cfg` file:

``` erlang
  { riakc, {git, "https://github.com/basho/riak-erlang-client.git", {branch, "develop-3.0"}}}
```

and then the actual support and configuration of the riak provider on the `sys.config.src` file:

``` erlang
       {erleans, [
                {providers, #{
                        in_memory => #{ 
                                module => erleans_provider_ets,
                                args => #{}},                        
                        riak => #{
                                module => erleans_provider_riak,
                                args => #{ 
                                        address => "10.0.3.228", 
                                        pb_port => 8087, 
                                }
                        }
                },
                {default_provider, riak}
                ]},
```

The only 2 required parameters are the IP address of the riak host/cluster and the protocol buffer port.

### Note

The riak provider doesn't implement the `all` function, but that's on purpose and might implemented quite easily if needed.

massimo.cesaro@inkwelldata.com
