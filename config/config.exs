import Config

config :erleans,
  providers: %{in_memory: %{module: :erleans_provider_ets, args: %{}}},
  default_provider: :in_memory
