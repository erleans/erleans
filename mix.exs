defmodule Erleans.MixProject do
  use Mix.Project

  def project do
    [
      app: :erleans,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:lager, []},
      {:lasp, "~> 0.8.2", override: true},
      {:lasp_pg, []},
      {:partisan, "~> 3.0", override: true},
      {:gproc, []},
      {:sbroker, []},
      {:backoff, []},
      {:erlware_commons, []},
      {:types, "~> 0.1.8", override: true},
      {:jch, []},
      {:eql, []},
      {:opencensus, []},
      {:pgo, []},
      {:uuid, "~> 1.8", hex: :uuid_erl, override: true}
    ]
  end
end
