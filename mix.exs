defmodule Erleans.MixProject do
  use Mix.Project

  def project do
    [
      app: :erleans,
      version: get_version("VERSION"),
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      package: package(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {:erleans_app, []},
      env: [deactivate_after: 2700000]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gproc, "~> 1.0.0"},
      {:gen_cluster, "~> 0.3.0"},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
    ]
  end

  defp get_version(file) do
    {:ok, version} = File.read(file)
    String.trim(version)
  end

  defp package() do
    [
      files: ~w(lib priv .formatter.exs mix.exs README* readme* LICENSE*
        license* CHANGELOG* changelog* src VERSION rebar.config rebar.lock),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/erleans/erleans"}
    ]
  end
end
