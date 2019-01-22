defmodule BuildNetwork.MixProject do
  use Mix.Project

  def project do
    [
      app: :build_network,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      escript: escript()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :elastic]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:poison, "~> 3.1"},
      {:combination, "~> 0.0.3"},
      {:elastic, git: "https://github.com/hpaul/elastic"},
      {:poolboy, "~> 1.5.1"},
      {:sqlitex, "~> 1.4.0"},
      {:nimble_csv, "~> 0.5"},
      {:benchee, "~> 0.9", only: :dev},
      {:flow, "~> 0.14"}
    ]
  end

  defp escript do
    [main_module: Network.CLI]
  end
end
