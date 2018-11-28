defmodule BuildNetwork.MixProject do
  use Mix.Project

  def project do
    [
      app: :build_network,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:mariaex, "~> 0.9.0-rc.0"},
      {:poison, "~> 3.1"},
      {:combination, "~> 0.0.3"},
      {:elastic, "~> 3.0"}
    ]
  end
end
