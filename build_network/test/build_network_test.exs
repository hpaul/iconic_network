defmodule BuildNetworkTest do
  use ExUnit.Case
  doctest BuildNetwork

  test "greets the world" do
    assert BuildNetwork.hello() == :world
  end
end
