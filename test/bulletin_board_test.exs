defmodule BulletinBoardTest do
  use ExUnit.Case
  doctest BulletinBoard

  test "greets the world" do
    assert BulletinBoard.hello() == :world
  end
end
