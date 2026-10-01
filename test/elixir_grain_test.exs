defmodule Erleans.ElixirTestGrain do
  use Erleans.Grain,
    placement: :prefer_local,
    provider: :in_memory,
    state: %{counter: 0, save_on_deactivate: false}

  def handle_call(:get, from, state) do
    {:ok, state, [{:reply, from, state.counter}]}
  end

  def handle_call({:stop, save?}, from, state) do
    {:deactivate, %{state | save_on_deactivate: save?}, [{:reply, from, :ok}]}
  end

  def handle_cast(:increment, state) do
    {:ok, %{state | counter: state.counter + 1}}
  end

  def deactivate(%{save_on_deactivate: true} = state), do: {:save_state, state}
  def deactivate(state), do: {:ok, state}
end

defmodule Erleans.GrainTest do
  use ExUnit.Case, async: false

  test "Elixir grains call, cast, and deactivate with or without saving" do
    for save? <- [false, true] do
      ref = Erleans.get_grain(Erleans.ElixirTestGrain, make_ref())
      assert 0 == Erleans.Grain.call(ref, :get)
      assert :ok == Erleans.Grain.cast(ref, :increment)
      assert 1 == Erleans.Grain.call(ref, :get)
      pid = :erleans_grain_registry.whereis_name(ref)
      monitor = Process.monitor(pid)
      assert :ok == Erleans.Grain.call(ref, {:stop, save?})
      assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, :deactivated}}, 1000

      expected = if save?, do: 1, else: 0
      version = if save?, do: 2, else: 1

      assert {:ok, %{counter: ^expected}, ^version} =
               :erleans_provider_ets.read(Erleans.ElixirTestGrain, :in_memory, ref.id)

      assert expected == Erleans.Grain.call(ref, :get)
    end
  end
end
