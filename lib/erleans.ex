defmodule Erleans do
  @moduledoc """
  Documentation for the Erleans Elixir interface.
  """

  @doc """
  Returns a Grain reference that can be used to make requests to a grain.

  ## Examples

      iex> ref = Erleans.get_grain(MyApp.ChatGrain, "username")

  """
  defdelegate get_grain(implementing_module, id), to: :erleans

end
