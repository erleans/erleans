defmodule Erleans.Grain do

  @callback state(term()) :: term()

  @callback activate(:erleans.grain_ref(), term()) :: {:ok, term(), :erleans_grain.opts()} | {:error, term()}

  @callback handle_call(term(), {pid(), term()}, term()) :: :erlans_grain.callback_result()
  @callback handle_cast(term(), term()) :: :erlans_grain.callback_result()
  @callback handle_info(term(), term()) :: :erlans_grain.callback_result()
  @callback deactivate(term()) :: :ok | :save_state | {:save, term()}

  @optional_callbacks state: 1, activate: 2, deactivate: 1, handle_info: 2

  @doc false
  defmacro __using__(args) do
    {placement, args} = Keyword.pop(args, :placement, :prefer_local)
    {provider, args} = Keyword.pop(args, :provider, :undefined)
    {state, _args} = Keyword.pop(args, :state, :undefined)

    quote location: :keep do
      @behaviour :erleans_grain

      @erleans_grain_placement unquote(placement)
      @erleans_grain_provider unquote(provider)
      @erleans_grain_state unquote(state)

      def placement do
        @erleans_grain_placement
      end

      def provider do
        @erleans_grain_provider
      end

      def state(_) do
        @erleans_grain_state
      end

      defoverridable state: 1
    end
  end

  defdelegate call(grain_ref, msg), to: :erleans_grain
  defdelegate call(grain_ref, msg, timeout), to: :erleans_grain
  defdelegate cast(grain_ref, msg), to: :erleans_grain
end
