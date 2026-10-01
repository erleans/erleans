defmodule Erleans.Grain do

  @callback key_type() :: :erleans.grain_key_type()

  @callback state(:erleans.grain_key()) :: term()

  @callback activate(:erleans.grain_ref(), term()) :: {:ok, term(), :erleans_grain.opts()} | {:error, term()}

  @callback handle_call(term(), {pid(), term()}, term()) :: :erleans_grain.callback_result()
  @callback handle_cast(term(), term()) :: :erleans_grain.callback_result()
  @callback handle_info(term(), term()) :: :erleans_grain.callback_result()
  @callback deactivate(term()) :: {:ok, term()} | {:save_state, term()}

  @optional_callbacks key_type: 0, state: 1, activate: 2, deactivate: 1, handle_info: 2

  @doc false
  defmacro __using__(args) do
    {key_type, args} = Keyword.pop(args, :key_type, :string)
    {placement, args} = Keyword.pop(args, :placement, :prefer_local)
    {provider, args} = Keyword.pop(args, :provider, :undefined)
    {state, _args} = Keyword.pop(args, :state, :undefined)

    quote location: :keep do
      @behaviour :erleans_grain

      @erleans_grain_key_type unquote(key_type)
      @erleans_grain_placement unquote(placement)
      @erleans_grain_provider unquote(provider)
      @erleans_grain_state unquote(state)

      def key_type do
        @erleans_grain_key_type
      end

      def placement do
        @erleans_grain_placement
      end

      def provider do
        @erleans_grain_provider
      end

      def state(_) do
        @erleans_grain_state
      end

      defoverridable key_type: 0
      defoverridable provider: 0
      defoverridable state: 1
    end
  end

  defdelegate call(grain_ref, msg), to: :erleans_grain
  defdelegate call(grain_ref, msg, timeout), to: :erleans_grain
  defdelegate cast(grain_ref, msg), to: :erleans_grain
end
