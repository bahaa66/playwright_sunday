defmodule CogyntWorkstationIngestWeb.JA_Keys do

  alias CogyntWorkstationIngestWeb.JA_Keys

  def init(opts \\ []) do
    opts
    |> Enum.into(%{})
  end

  def call(conn, _opts) do
    case conn.params do
      %Plug.Conn.Unfetched{} -> conn
      _ -> %{conn | params: camelize(conn.params, %{"data" => %{"attributes" => %{}}})}
    end
  end

  def camelize(map, keys) when is_map(map) do
    Enum.map(map, fn {k,v} ->
      # {camelize_key(k), camelize(v)}
      if keys do
        v = camelize(v, keys[k])
        {camelize_key(k), v}
      else
        {k,v}
      end
    end)
    |> Enum.into(%{})
  end
  def camelize(v, _keys) do
    v
  end

  def camelize_key(s) when is_binary(s) do
    Inflex.camelize(s, :lower)
  end
  def camelize_key(s) when is_atom(s) do
    Inflex.camelize(to_string(s), :lower)
  end

  # Convert from a map to a map with dashed keys.  Only string
  # keys in legals will be copied, all keys in from that are
  # atoms will be copied.  All keys will be converted from
  # camelCase to dash-case
  def dasherize from, to, legals do
    from
    |> Enum.reduce(to, fn {k,v}, o ->
      cond do
        k in legals ->
          Map.put(o, JA_Keys.dasherize_key(k), v)
        is_atom(k) ->
          Map.put(o, JA_Keys.dasherize_key(k), v)
        true ->
          o
      end
    end)
  end

  def dasherize(map) when is_map(map) do
    Enum.map(map, fn {k,v} ->
      {dasherize_key(k), v}
    end)
    |> Enum.into(%{})
  end
  def dasherize(v) do
    v
  end

  def dasherize_key(s) when is_binary(s) do
    s
    |> Inflex.underscore
    |> String.replace("_"," ")
    |> Inflex.parameterize
    |> String.to_atom
  end
  def dasherize_key(s) when is_atom(s) do
    s
    |> to_string
    |> Inflex.underscore
    |> String.replace("_"," ")
    |> Inflex.parameterize
    |> String.to_atom
  end
end
