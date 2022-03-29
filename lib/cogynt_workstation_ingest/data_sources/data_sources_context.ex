defmodule CogyntWorkstationIngest.DataSources.DataSourcesContext do
  @moduledoc """
  The DataSources context: public interface for datasource related functionality.
  """
  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo
  alias CogyntWorkstationIngest.Config
  alias Models.DataSources.DataSource

  # --------------------------------- #
  # --- Datasource Schema Methods --- #
  # --------------------------------- #
  @doc """
  Lists all the DataSources
  ## Examples
      iex> list_datasources()
      [%DataSource{}, ...]
  """
  def list_datasources do
    Repo.all(DataSource)
  end

  @doc """
  Creates a DataSource entry.
  ## Examples
      iex> create_datasource(%{field: value})
      {:ok, %DataSource{}}
      iex> create_datasource(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def create_datasource(attrs \\ %{}) do
    %DataSource{}
    |> DataSource.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Update a DataSource entry.
  ## Examples
      iex> update_datasource(%{field: value})
      {:ok, %DataSource{}}
      iex> update_datasource(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def update_datasource(%DataSource{} = datasource, attrs) do
    datasource
    |> DataSource.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Will create the DataSource if no record is found for the datasource_id.
  If a record is found it updates the record with the new attrs.
  ## Examples
      iex> upsert_datasource(%{field: value})
      {:ok, %DataSource{}}
      iex> upsert_datasource(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def upsert_datasource(attrs) do
    case get_datasource(attrs.id) do
      nil ->
        create_datasource(attrs)

      %DataSource{} = datasource ->
        update_datasource(datasource, attrs)
    end
  end

  @doc """
  Returns the DataSource for id.
  ## Examples
      iex> get_datasource(id)
      %DataSource{}
      iex> get_datasource(invalid_id)
       nil
  """
  def get_datasource(id) do
    Repo.get(DataSource, id)
  end

  @doc """
  Removes all the records in the DataSource table.
  It returns a tuple containing the number of entries
  and any returned result as second element. The second
  element is nil by default unless a select is supplied
  in the delete query
    ## Examples
      iex> hard_delete_datasources()
  """
  def hard_delete_datasources() do
    try do
      {:ok, result = %Postgrex.Result{}} = Repo.query("TRUNCATE datasource", [])

      CogyntLogger.info(
        "#{__MODULE__}",
        "hard_delete_datasources completed with result: #{result.connection_id}"
      )
    rescue
      _ ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "hard_delete_datasources failed"
        )
    end
  end

  @doc """
  Parses the connect_string stored in the DataSource table and converts it to the Array
  of Tuples.

  Responses:
    {:ok, [{host, port}, {host, port}, {host, port}]} | {:error, :does_not_exist}
  """
  def fetch_brokers(datasource_id) do
    case get_datasource(datasource_id) do
      nil ->
        {:error, :does_not_exist}

      %DataSource{connect_string: connect_string} ->
        {:ok, Config.parse_kafka_brokers(connect_string)}
    end
  end
end
