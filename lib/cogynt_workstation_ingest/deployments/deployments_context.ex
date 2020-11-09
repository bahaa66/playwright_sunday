defmodule CogyntWorkstationIngest.Deployments.DeploymentsContext do
  @moduledoc """
  The Deployments context: public interface for deployment related functionality.
  """
  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo
  alias Models.Deployments.Deployment

  # --------------------------------- #
  # --- Deployment Schema Methods --- #
  # --------------------------------- #
  @doc """
  Lists all the Deployments
  ## Examples
      iex> list_deployments()
      [%Deployment{}, ...]
  """
  def list_deployments do
    Repo.all(Deployment)
  end

  @doc """
  Creates a Deployment entry.
  ## Examples
      iex> create_deployment(%{field: value})
      {:ok, %Deployment{}}
      iex> create_deployment(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def create_deployment(attrs \\ %{}) do
    %Deployment{}
    |> Deployment.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Update a Deployment entry.
  ## Examples
      iex> update_deployment(%{field: value})
      {:ok, %Deployment{}}
      iex> update_deployment(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def update_deployment(%Deployment{} = deployment, attrs) do
    deployment
    |> Deployment.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Will create the Deployment if no record is found for the deployment id.
  If a record is found it updates the record with the new attrs.
  ## Examples
      iex> upsert_deployment(%{field: value})
      {:ok, %Deployment{}}
      iex> upsert_deployment(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def upsert_deployment(attrs) do
    case get_deployment(attrs.id) do
      nil ->
        create_deployment(attrs)

      %Deployment{} = deployment ->
        update_deployment(deployment, attrs)
    end
  end

  @doc """
  Returns the Deployment for id.
  ## Examples
      iex> get_deployment(id)
      %Deployment{}
      iex> get_deployment(invalid_id)
       nil
  """
  def get_deployment(id) do
    Repo.get(Deployment, id)
  end

  @doc """
  Removes all the records in the Deployment table.
  It returns a tuple containing the number of entries
  and any returned result as second element. The second
  element is nil by default unless a select is supplied
  in the delete query
    ## Examples
      iex> hard_delete_deployments()
      {10, nil}
  """
  def hard_delete_deployments() do
    Repo.delete_all(Deployment, timeout: 120_000)
  end

  @doc """
  Parses the Brokers out of the data_sources json value stored in the
  Deployments table. Example of the data_sources object that is being parsed.
  "data_sources": [
      {
        "spec": {
          "brokers": [{ "host": "kafka.cogilitycloud.com", "port": "31090" }]
        },
        "kind": "kafka",
        "lock_version": 2,
        "version": 1
      }
    ]

    Responses:
    {:ok, [{host, port}, {host, port}, {host, port}]} | {:error, :does_not_exist}
  """
  def get_kafka_brokers(id) do
    case get_deployment(id) do
      nil ->
        {:error, :does_not_exist}

      %Deployment{data_sources: data_sources} ->
        uris =
          Enum.reduce(data_sources, [], fn data_source, acc_0 ->
            case data_source["kind"] == "kafka" do
              true ->
                uris =
                  Enum.reduce(data_source["spec"]["brokers"], [], fn %{
                                                                       "host" => host,
                                                                       "port" => port
                                                                     },
                                                                     acc_1 ->
                    acc_1 ++ [{host, String.to_integer(port)}]
                  end)

                acc_0 ++ uris

              false ->
                acc_0
            end
          end)

        {:ok, uris}
    end
  end
end
