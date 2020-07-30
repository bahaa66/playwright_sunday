defmodule CogyntWorkstationIngest.Deployments.DeploymentsContext do
  @moduledoc """
  The Deployments context: public interface for deployment related functionality.
  """
  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo
  alias KafkaEx.Protocol.Fetch.Message
  alias Models.Deployments.Deployment
  alias Models.Enums.DeploymentStatusTypeEnum
  alias Models.Events.EventDefinition
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  # ----------------------------- #
  # --- Kafka Consumer Method --- #
  # ----------------------------- #
  def handle_deployment_messages(message_set) do
    for %Message{value: message} <- message_set do
      case Jason.decode(message, keys: :atoms) do
        {:ok, decoded_message} ->
          case Map.get(decoded_message, :object_type, nil) do
            nil ->
              CogyntLogger.warn(
                "#{__MODULE__}",
                "object_type key is missing from Deployment Stream message. #{
                  inspect(decoded_message, pretty: true)
                }"
              )

            "event_type" ->
              # Temp Store id as `authoring_event_definition_id` until field can be removed
              Map.put(decoded_message, :authoring_event_definition_id, decoded_message.id)
              |> Map.put(:topic, decoded_message.filter)
              |> Map.put(:title, decoded_message.name)
              |> Map.put_new_lazy(:event_type, fn ->
                if is_nil(decoded_message.dsType) do
                  :none
                else
                  decoded_message.dsType
                end
              end)
              |> Map.drop([:id])
              |> EventsContext.upsert_event_definition()

            "deployment" ->
              # Upsert Deployments
              {:ok, %Deployment{} = deployment} = upsert_deployment(decoded_message)

              # Fetch all event_definitions that exists and are assosciated with
              # the deployment_id
              current_event_definitions =
                EventsContext.query_event_definitions(
                  filter: %{
                    deployment_id: decoded_message.id
                  }
                )

              # If any of these event_definition_id are not in the list of event_definition_ids
              # passed in the deployment message, mark them as inactive and shut off the consumer.
              # If they are in the list make sure to update the DeploymentStatus if it needs to be changed
              Enum.each(current_event_definitions, fn %EventDefinition{
                                                        deployment_status: deployment_status,
                                                        id: event_definition_id,
                                                        authoring_event_definition_id:
                                                          authoring_event_definition_id
                                                      } = current_event_definition ->
                case Enum.member?(
                       decoded_message.event_type_ids,
                       authoring_event_definition_id
                     ) do
                  true ->
                    if deployment_status == DeploymentStatusTypeEnum.status()[:inactive] or
                         deployment_status == DeploymentStatusTypeEnum.status()[:not_deployed] do
                      EventsContext.update_event_definition(current_event_definition, %{
                        deployment_status: DeploymentStatusTypeEnum.status()[:active]
                      })
                    end

                  false ->
                    EventsContext.update_event_definition(current_event_definition, %{
                      active: false,
                      deployment_status: DeploymentStatusTypeEnum.status()[:inactive]
                    })

                    {:ok, consumer_state} =
                      ConsumerStateManager.get_consumer_state(event_definition_id)

                    if consumer_state.status != nil do
                      ConsumerStateManager.manage_request(%{
                        stop_consumer: event_definition_id
                      })
                    end
                end
              end)

              # Start Drilldown Consumer for Deployment
              ConsumerGroupSupervisor.start_child(:drilldown, deployment)

            _ ->
              nil
          end

        {:error, error} ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Failed to decode message for Deployment Topic. Message: #{
              inspect(message, pretty: true)
            }. Error: #{inspect(error, pretty: true)}"
          )
      end
    end
  end

  # --------------------------------- #
  # --- Deployment Schema Methods --- #
  # --------------------------------- #
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
  def upsert_deployment(attrs \\ %{}) do
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
    Repo.delete_all(Deployment)
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
