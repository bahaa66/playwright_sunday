defmodule CogyntWorkstationIngest.Drilldown.DrilldownSinkConnector do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  @connectors_path "/connectors"
  @connector_status "/status"
  @connector_create "/config"
  @connector_task_restart "/restart"

  def kafka_connect_health() do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Checking kafka_connect_health @url: #{Config.kafka_connect_host()}"
    )

    HTTPoison.get(Config.kafka_connect_host())
    |> handle_response()
  end

  def create_or_update() do
    # check if connectors exist
    case connector_exists?() do
      {:ok, connector_list} ->
        ts_consumer_group = fetch_drilldown_connector_cgid(Config.ts_connector_name())
        tse_consumer_group = fetch_drilldown_connector_cgid(Config.tse_connector_name())

        if Enum.empty?(connector_list) or
             !Enum.member?(connector_list, ts_consumer_group) or
             !Enum.member?(connector_list, tse_consumer_group) do
          # no connectors so create connector which starts the task
          CogyntLogger.info(
            "#{__MODULE__}",
            "No connectors exists, creating new #{ts_consumer_group} and
          #{tse_consumer_group}"
          )

          with {:ok, _} <- create_connector_request(ts_consumer_group),
               {:ok, _} <- create_connector_request(tse_consumer_group) do
            CogyntLogger.info(
              "#{__MODULE__}",
              "Successfully created connector request #{ts_consumer_group} and
             #{tse_consumer_group}"
            )
          else
            {:error, reason} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "Failed to create connector because: #{inspect(reason)}"
              )

              {:error, reason}
          end
        else
          # if connector is present, check the status, if not running , start
          with {:ok, _} <- check_connector_status(ts_consumer_group),
               {:ok, _} <- check_connector_status(tse_consumer_group) do
            CogyntLogger.info(
              "#{__MODULE__}",
              "Successfully created connector request #{ts_consumer_group} and
             #{tse_consumer_group}"
            )
          else
            {:error, reason} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "Failed to create connector because: #{inspect(reason)}"
              )

              {:error, reason}
          end
        end

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to create connector because: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  def connector_exists?() do
    connectors_url = "#{Config.kafka_connect_host()}#{@connectors_path}"
    CogyntLogger.info("#{__MODULE__}", "Checking if connector_exists? @url: #{connectors_url}")

    HTTPoison.get(connectors_url)
    |> handle_response()
  end

  def connector_status?(name) do
    connector_status_url =
      "#{Config.kafka_connect_host()}#{@connectors_path}/#{name}#{@connector_status}"

    CogyntLogger.info(
      "#{__MODULE__}",
      "Checking if connector_status? @url: #{connector_status_url}"
    )

    HTTPoison.get(connector_status_url)
    |> handle_response()
  end

  def check_connector_status(connector_name) do
    case connector_status?(connector_name) do
      {:ok, %{connector: %{state: _state}, tasks: [%{id: id, state: state}]} = _} ->
        CogyntLogger.info("#{__MODULE__}", "#{connector_name} exists with state: #{state}")

        if state != "RUNNING" do
          delete_connector(connector_name)
          create_connector_request(connector_name)
        else
          restart_task(connector_name, id)
        end

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to create connector because: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  def create_connector_request(connector_name) do
    connector_create_url =
      "#{Config.kafka_connect_host()}#{@connectors_path}/#{connector_name}#{@connector_create}"

    content_type = %{"Content-Type" => "application/json"}
    {:ok, request} = request_body(connector_name)

    CogyntLogger.info(
      "#{__MODULE__}",
      "Making config_update_request: @url: #{connector_create_url} with request: #{request}"
    )

    HTTPoison.put(connector_create_url, request, content_type)
    |> handle_response()
  end

  def delete_connector(name) do
    delete_connector_url = "#{Config.kafka_connect_host()}#{@connectors_path}/#{name}"
    CogyntLogger.info("#{__MODULE__}", "Deleting connector @url: #{delete_connector_url}")
    # delete the connector
    HTTPoison.delete(delete_connector_url)
  end

  def restart_task(name, id) do
    restart_task_url =
      "#{Config.kafka_connect_host()}#{@connectors_path}/#{name}/tasks/#{id}#{
        @connector_task_restart
      }"

    CogyntLogger.info("#{__MODULE__}", "Restarting connector @url: #{restart_task_url}")
    HTTPoison.post(restart_task_url, "", %{"Content-Type" => "application/json"})
  end

  def fetch_drilldown_connector_cgid(topic_name, deployment_id \\ nil) do
    if is_nil(deployment_id) do
      case Redis.hash_get("dcgid", topic_name) do
        {:ok, nil} ->
          generate_connector_name(topic_name)

        {:ok, consumer_group_id} ->
          topic_name <> "-" <> consumer_group_id
      end
    else
      {:ok, brokers} = DeploymentsContext.get_kafka_brokers(deployment_id)
      hash_string = Integer.to_string(:erlang.phash2(brokers))

      case Redis.hash_get("dcgid", "#{topic_name}-#{hash_string}") do
        {:ok, nil} ->
          generate_connector_name(topic_name, deployment_id)

        {:ok, consumer_group_id} ->
          "#{topic_name}-#{hash_string}" <> "-" <> consumer_group_id
      end
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp request_body(connector_name) do
    config = %{
      "name" => connector_name,
      "connector.class" => "io.confluent.connect.jdbc.JdbcSinkConnector",
      "tasks.max" => 1,
      "connection.user" => Config.postgres_username(),
      "connection.password" => Config.postgres_password(),
      "connection.url" => "jdbc:postgresql://cogynt-workstationpg:5432/workstation_repo",
      "pk.mode" => "none",
      "errors.log.enable" => true,
      "errors.log.include.messages" => true,
      "value.converter.schema.registry.url" => "http://schemaregistry:8081",
      "insert.mode" => "insert",
      "auto.create" => false,
      "auto.evolve" => true,
      "batch.size" => 500,
      "value.converter" => "io.confluent.connect.avro.AvroConverter",
      "dialect.name" => "PostgreSqlDatabaseDialect",
      "key.converter" => "org.apache.kafka.connect.storage.StringConverter"
    }

    if String.contains?(connector_name, Config.tse_connector_name()) do
      Map.put(config, "topics", "template_solution_events")
      |> Map.put("transforms", "renameFields")
      |> Map.put(
        "transforms.renameFields.renames",
        "templateTypeName:template_type_name, templateTypeId:template_type_id, assertionName:assertion_name"
      )
      |> Map.put(
        "transforms.renameFields.type",
        "org.apache.kafka.connect.transforms.ReplaceField$Value"
      )
    else
      Map.put(config, "topics", "template_solutions")
      |> Map.put("transforms", "dropPrefix, renameFields")
      |> Map.put("transforms.dropPrefix.type", "org.apache.kafka.connect.transforms.RegexRouter")
      |> Map.put("transforms.dropPrefix.regex", ".*")
      |> Map.put("transforms.dropPrefix.replacement", "template_solution")
      |> Map.put(
        "transforms.renameFields.renames",
        "templateTypeName:template_type_name, templateTypeId:template_type_id"
      )
      |> Map.put(
        "transforms.renameFields.type",
        "org.apache.kafka.connect.transforms.ReplaceField$Value"
      )
    end
    |> Jason.encode()
  end

  defp generate_connector_name(topic_name, deployment_id \\ nil) do
    if is_nil(deployment_id) do
      cgid = "#{UUID.uuid1()}"

      case Redis.hash_set_if_not_exists("dcgid", topic_name, cgid) do
        {:ok, 0} ->
          {:ok, existing_id} = Redis.hash_get("dcgid", topic_name)
          topic_name <> "-" <> existing_id

        {:ok, 1} ->
          topic_name <> "-" <> cgid
      end
    else
      {:ok, brokers} = DeploymentsContext.get_kafka_brokers(deployment_id)

      hash_string = Integer.to_string(:erlang.phash2(brokers))

      cgid = "#{UUID.uuid1()}"

      case Redis.hash_set_if_not_exists("dcgid", "#{topic_name}-#{hash_string}", cgid) do
        {:ok, 0} ->
          {:ok, existing_id} = Redis.hash_get("dcgid", "#{topic_name}-#{hash_string}")
          "#{topic_name}-#{hash_string}" <> "-" <> existing_id

        {:ok, 1} ->
          "#{topic_name}-#{hash_string}" <> "-" <> cgid
      end
    end
  end

  defp handle_response({:error, %HTTPoison.Error{reason: reason}}), do: {:error, reason}

  defp handle_response({:ok, %HTTPoison.Response{status_code: 201, body: body}}),
    do: Jason.decode(body, keys: :atoms)

  defp handle_response({:ok, %HTTPoison.Response{status_code: 200, body: body}}),
    do: Jason.decode(body, keys: :atoms)

  defp handle_response({:ok, %HTTPoison.Response{status_code: 204, body: body}}),
    do: Jason.decode(body, keys: :atoms)

  defp handle_response({:ok, %HTTPoison.Response{status_code: 404}}), do: {:error, :not_found}

  defp handle_response({:ok, %HTTPoison.Response{status_code: 500, body: body}}) do
    case Jason.decode(body, keys: :atoms) do
      {:ok, %{error_code: 500, message: body}} ->
        {:error, body}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
