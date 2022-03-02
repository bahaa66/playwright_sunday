defmodule Mix.Tasks.Elasticsearch.UpdateIndex do
  @moduledoc """
  A mix task that creates a new json config used for indexes in elasticsearch. The task checks to see if there is an active json file that
  already exists for the index and for the optional --env command line argument passed (defaults to dev). If an active config already exists
  it copies it to an archived file with the following filename format <INDEX>.<ENV>.archived.<DATETIME>.json. It then renames the active file
  to have nows timestamp instead of the old datetime. This is the file you can now make edits to
  """
  @shortdoc "Update an exsisting elasticsearch index config or create a new one if it doesn't exist."

  use Mix.Task

  @index_definition_directory "priv/elasticsearch"

  @impl Mix.Task
  def run(argv) do
    case OptionParser.parse(argv, strict: [verbose: :boolean, env: :string]) do
      {_, [], _} ->
        Mix.shell().error(
          "You must provide an index name i.e. `mix elasticsearch.update_index test_index`"
        )

      {options, [index], []} ->
        env = Keyword.get(options, :env, "dev")

        with active_indexes when active_indexes != [] <-
               Path.wildcard("#{@index_definition_directory}/#{index}.#{env}.active*.json"),
             most_recent_active <- List.last(active_indexes),
             file_name <- Path.basename(most_recent_active, ".json"),
             {:ok, active_file_name, archive_file_name} <-
               archive_active(most_recent_active, file_name) do
          Mix.shell().info(
            "The contents of #{file_name} were copied to #{archive_file_name} and you can now make changes to the new #{active_file_name} config."
          )
        else
          [] ->
            Mix.shell().info(
              "Index definition not found for #{index}.#{env}. Creating a new one."
            )

            create_default_config(index, env)
            |> case do
              {:ok, file_name} ->
                Mix.shell().info(
                  "The new configs for index #{index} were created at #{file_name} and can now be modified."
                )

              {:error, error} ->
                Mix.shell().error(
                  "Could not create new index config for the following reason. Error: #{inspect(error)}"
                )
            end

          {:error, error} ->
            Mix.shell().error(
              "Could not create new index config for the following reason. Error: #{inspect(error)}"
            )
        end
    end
  end

  defp archive_active(from, file_name) do
    String.split(file_name, ".")
    |> case do
      [index, env, "active", date_string] ->
        now_string = DateTime.now!("Etc/UTC") |> Calendar.strftime("%Y%m%d%I%M%S")
        archive_file_name = archive_file_name(index, env, date_string)
        active_file_name = active_file_name(index, env, now_string)

        with :ok <- File.cp(from, archive_file_name),
             :ok <- File.rename(from, active_file_name) do
          {:ok, active_file_name, archive_file_name}
        else
          error -> error
        end

      [index, env, "active"] ->
        now = DateTime.now!("Etc/UTC")
        date_string = Calendar.strftime(now |> DateTime.add(-1, :second), "%Y%m%d%I%M%S")
        now_string = Calendar.strftime(now, "%Y%m%d%I%M%S")
        archive_file_name = archive_file_name(index, env, date_string)
        active_file_name = active_file_name(index, env, date_string)

        with :ok <- File.cp(from, archive_file_name),
             :ok <- File.rename(from, active_file_name) do
          {:ok, active_file_name, archive_file_name}
        else
          error -> error
        end
    end
  end

  defp create_default_config(index, env) do
    contents = %{
      settings: index_settings(env),
      mappings: %{}
    }

    file_name =
      active_file_name(
        index,
        env,
        DateTime.now!("Etc/UTC") |> Calendar.strftime("%Y%m%d%I%M%S")
      )

    if File.exists?(file_name) do
      {:error, :file_already_exists}
    else
      with {:ok, contents} <- Jason.encode(contents, pretty: true),
           :ok <- File.write(file_name, contents) do
        {:ok, file_name}
      else
        error ->
          nil
      end
    end
  end

  defp active_file_name(index, env, date),
    do: "#{@index_definition_directory}/#{index}.#{env}.active.#{date}.json"

  defp archive_file_name(index, env, date),
    do: "#{@index_definition_directory}/#{index}.#{env}.archived.#{date}.json"

  defp index_settings("prod") do
    %{
      index: %{
        analysis: %{
          analyzer: %{
            keyword_lowercase: %{
              type: "custom",
              tokenizer: "keyword",
              filter: [
                "lowercase"
              ]
            }
          }
        },
        max_result_window: 1_000_000,
        refresh_interval: "1s",
        number_of_shards: "2",
        number_of_replicas: "2"
      }
    }
  end

  defp index_settings(_) do
    %{
      index: %{
        analysis: %{
          analyzer: %{
            keyword_lowercase: %{
              type: "custom",
              tokenizer: "keyword",
              filter: [
                "lowercase"
              ]
            }
          }
        },
        max_result_window: 1_000_000,
        refresh_interval: "1s",
        number_of_shards: "1",
        number_of_replicas: "0"
      }
    }
  end
end
