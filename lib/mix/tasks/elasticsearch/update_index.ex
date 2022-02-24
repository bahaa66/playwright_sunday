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
      [index, env, "active", datestring] ->
        now_string = DateTime.now!("Etc/UTC") |> Calendar.strftime("%Y%m%d%I%M%S")

        archive_file_name =
          "#{@index_definition_directory}/#{index}.#{env}.archived.#{datestring}.json"

        active_file_name =
          "#{@index_definition_directory}/#{index}.#{env}.active.#{now_string}.json"

        with :ok <-
               File.cp(
                 from,
                 "#{@index_definition_directory}/#{index}.#{env}.archived.#{datestring}.json"
               ),
             :ok <- File.rename(from, active_file_name) do
          {:ok, active_file_name, archive_file_name}
        else
          error -> error
        end

      [index, env, "active"] ->
        now = DateTime.now!("Etc/UTC")
        date_string = Calendar.strftime(now |> DateTime.add(-1, :second), "%Y%m%d%I%M%S")
        now_string = Calendar.strftime(now, "%Y%m%d%I%M%S")

        archive_file_name =
          "#{@index_definition_directory}/#{index}.#{env}.archived.#{date_string}.json"

        active_file_name =
          "#{@index_definition_directory}/#{index}.#{env}.active.#{now_string}.json"

        with :ok <-
               File.cp(
                 from,
                 "#{@index_definition_directory}/#{index}.#{env}.archived.#{date_string}.json"
               ),
             :ok <- File.rename(from, active_file_name) do
          {:ok, active_file_name, archive_file_name}
        else
          error -> error
        end
    end
  end
end
