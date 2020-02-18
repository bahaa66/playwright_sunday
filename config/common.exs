use Mix.Config

config :migrations, :application,
  app_name: System.get_env("APPLICATION_NAME") || :cogynt_workstation_ingest,
  repo: System.get_env("REPO_MODULE") || CogyntWorkstationIngest.Repo
