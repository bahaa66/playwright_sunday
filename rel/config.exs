# Import all plugins from `rel/plugins`
# They can then be used by adding `plugin MyPlugin` to
# either an environment, or release definition, where
# `MyPlugin` is the name of the plugin module.
~w(rel plugins *.exs)
|> Path.join()
|> Path.wildcard()
|> Enum.map(&Code.eval_file(&1))
use Distillery.Releases.Config,
    # This sets the default release built by `mix release`
    default_release: :default,
    # This sets the default environment used by `mix release`
    default_environment: :prod
# For a full list of config options for both releases
# and environments, visit https://hexdocs.pm/distillery/config/distillery.html

# You may define one or more environments in this file,
# an environment's settings will override those of a release
# when building in that environment, this combination of release
# and environment configuration is called a profile
environment :dev do
  # If you are running Phoenix, you should make sure that
  # server: true is set and the code reloader is disabled,
  # even in dev mode.
  # It is recommended that you build with MIX_ENV=prod and pass
  # the --env flag to Distillery explicitly if you want to use
  # dev mode.
  set include_src: false
  set include_system_libs: true
  set include_erts: true
  set cookie: :"&TtWkBmOpfd9V>@D[:}hT*ibY$gR=`Bs9|5N9N%4na;DwM~{~~U3h$bTYVnTE894"
  set vm_args: "rel/vm.args"
end

environment :prod do
  set include_erts: true
  set include_system_libs: true
  set include_src: false
  set cookie: :">CJ!Fd]3C?7RL%&F1%{F=6@;{i&HX4{XjhZ3vJCrV;9tg3bO|6:n5Oi9BtnI,A(h"
  set pre_start_hooks: "rel/hooks/pre_start"
  set vm_args: "rel/vm.args"
end
# You may define one or more releases in this file.
# If you have not set a default release, or selected one
# when running `mix release`, the first release in the file
# will be used by default
release :cogynt_workstation_ingest do
  set version: System.get_env("RELEASE_VERSION")
  set applications: [
    :runtime_tools
  ]
  set config_providers: [
    {Distillery.Releases.Config.Providers.Elixir, ["${RELEASE_ROOT_DIR}/etc/runtime_config.exs"]}
  ]
  set overlays: [
    {:copy, "rel/runtime_config.exs", "etc/runtime_config.exs"}
  ]
end
