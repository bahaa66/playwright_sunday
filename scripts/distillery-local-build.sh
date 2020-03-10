#!/bin/bash
VERSION="$(git rev-parse --short HEAD)"
RELEASE_DIR="./_build/prod/rel/cogynt_workstation_ingest/releases/${VERSION}"

export RELEASE_VERSION=${VERSION}
export PATH=$PATH:~/.mix
export MIX_ENV=prod

cat config/dev.exs > config/prod.exs

sed -i "s/debug_errors: true/debug_errors: false/" config/prod.exs
sed -i "s/code_reloader: true/code_reloader: false/" config/prod.exs

cat config/prod.exs > rel/runtime_config.exs

pkill -9 epmd
pkill -9 erl_child_setup

if [[ ! -d priv/static ]]; then
	mkdir  priv
    mkdir  priv/static
fi

mix local.hex --force && mix local.rebar --force && mix deps.get 

# Compile with distillery
mix distillery.release.clean

mix  phx.digest
#Compile with distillery
mix distillery.release  --env=prod --verbose

if [ -d $RELEASE_DIR ]
then
echo "Build Complete"
else
echo "Build failed"
exit 1
fi