#!/bin/bash
VERSION="$(git rev-parse --short HEAD)"
RELEASE_DIR="./_build/prod/rel/cogynt_workstation_ingest/releases/${VERSION}"

export RELEASE_VERSION=${VERSION}
export PATH=$PATH:~/.mix
export MIX_ENV=prod

cat config/dev.exs > ./config/prod.exs &&\
sed -i "s/debug_errors: true/debug_errors: false/" config/prod.exs &&\
sed -i "s/code_reloader: true/code_reloader: false/" config/prod.exs &&\
cat config/prod.exs &&\
cat config/prod.exs > rel/runtime_config.exs

if [[ ! -d priv/static ]]; then
	mkdir  priv
    mkdir  priv/static
fi

# Pull dependencies
mix local.hex --force && mix local.rebar --force && mix deps.get &&  mix deps.update tzdata

# Compile with distillery
mix distillery.release --env=prod --verbose

if [ ! -d $RELEASE_DIR ];
then
echo "Build failed"
exit 1
fi

echo "Packaging Release"
# Add appspec.yml to tar archive
cp -pr appspec.yml scripts $RELEASE_DIR
cd $RELEASE_DIR
shopt -s extglob
rm -rv !(cogynt-workstation-ingest.tar.gz|scripts|appspec.yml)
tar xvf cogynt-workstation-ingest.tar.gz
rm -f cogynt-workstation-ingest.tar.gz
zip -r cogynt-workstation-ingest.zip ./*
cp cogynt-workstation-ingest.zip /tmp
echo "Build complete"