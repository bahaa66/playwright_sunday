# ---- Build Stage ----
FROM 478685488679.dkr.ecr.us-west-2.amazonaws.com/centos-elixir:1.8.2 AS builder

# Set environment variables for building the application
ARG MIX_ENV=prod
ARG APP_NAME=cogynt_workstation_ingest
ARG TEST=1
ARG USE_LOCAL_CEP=false
ARG use_local=false
ARG RELEASE_VERSION=0.0.0
ENV RELEASE_VERSION=${RELEASE_VERSION}

# Set working application build directory
WORKDIR /app

# Copy over all the necessary application files and directories
COPY config ./config
COPY lib ./lib
COPY priv ./priv
COPY rel ./rel
COPY mix.exs .
COPY mix.lock .
RUN cat ./config/dev.exs > ./config/prod.exs &&\
    sed -i "s/debug_errors: true/debug_errors: false/" ./config/prod.exs &&\
    sed -i "s/code_reloader: true/code_reloader: false/" ./config/prod.exs &&\
    cat ./config/prod.exs &&\
    cat ./config/prod.exs > ./rel/runtime_config.exs
# Fetch the application dependencies and build the application
RUN mix deps.get
RUN mix deps.update tzdata
RUN MIX_ENV=${MIX_ENV} mix distillery.release --env=prod --verbose

RUN RELEASE_DIR=`ls -d _build/${MIX_ENV}/rel/${APP_NAME}/releases/*/` && \
  mkdir /dist && \
  tar -xf "${RELEASE_DIR}/${APP_NAME}.tar.gz" -C /dist && \
  chown -R cogility:cogility /dist


# ---- Application Stage ----
FROM 478685488679.dkr.ecr.us-west-2.amazonaws.com/centos-base:7.6 AS app

ARG APP_NAME=cogynt-workstation-ingest
ARG RELEASE_VERSION=0.0.1
ENV RELEASE_VERSION=${RELEASE_VERSION}

# Copy over the build artifact from the previous step and create a non root user
WORKDIR /opt/cogility
COPY --from=builder --chown=cogility:cogility /dist app
RUN setcap 'cap_net_bind_service=+ep' /opt/cogility/app/erts-10.5/bin/beam.smp

USER cogility

# Run the app
CMD ["./app/bin/cogynt-workstation-ingest", "foreground"]