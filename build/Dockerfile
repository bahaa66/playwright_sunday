# ---- Build Stage ----
FROM 478685488679.dkr.ecr.us-west-2.amazonaws.com/common/ubi8/elixir/1.12:1.6.2 AS builder

# Set environment variables for building the application 
ARG APP_NAME=cogynt_workstation_ingest
ENV MIX_ENV=prod \
    NODEJS_VERSION=16 \
    TEST=1 \
    USE_LOCAL_CEP=false \
    use_local=false

# Copy over all the necessary application files and directories
COPY . .

# RUN cat ./config/dev.exs > ./config/prod.exs \
#     && sed -i "s/debug_errors: true/debug_errors: false/" ./config/prod.exs \
#     && sed -i "s/code_reloader: true/code_reloader: false/" ./config/prod.exs \
#     && cat ./config/prod.exs > ./config/runtime.exs
# Fetch the application dependencies and build the application
RUN mix local.hex --force \
    && mix local.rebar --force \
    && mix deps.get --only prod \
    && mix deps.compile
RUN if [ -d "assets" ] ; \
    then \
    set -xe \
    && microdnf -y install yum \
    && yum -y remove nodejs \
    && curl -sL https://rpm.nodesource.com/setup_${NODEJS_VERSION}.x | bash - \
    && yum -y --setopt=tsflags=nodocs install nodejs \
    && yum -y clean all --enablerepo='*' \
    && cd assets \
    && npm install \
    && cd .. \
    && npm run deploy --prefix ./assets \
    && mix phx.digest ; \
    fi ;

RUN mix compile \
    && mix release --path ./dist

# ---- Application Stage ----
FROM 478685488679.dkr.ecr.us-west-2.amazonaws.com/common/ubi8/minimal:1.6.2 AS app

ARG APP_NAME=cogynt_workstation_ingest

COPY --from=builder --chown=${APP_USER}:${APP_USER} ${APP_ROOT}/dist ${APP_ROOT}
COPY --from=builder --chown=${APP_USER}:${APP_USER} ${APP_ROOT}/rel/hooks ${APP_ROOT}
RUN mv ${APP_ROOT}/bin/${APP_NAME} ${APP_ROOT}/bin/server
# Run the app
CMD ["sh", "-c", "${APP_ROOT}/pre_start.sh ; server start"]