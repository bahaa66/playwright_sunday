.DEFAULT_GOAL := build-image

########################
## Internal variables
########################
APP_NAME?=dst-cogynt-workstation-ingest
APP_HOST_PORT?=3002
AWS_REGION?=us-west-2
DOCKERFILE?=./build/Dockerfile
APP_ENV?=local

########################
## Docker Image Name
########################
ifdef ECR_URI
	export BUILD_VERSION=${GIT_BRANCH}.$(shell echo ${CODEBUILD_RESOLVED_SOURCE_VERSION} | head -c 8)
	IMAGE_NAME=${ECR_URI}:${BUILD_VERSION}
else
  IMAGE_NAME=${APP_NAME}
endif

########################
## Helpers variables
########################
M=$(shell printf "\033[34;1m▶\033[0m")
TIMESTAMP := $(shell /bin/date "+%Y-%m-%d_%H-%M-%S")

######
## Docker commands
######
.PHONY: build-image run-container

build-image: ecr-login ; $(info $(M) Building docker image...)
	docker build --file $(DOCKERFILE) --tag $(IMAGE_NAME) .

run-container: ; $(info $(M) Running docker container...)
	docker run \
	-e APP_ENV=$(APP_ENV) \
	-e APP_NAME=$(APP_NAME) \
	-e AWS_REGION=$(AWS_REGION) \
	--name $(APP_NAME) \
	-d $(APP_NAME)

sh-container:
	docker exec -it $(APP_NAME) /bin/sh

log-container:
	docker logs --tail="50" -f -t $(APP_NAME)

######
## Ship commands
######
.PHONY: ecr-login push-image

ecr-login: ; $(info $(M) Logging in to Amazon ECR...)
	$(shell aws ecr get-login --no-include-email --region ${AWS_REGION})

push-image: ecr-login ; $(info $(M) Pushing docker image...)
	docker push $(IMAGE_NAME)

publish: build-image push-image ; $(info $(M) Publishing docker image...)

helm-update: ; $(info $(M) Updating Helm deps...)
	helm dependency update helm/${APP_NAME}

ifdef RELEASE_VERSION
tag-helm: helm-update; $(info $(M) Tagging helm chart with semver...)
	sed -i.bak 's/tag:.*/tag: ${RELEASE_VERSION}/' helm/${APP_NAME}/values.yaml
	sed -i.bak 's/version:.*/version: ${RELEASE_VERSION}/' helm/${APP_NAME}/Chart.yaml
	sed -i.bak 's/appVersion:.*/appVersion: ${RELEASE_VERSION}/' helm/${APP_NAME}/Chart.yaml

push-semver: ecr-login ; $(info $(M) Pushing semver tagged docker image...)
	docker tag ${IMAGE_NAME} ${ECR_URI}:${RELEASE_VERSION}
	docker push ${ECR_URI}:${RELEASE_VERSION}
else
push-semver: ; $(info $(M) RELEASE_VERSION not set)
tag-helm: ; $(info $(M) RELEASE_VERSION not set)
endif

######
## Test commands
######
.PHONY: test lint

test: ; $(info $(M) Running application tests...)
	mix test --trace --formatter=JUnitFormatter --exclude integration

lint: ; $(info $(M) Running application linter...)
	
