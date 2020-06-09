########################
## Variables
########################
export APP_NAME = dst-cogynt-workstation-ingest
export DOCKER_FILE = ./build/Dockerfile

BUILD_HARNESS_ORG=Cogility
BUILD_HARNESS_PROJECT=build-harness
BUILD_HARNESS_BRANCH ?= master
export BUILD_HARNESS_PATH ?= $(shell 'pwd')/.build-harness
ifdef GH_TOKEN
	BUILD_HARNESS_REPO="https://$(GH_TOKEN)@github.com/${BUILD_HARNESS_ORG}/${BUILD_HARNESS_PROJECT}.git"
else
	BUILD_HARNESS_REPO="https://github.com/${BUILD_HARNESS_ORG}/${BUILD_HARNESS_PROJECT}.git"
endif

########################
## Build Harness
########################
init::
	@if [[ -x $(BUILD_HARNESS_PATH) ]]; then exit 0; else \
	echo '-> Downloading Build Harness... '; \
	git clone -c advice.detachedHead=false --depth=1 -b $(BUILD_HARNESS_BRANCH) $(BUILD_HARNESS_REPO) $(BUILD_HARNESS_PATH); exit 0; fi;

build-harness/update:
	@if [ "$(BUILD_HARNESS_PATH)" ] && [ -d "$(BUILD_HARNESS_PATH)" ]; then \
		echo "Removing existing $(BUILD_HARNESS_PATH)"; \
		rm -rf "$(BUILD_HARNESS_PATH)"; \
		$(MAKE) init; \
	fi;
	
-include $(BUILD_HARNESS_PATH)/Makefile