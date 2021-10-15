#!/bin/bash
set -e
echo "Running Elasticsearch and seeds Index"
release_ctl eval "Elixir.CogyntWorkstationIngest.ReleaseTasks.elasticindexes"
echo "Elasticsearch Index and seeds run successfully"

