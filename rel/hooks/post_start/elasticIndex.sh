#!/bin/bash
set -e
echo "Running Elasticsearch and seeds Index"
release_ctl eval "Elixir.CogyntWorkstationIngest.ReleaseTasks.seed"
echo "Elasticsearch Index and seeds run successfully"

