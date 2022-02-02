#!/bin/bash
set -e
echo "Running Elasticsearch startup tasks"
server eval "Elixir.CogyntWorkstationIngest.ReleaseTasks.eval_elasticsearch"
echo "Elasticsearch Event Index ran succesfully"