#!/bin/sh

echo 'Setting ERLANG_NAME...'
export ERLANG_NAME=$POD_IP
echo $ERLANG_NAME