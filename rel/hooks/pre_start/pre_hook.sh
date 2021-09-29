#!/bin/sh

echo 'Setting ERLANG_NAME...'
export ERLANG_NAME=$POD_IP
echo $ERLANG_NAME
export ERLANG_COOKIE=">CJ!Fd]3C?7RL%&F1%{F=6@;{i&HX4{XjhZ3vJCrV;9tg3bO|6:n5Oi9BtnI,A(h"
echo $ERLANG_COOKIE