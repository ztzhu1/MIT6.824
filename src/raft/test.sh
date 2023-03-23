#!/bin/bash
esc=$(printf "\033")
if [[ -n $1 ]]
then
    flag="-run $1"
fi
go test ${flag} | sed -u -E "s/(.*?pass.*?)/${esc}[1;32m\1${esc}[0m/gi" | sed -E "s/(.*?fail[^u].*?)/${esc}[1;31m\1${esc}[0m/gi"