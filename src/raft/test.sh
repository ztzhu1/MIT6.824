#!/bin/bash
esc=$(printf "\033")
if [[ -n $1 ]]
then
    if [[ $1 == "-race" ]]
    then
        flag="$1"
    else
        flag="-run $1"
    fi
fi

if [[ -n $2 ]]
then
    if [[ $2 == "-race" ]]
    then
        flag="${flag} $2"
    else
        flag="${flag} -run $2"
    fi
fi

go test ${flag} | sed -u -E "s/(.*?pass.*?)/${esc}[1;32m\1${esc}[0m/gi" | sed -u -E "s/(.*?fail[^u].*?)/${esc}[1;31m\1${esc}[0m/gi" | sed -u -E "s/(.*?Test .*?)/${esc}[1;35m\1${esc}[0m/gi"