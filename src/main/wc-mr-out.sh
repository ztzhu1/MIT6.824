#!/bin/bash

word=$1
s=0

if [ -z $word ]; then
    word=a
fi

for file in ./mr-tmp/mr-out-*; do
    line=($(cat $file | grep "^${word} "))
    if [ ! -z ${line[0]} ]; then
        #        s=$((s + ${line[1]}))
        ((s += ${line[1]}))
    fi
done
echo "${word} $s"
