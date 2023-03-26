#!/bin/bash
if [[ -z $1 ]] || [[ -z $2 ]]
then
    echo "usage: batch_test 'test name' 'repeat num'"
    exit
fi

fail=false
trap ctrl_c INT
function ctrl_c() {
    fail=true
}

for (( i=0; i < $2; ++i ))
do
    echo -e "\x1b[1;34m${i}th test begins\x1b[0m"
    bash ./test.sh $1 | grep -ie "fail[^u]" && fail=true
    if [[ $fail = "true" ]]
    then
        break
    fi
done

if [[ $fail = "false" ]]
then
    echo -e "\x1b[1;32mPass\x1b[0m"
else
    echo -e "\x1b[1;31mFail\x1b[0m"
fi