#!/bin/bash

if [[ (( $# == 1 )) && (( $1 > 0 )) && (( $1 < 7 )) ]]
then
	./TTDBClient.py < test$1.in | diff test$1.out -
elif [[ (( $# == 1 )) && -e $1.in && -e $1.out ]]
then
	./TTDBClient.py < $1.in | diff $1.out -
elif [[ (($# == 0)) ]]
then
  i=1
  while [[ -e test$i.in && -e test$i.out ]]
  do
    echo "Test $i"
    ./TTDBClient.py < test$i.in | diff test$i.out -
    i=$((i + 1))
  done
else
	echo "Must pass either a test number (1-6) or a test filename as a parameter."
fi
