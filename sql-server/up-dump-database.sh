#!/bin/bash

dump(){
	for file in *
 	do
                mysql -u root DB_TEST < $file
		if [ $? -eq 0 ]
		then
			echo "Dump from" $file "was peformed"
		else
			echo "Dump from" $file "was not performed"
		fi
	done
}

LOCAL=$(ls | grep dump)

if [ $(pwd | grep dump) ]
then
        dump
elif [ "${LOCAL:-0}" != 0 ]
then
        cd dump/
        dump
else
        echo "Dump folder not found."
fi