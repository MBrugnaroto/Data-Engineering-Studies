#!/bin/bash

extract_memory_allocated_process(){
	if [ ! -d log ]
	then
		mkdir log
	fi

	processes=$(ps -e -o pid --sort -size | head -n 11 | grep [0-9])

	for pid in $processes
	do
		process_name=$(ps -p $pid -o comm=)
		echo -n $(date +%F,%H:%M:%S,) >> log/$process_name.log
		process_size=$(ps -p $pid -o size | grep [0-9])
		echo "$(bc <<< "scale=2;$process_size/1024") MB" >> log/$process_name.log
	done
}
extract_memory_allocated_process 2>error_log.txt
if [ $? -eq 0 ]
then
	echo "Extraction completed successfully"
else
	echo "There is a problem extracting the amount of memory allocated per process"
fi

