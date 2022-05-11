#!/bin/bash

PATH_DUMP=/home/$(whoami)
cd $PATH_DUMP

if [ -d backup ]
then
	mkdir backup
fi

mysqldump -u root $1 > $1.sql
if [ $? -eq 0 ]
then
	echo "Backup was performed successfully"
else
	echo "There was a problem with the backup"
fi
