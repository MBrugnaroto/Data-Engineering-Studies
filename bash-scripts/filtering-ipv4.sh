#!/bin/bash

regex="\b([0-9]{1,3}.){3}[0-9]{1,3}\b"
ip=$1

if [[ $ip =~ $regex ]]
then
	cat *.log | grep $ip
	if [ $? -ne 0 ]
	then
		echo "The IP address "$ip" sought is not present in the file"
	fi
else
	echo "Invalid IPv4 format: "$ip
fi
