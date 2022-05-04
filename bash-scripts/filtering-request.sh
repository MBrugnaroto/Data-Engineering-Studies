#!/bin/bash

request=$1

while [ -z $request ]
do
	read -p "You forgot to put the parameter (GET, PUT, POST, DELETE): " request
done

request_uppercase=$(echo $request | awk '{ print toupper($1) }')

case $request_uppercase in
	GET)
	cat *.log | grep GET
	;;

	POST)
	cat *.log | grep GET
	;;

	PUT)
	cat *.log | grep PUT
	;;

	DELETE)
	cat *.log | grep DELETE
	;;

	*)
	echo "The parameter passed is not valid"
	;;
esac
