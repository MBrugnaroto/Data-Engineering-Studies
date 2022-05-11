#!/bin/bash

convert_img(){
	if [ ! -d converted_to_png ]
	then
		mkdir converted_to_png
	fi

	for image in *.jpg
	do
		image_without_extension=$(ls $image | awk -F. '{ print $1}')
		convert $image converted_to_png/$image_without_extension.png
	done
}

convert_img 2>error_log.txt
if [ $? -eq 0 ]
then
	echo "Successful converion"
else
	echo "There was a problem converting"
fi
