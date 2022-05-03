#!/bin/bash

convert_img(){
	if [ ! -d converted_to_png ]
	then
		 mkdir converted_to_png
	fi

	local path_img=$1

	IFS="/" read -a new_path <<< $path_img
	local filename=${new_path[-1]}
	new_path[${#new_path[@]}-1]="converted_to_png"
	new_path=$(printf '%s\n' "$(IFS=/; printf '%s' "${new_path[*]}")")

	IFS="." read -a new_extension <<< $filename
	new_extension[${#new_extension[@]}-1]="png"
	new_filename=$(IFS=. ; echo "${new_extension[*]}")

	convert $path_img $new_path/$new_filename
}

go_through_directory(){
	cd $1
	local work_path=$2

	for file in *
	do 
		local path_file=$(find $work_path -name $file)

		if [ -d $path_file ]
		then
			go_through_directory $path_file $work_path
		else
			convert_img $path_file
		fi
	done
}

echo "The images in the directory will be converted to png: $PWD"

go_through_directory $PWD $PWD
if [ $? -eq 0 ]
then
	echo "Successful conversion"
else
	echo "There was a problem converting"
fi
