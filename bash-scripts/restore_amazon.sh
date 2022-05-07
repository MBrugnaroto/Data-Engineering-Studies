
#!/bin/bash

PATH_RESTORE=/home/mbrugnar/restore_amazon

aws s3 sync s3://mateusbrugnaroto-shell/$(date +%F) $PATH_RESTORE

cd $PATH_RESTORE

if [ -f $1.sql ]
then
	mysql -u root mutillidae < $PATH_RESTORE/$1.sql
	if [ $? -eq 0 ]
	then
		echo "The restore was successful"
	fi
else
	echo "The file you are looking for does not exist in the directory"
fi

