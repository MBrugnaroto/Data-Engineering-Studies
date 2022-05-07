
#!/bin/bash

PATH_BACKUP=/home/mbrugnar/backup_mutillidae_amazon

cd $PATH_BACKUP

tables=$(mysql -u root mutillidae -e "show tables;" | grep -v Tables)
today=$(date +%F)

if [ ! -d today ]
then 
	mkdir $today
fi

for table in $tables
do
	mysqldump -u root mutillidae $table > $PATH_BACKUP/$today/$table.sql
done

aws s3 sync $PATH_BACKUP s3://mateusbrugnaroto-shell
