#!/bin/bash

for db in "$@"
do
mysql -u root <<EOF
        USE $db;
		SET GLOBAL log_bin_trust_function_creators = 1;
		DROP FUNCTION IF EXISTS FN_RAND_NUM;

		DELIMITER &&
		CREATE FUNCTION FN_RAND_NUM(MIN INT, MAX INT)
		RETURNS INT
		BEGIN
			DECLARE RESULT INT DEFAULT 0;
			SELECT FLOOR((RAND()*(MAX-MIN))+MIN) INTO RESULT;
			RETURN RESULT;
		END &&
		DELIMITER ;
EOF
if [ $? -eq 0 ]
then
	echo 'Creation of function in' $db 'database was successfull.'
else
	echo 'Creation of function in' $db 'database was not performed.'
fi
done
