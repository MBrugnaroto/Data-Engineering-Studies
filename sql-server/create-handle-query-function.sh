#!/bin/bash

for db in "$@"
do
mysql -u root <<EOF
        USE DB_TEST;
        SET GLOBAL log_bin_trust_function_creators = 1;
        DROP FUNCTION IF EXISTS FN_HANDLE_QUERY;

        DELIMITER $$
        CREATE FUNCTION FN_HANDLE_QUERY(QUERY LONGTEXT)
        RETURNS LONGTEXT
        BEGIN
            SET @FIRSTTREATMENT = REPLACE(QUERY, ')|(', '),(');
            SET @FINALTREATMENT = REPLACE(@FIRSTTREATMENT, '|', ';');
            RETURN @FINALTREATMENT;
        END $$
        DELIMITER ;
EOF
if [ $? -eq 0 ]
then
        echo 'Creation of function in' $db 'database was successfull.'
else
        echo 'Creation of function in' $db 'database was not performed.'
fi
done