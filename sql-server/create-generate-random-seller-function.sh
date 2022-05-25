#!/bin/bash

mysql -u root <<EOF
        USE DB_TEST;
        SET GLOBAL log_bin_trust_function_creators = 1;
        DROP FUNCTION IF EXISTS FN_RAND_SELLER;

        DELIMITER &&
        CREATE FUNCTION FN_RAND_SELLER()
        RETURNS VARCHAR(5)
        BEGIN
               	DECLARE vSeller VARCHAR(5);
    		DECLARE vPosition INT DEFAULT 0;
    		DECLARE vMax INT DEFAULT 0;
    		SELECT COUNT(*) INTO vMax FROM tabela_de_vendedores;
    		SET vPosition = FN_RAND_NUM(1, vMax)-1;
		SELECT MATRICULA INTO vSeller FROM tabela_de_vendedores LIMIT vPosition, 1;
		RETURN vSeller;
	END &&
        DELIMITER ;
EOF
if [ $? -eq 0 ]
then
        echo 'Creation of function in DB_TEST database was successfull.'
else
        echo 'Creation of function in DB_TEST database was not performed.'
fi
