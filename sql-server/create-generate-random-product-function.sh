#!/bin/bash

mysql -u root <<EOF
        USE DB_TEST;
        SET GLOBAL log_bin_trust_function_creators = 1;
        DROP FUNCTION IF EXISTS FN_RAND_PRODUCT;

        DELIMITER &&
        CREATE FUNCTION FN_RAND_PRODUCT()
        RETURNS VARCHAR(11)
        BEGIN
               	DECLARE vProduct VARCHAR(11);
    		DECLARE vPosition INT DEFAULT 0;
    		DECLARE vMax INT DEFAULT 0;
    		SELECT COUNT(*) INTO vMax FROM tabela_de_produtos;
    		SET vPosition = FN_RAND_NUM(1, vMax)-1;
		SELECT CODIGO_DO_PRODUTO INTO vProduct FROM tabela_de_produtos LIMIT vPosition, 1;
		RETURN vProduct;
	END &&
        DELIMITER ;
EOF
if [ $? -eq 0 ]
then
        echo 'Creation of function in' $db 'database was successfull.'
else
        echo 'Creation of function in' $db 'database was not performed.'
fi
