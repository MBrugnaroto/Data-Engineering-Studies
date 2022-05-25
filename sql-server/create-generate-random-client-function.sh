#!/bin/bash

mysql -u root <<EOF
        USE $1;
        SET GLOBAL log_bin_trust_function_creators = 1;
        DROP FUNCTION IF EXISTS FN_RAND_CLIENT;

        DELIMITER &&
        CREATE FUNCTION FN_RAND_CLIENT()
        RETURNS VARCHAR(11)
        BEGIN
                DECLARE vClient VARCHAR(11);
                DECLARE vPosition INT DEFAULT 0;
                DECLARE vMax INT DEFAULT 0;
                SELECT COUNT(*) INTO vMax FROM tabela_de_clientes;
                SET vPosition = FN_RAND_NUM(1, vMax)-1;
                SELECT CPF INTO vClient FROM tabela_de_clientes LIMIT vPosition, 1;
                RETURN vClient;
        END &&
        DELIMITER ;
EOF
if [ $? -eq 0 ]
then
        echo 'Creation of function in' $1 'database was successfull.'
else
        echo 'Creation of function in' $1 'database was not performed.'
fi
