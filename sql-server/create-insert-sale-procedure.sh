#!/bin/bash

mysql -u root <<EOF
        USE DB_TEST;
        DROP PROCEDURE IF EXISTS PR_INSERT_SALE;

        DELIMITER &&
        CREATE PROCEDURE PR_INSERT_SALE(vDate DATE, vMaxNotes INT, vMaxItens INT, vMaxQuantity INT)
        BEGIN
                DECLARE vClient VARCHAR(11);
                DECLARE vSaller VARCHAR(5);
                DECLARE vNumNote INT;
                DECLARE vTax FLOAT;
                DECLARE vCountNotes, vCountItens INT DEFAULT 1;
                DECLARE vQuantity INT;
                DECLARE vProduct VARCHAR(10);
                DECLARE vProductPrice FLOAT;

                SELECT MAX(NUMERO) INTO vNumNote FROM notas_fiscais;
                SELECT AVG(IMPOSTO) INTO vTax FROM notas_fiscais;
                SET @insertNotesQuery = "INSERT INTO notas_fiscais(CPF, MATRICULA, DATA_VENDA, NUMERO, IMPOSTO) VALUES ";
                SET @insertItensQuery = 'INSERT INTO itens_notas_fiscais(NUMERO, CODIGO_DO_PRODUTO, QUANTIDADE, PRECO) VALUES ';

                WHILE vCountNotes <= vMaxNotes
                DO
                        SET vClient = FN_RAND_CLIENT();
                        SET vSaller = FN_RAND_SELLER();
                        SET vNumNote = vNumNote + 1;
                        SET vCountItens = 0;
                        
                        SET @insertNotesQuery = CONCAT(@insertNotesQuery, '(', '"',vClient,'"', ',', '"', vSaller, '"', ',', '"', vDate, '"', ',', vNumNote, ',', vTax, ')|');
                        
                        WHILE vCountItens < vMaxItens
                        DO
                                SET vQuantity = FN_RAND_NUM(1, vMaxQuantity);
                                SET vProduct = FN_RAND_PRODUCT();
                                SELECT COUNT(*) INTO @vValid FROM itens_notas_fiscais WHERE NUMERO = vNumNote AND CODIGO_DO_PRODUTO = vProduct;
                                SELECT COUNT(*) INTO @vValidQuery FROM (SELECT @insertItensQuery AS COMPARE) AS AUX where COMPARE LIKE (CONCAT('%',vNumNote, ',', vProduct, '%'));

                                IF @vValid = 0 AND @vValidQuery = 0 THEN
                                        SELECT PRECO_DE_LISTA INTO vProductPrice FROM tabela_de_produtos WHERE CODIGO_DO_PRODUTO = vProduct;
                                        SET @insertItensQuery = CONCAT(@insertItensQuery, '(', vNumNote, ',', vProduct, ',', vQuantity, ',', vProductPrice, ')|');
                                END IF;

                                SET vCountItens = vCountItens+1;
                        END WHILE;
                        SET vCountNotes = vCountNotes+1;
                END WHILE;

                SET @insertNotesQuery = REPLACE(@insertNotesQuery, ')|(', '),(');
                SET @insertNotesQuery = REPLACE(@insertNotesQuery, '|', ';');

                Prepare stmt FROM @insertNotesQuery;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;

                SET @insertItensQuery = REPLACE(@insertItensQuery, ')|(', '),(');
                SET @insertItensQuery = REPLACE(@insertItensQuery, '|', ';');

                PREPARE stmt FROM @insertItensQuery;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
        END &&
        DELIMITER ;
EOF
if [ $? -eq 0 ]
then
        echo 'Creation of function in' $db 'database was successfull.'
else
        echo 'Creation of function in' $db 'database was not performed.'
fi
