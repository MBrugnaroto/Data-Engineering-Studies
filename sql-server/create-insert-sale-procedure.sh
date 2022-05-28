#!/bin/bash

mysql -u root <<EOF
        USE $1;
        DROP PROCEDURE IF EXISTS PR_INSERT_SALE;

        DELIMITER &&
        CREATE PROCEDURE PR_INSERT_SALE(vDate DATE, vMaxNotes INT, vMaxItens INT, vMaxQuantity INT, vCommitValue INT)
        BEGIN
                DECLARE vClient VARCHAR(11);
                DECLARE vSaller VARCHAR(5);
                DECLARE vNumNote INT;
                DECLARE vTax FLOAT;
                DECLARE vCountNotes, vCountItens INT DEFAULT 1;
                DECLARE vQuantity INT;
                DECLARE vProduct VARCHAR(10);
                DECLARE vProductPrice FLOAT;
                DECLARE vCommit INT DEFAULT 0;
                DECLARE vNotesValues LONGTEXT DEFAULT '';
                DECLARE vItensValues LONGTEXT DEFAULT '';
                DECLARE vTotalTime FLOAT DEFAULT 0.0;

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
                        SET vNotesValues = CONCAT(vNotesValues, '(', '"',vClient,'"', ',', '"', vSaller, '"', ',', '"', vDate, '"', ',', vNumNote, ',', vTax, ')|');
                        
                        WHILE vCountItens < vMaxItens
                        DO
                                SET vQuantity = FN_RAND_NUM(1, vMaxQuantity);
                                SET vProduct = FN_RAND_PRODUCT();
                                SELECT COUNT(*) INTO @vValid FROM itens_notas_fiscais WHERE NUMERO = vNumNote AND CODIGO_DO_PRODUTO = vProduct;
                                SELECT COUNT(*) INTO @vValidQuery FROM (SELECT vItensValues AS COMPARE) AS AUX where COMPARE LIKE (CONCAT('%',vNumNote, ',', vProduct, '%'));

                                IF @vValid = 0 AND @vValidQuery = 0 THEN
                                        SELECT PRECO_DE_LISTA INTO vProductPrice FROM tabela_de_produtos WHERE CODIGO_DO_PRODUTO = vProduct;
                                        SET vItensValues = CONCAT(vItensValues, '(', vNumNote, ',', vProduct, ',', vQuantity, ',', vProductPrice, ')|');
                                END IF;
                                SET vCountItens = vCountItens+1;
                                SET vCommit = vCommit+1;
                                SELECT LENGTH(vItensValues) INTO @totalItens;

                                IF vCommitValue = vCommit AND @totalItens > 0
                                THEN
                                        SELECT FN_HANDLE_QUERY(vNotesValues) INTO @treatedNotesQuery;
                                        SELECT FN_HANDLE_QUERY(vItensValues) INTO @treatedItensQuery;
                                        
                                        SELECT CURTIME(6) INTO @start;
                                        CALL PR_QUERY_EXECUTOR(CONCAT(@insertNotesQuery, @treatedNotesQuery));
                                        CALL PR_QUERY_EXECUTOR(CONCAT(@insertItensQuery, @treatedItensQuery));
                                        SELECT CURTIME(6) INTO @end;
                                        SELECT TIME_TO_SEC(TIMEDIFF(@end, @start)) INTO @diffTime;
                                        SET vTotalTime = vTotalTime + @diffTime;
                                        SET vNotesValues = ''; 
                                        SET vItensValues = ''; 
                                        SET vCommit = 0;
                                END IF;
                        END WHILE;
                        SET vCountNotes = vCountNotes+1;
                END WHILE;
                
                SELECT LENGTH(vNotesValues) INTO @totalNotes;
                SELECT LENGTH(vItensValues) INTO @totalItens;
                
                IF @totalNotes > 0 
                THEN
                        SELECT FN_HANDLE_QUERY(vNotesValues) INTO @treatedNotesQuery;

                        SELECT CURTIME(6) INTO @start;
                        CALL PR_QUERY_EXECUTOR(CONCAT(@insertNotesQuery, @treatedNotesQuery));
                        SELECT CURTIME(6) INTO @end;
                        SELECT TIME_TO_SEC(TIMEDIFF(@end, @start)) INTO @diffTime;
                        SET vTotalTime = vTotalTime + @diffTime;
                END IF;

                IF @totalItens > 0
                THEN
                        SELECT FN_HANDLE_QUERY(vItensValues) INTO @treatedItensQuery;

                        SELECT CURTIME(6) INTO @start;
                        CALL PR_QUERY_EXECUTOR(CONCAT(@insertItensQuery, @treatedItensQuery));
                        SELECT CURTIME(6) INTO @end;
                        SELECT TIME_TO_SEC(TIMEDIFF(@end, @start)) INTO @diffTime;
                        SET vTotalTime = vTotalTime + @diffTime;
                END IF;

                SELECT vTotalTime AS 'TOTAL_TIME_INSERT';
        END &&
        DELIMITER ;
EOF
if [ $? -eq 0 ]
then
        echo 'Creation of Insert Rand Sale Procedure in' $1 'database was successfull.'
else
        echo 'Creation of Insert Rand Sale Procedure in' $1 'database was not performed.'
fi
