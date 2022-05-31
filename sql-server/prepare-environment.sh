#!/bin/bash
DB='DB_TEST'

/bin/bash create-database.sh $DB
/bin/bash up-dump-database.sh $DB
/bin/bash create-table.sh $DB "round_statistics"
/bin/bash create-generate-random-number-function.sh $DB
/bin/bash create-generate-random-client-function.sh $DB
/bin/bash create-generate-random-product-function.sh $DB
/bin/bash create-generate-random-seller-function.sh $DB
/bin/bash create-handle-query-function.sh $DB
/bin/bash create-query-executor-procedure.sh $DB
/bin/bash create-insert-sale-procedure.sh $DB