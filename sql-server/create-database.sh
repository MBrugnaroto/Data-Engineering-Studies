#!/bin/bash

mysql -u root <<EOF
	DROP DATABASE IF EXISTS sucos_vendas;

	CREATE DATABASE IF NOT EXISTS sucos_vendas
	DEFAULT CHARACTER SET UTF8;

	SHOW DATABASES;
EOF

