#!/bin/bash

# Executes the extract-transform job
spark-submit --jars jars/mssql-jdbc-12.6.1.jre11.jar --deploy-mode client ./apps/extract-transform.py

# Copy transformed data from hdfs to local
hdfs dfs -copyToLocal ./transformed_data ./data

# Send processed data files to a txt that will be used in the load phase
find ./data/transformed_data -name "*.csv" > ./data/files.txt

perl -p -i -e 'chomp if eof' ./data/files.txt

cd IAC
terraform apply -auto-approve