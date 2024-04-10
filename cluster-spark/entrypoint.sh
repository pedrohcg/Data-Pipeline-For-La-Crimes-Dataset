#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then

  hdfs namenode -format

  # Inicializa os processos no master
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  # Cria as pastas necessárias
  while ! hdfs dfs -mkdir -p /spark-logs;
  do
    echo "Falha ao criar a pasta /spark-logs no hdfs"
  done
  
  echo "Criada a pasta /spark-logs no hdfs"
  hdfs dfs -mkdir -p /opt/spark/data
  echo "Criada a pasta /opt/spark/data no hdfs"


  # Copia os dados para o HDFS
  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data
  hdfs dfs -ls /opt/spark/data

  # Instala Terraform e GCP Shell
  TERRAFORM_VERSION=1.6.4

  wget https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip && \
    unzip terraform_${TERRAFORM_VERSION}_linux_amd64.zip && \
    mv terraform /usr/local/bin/ && \
    rm terraform_${TERRAFORM_VERSION}_linux_amd64.zip

  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-sdk -y
  
elif [ "$SPARK_WORKLOAD" == "worker" ];
then

  # Inicializa processos no worker
  hdfs --daemon start datanode
  yarn --daemon start nodemanager

elif [ "$SPARK_WORKLOAD" == "history" ];
then

  while ! hdfs dfs -test -d /spark-logs;
  do
    echo "spark-logs não existe ainda...criando"
    sleep 1;
  done
  echo "Exit loop"

  # Inicializa o history server
  start-history-server.sh
fi

tail -f /dev/null
