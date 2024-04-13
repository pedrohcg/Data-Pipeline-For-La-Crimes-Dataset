
# Pipeline de Dados Usando Processamento Distribuído com Spark e Visualização de Dados com Google BigQuery

## Descrição do Projeto

Neste projeto foi desenvolvida uma pipeline ETL onde os dados vão ser extraídos de uma instância de banco de dados Microsoft SQL Server, após a extração é usado PySpark para a limpeza e transformação de dados e, por fim, a carga e a criação da estrutura no BigQuery é feita com Terraform onde os arquivos resultantes do processamento no cluster Spark são carregados a um Bucket na GCP e então carregados para dentro do BigQuery através de jobs que também foram criados pelo Terraform.

## Objetivos do Projeto
O principal objetivo deste projeto é demonstrar minha capacidade de criar Pipelines de Dados de uma ponta a outra, bem como demonstrar minhas habilidades com diversas ferramentas que são de extrema importância para um engenheiro de dados como SGBDs, Apache Spark, PySPark,outilização de serviços de nuvem e Google BigQuery.

Além disso, outro objetivo importante deste projeto é responder as seguintes perguntas com análise de dados:

- pergunta1
- pergunta2
- pergunta3
- pergunta4
- pergunta5
- pergunta6
- pergunta7
- pergunta8
- pergunta9
- pergunta10


## Tecnologias

As tecnologias utilizadas neste projeto foram:

 - [Docker](https://www.docker.com/)
 - [Python](https://www.python.org/)
 - [PySpark](https://spark.apache.org/docs/latest/api/python/index.html)
 - [Apache Spark](https://spark.apache.org/)
 - [Microsoft SQL Server](https://www.microsoft.com/pt-br/sql-server/sql-server-downloads)
 - [Terraform](https://www.terraform.io/)

## Dataset
O dataset usado neste projeto foi o [Crime Data from 2020 to Present] disponível no Kaggle e pode ser acessado nesse [link](https://www.kaggle.com/datasets/sahirmaharajj/crime-data-from-2020-to-present-updated-monthly/data). Abaixo temos um diagrama do dataset original:

imagem-do-dataset-original.png

Ao todo o dataset possui 28 colunas, elas são:

- **DR_NO**: identificador único dos registros;
- **Date_Rptd**: data que o crime foi reportado;
- **Date_OCC**: data que o crime aconteceu;
- **Time_OCC**: horário que o crime aconteceu;
- **Area**: id da área do crime; 
- **Area_Name**: nome da área do crime;
- **Rpt_Dist_No**: da pra deletar;
- **Part_1-2**: da pra deletar;
- **Crmd_Cd**: código do crime;
- **Crmd_Desc**: descrição do crime;
- **Mocodes**: atvidades associadas a quem cometeu o crime;
- **Vict_Age**: idade da vítima;
- **Vict_Sex**: sexo da vítima;
- **Vict_Descent**: descendência da vítima;
- **Premis_Cd**: código do tipo de estrutura, veículo ou localização do crime;
- **Premis_Desc**: descrição do código descrito no item anterior;
- **Weapon_Used_Cd**: código da arma usada no crime;
- **Weapon_Desc**: descrição do código descrito no item anterior;
- **Status**: código do status do crime;
- **Status_Desc**: descrição do código descrito no item anterior;
- **Crmd_Cd_1**: código do crime, mesmo que Crmd_Cd
- **Crmd_Cd_2, Crmd_Cd_3 e Crmd_Cd_4**: códigos de crimes menores;
- **Location**: endereço da rua do crime;
- **Lat e Lon**: latitude e longitude do lugar;

Também fazemos uso da descrição dos códigos descritos no campo "Mocodes" que podem ser obtidos no site https://data.lacity.org/. Os códigos serão disponibilizados com o nome MO_CODES-MO_CODES_Numerical_20191119.csv na pasta "scripts-database" do projeto para facilitar o uso, porém, o csv do dataset deverá ser baixado diretamente do Kaggle por causa do seu tamanho.

## Extração de Dados
A extração de dados é feita 

## Como Usar

Para executar este projeto um dos pré-requisitos será ter Docker instalado no seu computador, você pode encontrar um tutorial de como instalar o Docker para o seu sistema operacional [aqui](https://docs.docker.com/engine/install/). Também é necessário ter uma conta ativa na GCP para que seja possível usar os recursos do BigQuery e Storage Bucket.

Com o Docker instalado e uma conta na GCP abra a linha de comando do seu sistema operacional e execute os seguintes passos:

```bash
# Clone o repositório para o seu diretório local
$ git clone https://github.com/pedrohcg/Data-Pipeline-For-La-Crimes-Dataset.git

# Então entre no diretório do projeto pela linha de comando

# Vamos primeiro criar o cluster do spark, para isso precisamos entrar na pasta cluster-spark
cd cluster-spark

# E então executamos o comando para levantar o cluster. A quantidade de workers pode ser alterada na flag "scale", para esse projeto vou criar um cluster com 5 workers.
docker-compose -f docker-compose.yml up -d --scale spark-worker-yarn=5

# Com o cluster pronto temos que configurar o CLI do GCP no master do cluster, para isso temos que nos conectar ao master com o comando
$ docker exec -it spark-master-yarn /bin/bash

# Outra opção é se conectar diretamente pelo Docker Desktop

# Conectado no container vamos executar os comandos
$ gcloud auth application-default login

# E então é só seguir com a autenticação do Google. Obs: caso você tenha optado por se conectar ao master via linha de comando não se esqueça de sair do container antes de executar os próximos

# Com o cluster pronto precisamos preparar o banco de dados, para isso vamos entrar na pasta scripts-database
$ cd ../scripts-database

# Então temos que criar uma imagem para o container que vai rodar o banco de dados com o seguinte comando
$ docker build -t mssql-database-data-pipeline .

# Depois vamos criar o container usando a imagem que acabamos de criar, também passamos a flag "network" para colocar a container na mesma network que o cluster spark para que assim seja possível a comunicação entre ambos
$ docker run -p 1433:1433 --name mssql2 --network cluster-yarn_default -d mssql-database-data-pipeline

# O banco de dados que vai ser criado com essa imagem já terá os dados do dataset importados

# Agora que já temos quase tudo pronto para executar a pipeline vamos verificar qual foi o ip que o docker atribuiu ao conteiner do banco
$ docker network inspect cluster-yarn_default

# E então copiamos o valor do campo "IPv4Address" do container "mssql" e o substituimos no valor da variável "database_ip" no arquivo extract-transform.py que fica na pasta cluster-spark/jobs/

# Com esses passos feitos precisamos apenas executar a pipeline, para isso executamos o seguinte comando
$ docker exec spark-master-yarn ./pipeline.sh

# Pronto! Agora você tem um DW na nuvem pronto para ser usado para análises.
```
