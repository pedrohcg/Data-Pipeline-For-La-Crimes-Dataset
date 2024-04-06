import os
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder.appName("Projeto1").getOrCreate()

# Define o Schema dos dados que vão ser extraídos do json
schema = types.StructType([
    types.StructField("nome", types.StringType(), True),
    types.StructField("idade", types.IntegerType(), True),
    types.StructField("email", types.StringType(), True),
    types.StructField("salario", types.IntegerType(), True),
    types.StructField("cidade", types.StringType(), True)
]) 

df_dsa = spark.read.schema(schema).json('data/usuarios.json')

# Drop da coluna email
df_dsa_sem_email = df_dsa.drop("email")

# Filtragem de dados
df = df_dsa_sem_email.filter(
    (col("idade") > 35) &
    (col("cidade") == "Natal") &
    (col("salario") < 7000)
)

df.printSchema()
df.show()

if df.rdd.isEmpty():
    print("Nenhum dado encontrado no arquivo JSON. Verifique o formato do arquivo")
else:
    # Limpa os dados removendo o @ se existir da coluna nome
    df_clean = df.withColumn("nome", regexp_replace(col("nome"), "@", ""))
    
    # Define o caminho para o banco de dados sqlite
    sqlite_db_path = os.path.abspath("data/usuarios.db")
    
    # Define o URL de conexão
    sqlite_uri = "jdbc:sqlite://" + sqlite_db_path
    
    # Define o driver JDBC
    properties = {"driver": "org.sqlite.JDBC"}
    
    # Valida se a tabela já existe, se já existir faz append se não faz overwrite
    try:
        # Tenta ler a tabela usuarios
        spark.read.jdbc(url=sqlite_uri, table="dsa_usuarios", properties=properties)
        write_mode = "append"
    except:
        write_mode = "overwrite"
    
    # Escreve os dados no banco
    df_clean.write.jdbc(url=sqlite_uri, table="dsa_usuarios", mode=write_mode, properties=properties)
    
    print(f"Dados gravados no banco de dados SQLite em usuarios.db usando o modo {write_mode}")