from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import when, col, regexp_replace, to_date, split

# Creates SparkSession
spark = SparkSession.builder.appName("ETL").getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

database_ip = "172.29.0.2"
database = "crimes_la"
user = "spark_login"
password = "senha12345_"

# creates dataframe from the table
df = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{database_ip}:1433;databaseName={database}") \
    .option("dbtable", "crimes") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("trustServerCertificate", "true") \
    .load()

df2 = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{database_ip}:1433;databaseName={database}") \
    .option("dbtable", "mo_codes") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("trustServerCertificate", "true") \
    .load()

# Cleans the wrong time data in the datatime columns of the original database and creates a new column with the date type
df = df.withColumn('Date_Rptd_clean', to_date(split(regexp_replace('Date_Rptd', '/', '-'), ' ').getItem(0), 'MM-dd-yyyy'))
df = df.withColumn('Date_OCC_clean', to_date(split(regexp_replace('Date_OCC', '/', '-'), ' ').getItem(0), 'MM-dd-yyyy'))

# creates a new dataframe with area data
df_area = df.select('Area', 'Area_Name').distinct()

df_area.show()

# creates a new dataframe with only the id and description of a crime
df_crimes = df.select('Crm_Cd', 'Crm_Cd_Desc').distinct()

df_crimes.show()

# creates a new dataframe with all the crime codes from an occurence
df_crimes_list = df.select('DR_NO', 'Crm_Cd', 'Crm_Cd_2', 'Crm_Cd_3', 'Crm_Cd_4')

df_crimes_list.show()

# creates a new dataframe with victims data
df_victims = df.select('DR_NO', 'Vict_Age', 'Vict_Sex', 'Vict_Descent')

# cleans data
df_victims = df_victims.fillna({'Vict_Sex': 'X', 'Vict_Descent': 'X'})
df_victims = df_victims.na.replace(['-', 'H'], 'X', 'Vict_Sex')
df_victims = df_victims.na.replace('-', 'X', 'Vict_Descent')

df_victims.show()

# creates a new dataframe and breaks the array that stores all other codes of crimes of the suspect
df_mocodes_crimes = df.select('DR_NO', split(col('Mocodes'), ' ').getItem(0).alias('Mocode1'),
                                       split(col('Mocodes'), ' ').getItem(1).alias('Mocode2'),
                                       split(col('Mocodes'), ' ').getItem(2).alias('Mocode3'),
                                       split(col('Mocodes'), ' ').getItem(3).alias('Mocode4'),
                                       split(col('Mocodes'), ' ').getItem(4).alias('Mocode5'),
                                       split(col('Mocodes'), ' ').getItem(5).alias('Mocode6'),
                                       split(col('Mocodes'), ' ').getItem(6).alias('Mocode7'),
                                       split(col('Mocodes'), ' ').getItem(7).alias('Mocode8'),
                                       split(col('Mocodes'), ' ').getItem(8).alias('Mocode9'),
                                       split(col('Mocodes'), ' ').getItem(9).alias('Mocode10'),
)

df_mocodes_crimes.show()

# creates a new dataframe with descent description
df_descent = df_victims.select('Vict_Descent').distinct()
df_descent = df_descent.withColumn('Descent_Desc', when(df_descent.Vict_Descent == "A", "Asian")
                                                   .when(df_descent.Vict_Descent == "B", "Black")
                                                   .when(df_descent.Vict_Descent == "C", "Chinese")
                                                   .when(df_descent.Vict_Descent == "D", "Cambodian")
                                                   .when(df_descent.Vict_Descent == "F", "Filipino")
                                                   .when(df_descent.Vict_Descent == "G", "Guamanian")
                                                   .when(df_descent.Vict_Descent == "H", "Hispanic/Latin/Mexican")
                                                   .when(df_descent.Vict_Descent == "I", "American Indian/Alaskan Native")
                                                   .when(df_descent.Vict_Descent == "J", "American Indian/Alaskan Native")
                                                   .when(df_descent.Vict_Descent == "K", "Korean")
                                                   .when(df_descent.Vict_Descent == "L", "Laotian")
                                                   .when(df_descent.Vict_Descent == "O", "Other")
                                                   .when(df_descent.Vict_Descent == "P", "Pacific Islander")
                                                   .when(df_descent.Vict_Descent == "S", "Samoan")
                                                   .when(df_descent.Vict_Descent == "U", "Hawaiian")
                                                   .when(df_descent.Vict_Descent == "V", "Vietnamese")
                                                   .when(df_descent.Vict_Descent == "W", "White")
                                                   .when(df_descent.Vict_Descent == "X", "Unknown")
                                                   .when(df_descent.Vict_Descent == "Z", "Asian Indian")
)

df_descent.show()

# creates a new dataframe with premis data
df_premis = df.select('Premis_Cd', 'Premis_Desc').distinct()

# cleans premis data
df_premis = df_premis.fillna({'Premis_Desc': 'Unknown'})

df_premis.show()

# creates a new dataframe with weapon data
df_weapons = df.select('Weapon_Used_Cd', 'Weapon_Desc').distinct()

# cleans weapon data
df_weapons = df_weapons.fillna({'Weapon_Used_Cd': 0, 'Weapon_Desc': 'No Weapon Used'})

df_weapons.show()

# creates a new dataframe with status data
df_status = df.select('Status', 'Status_Desc').distinct()

df_status.show()

# creates a new dataframe with location data
df_location = df.select('DR_NO', 'Area', 'Location', 'Cross_Street', 'Premis_Cd', 'LAT', 'LON')

df_location.show()

# Drop columns from original dataframe
df = df.drop('Date_Rptd', 'Date_OCC', 'Area', 'Area_Name', 'Crm_Cd', 'Crm_Cd_Desc', 'Mocodes', 'Vict_Age', 'Vict_Sex', 'Vict_Descent', 'Premis_Cd', 'Premis_Desc', 'Weapon_Desc', 'Status_Desc', 'Crm_Cd_1', 'Crm_Cd_2', 'Crm_Cd_3', 'Crm_Cd_4', 'Location', 'Cross_Street', 'LAT', 'LON')

df.show()
df2.show()

# write the files on hadoop
df.write.mode('overwrite').option("header", "true").csv('transformed_data/crimes_la')
df2.write.mode('overwrite').option("header", "true").csv('transformed_data/mocodes_desc')
df_area.write.mode('overwrite').option("header", "true").csv('transformed_data/areas')
df_crimes.write.mode('overwrite').option("header", "true").csv('transformed_data/crimes')
df_crimes_list.write.mode('overwrite').option("header", "true").csv('transformed_data/crimes_list')
df_victims.write.mode('overwrite').option("header", "true").csv('transformed_data/victmis')
df_mocodes_crimes.write.mode('overwrite').option("header", "true").csv('transformed_data/mocodes_crimes')
df_descent.write.mode('overwrite').option("header", "true").csv('transformed_data/descent')
df_premis.write.mode('overwrite').option("header", "true").csv('transformed_data/premis')
df_weapons.write.mode('overwrite').option("header", "true").csv('transformed_data/weapons')
df_status.write.mode('overwrite').option("header", "true").csv('transformed_data/status')
df_location.write.mode('overwrite').option("header", "true").csv('transformed_data/location')