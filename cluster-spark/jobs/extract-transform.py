from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, regexp_replace, to_date, split, lit, substring, concat, monotonically_increasing_id

# Creates SparkSession
spark = SparkSession.builder.appName("ETL").getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

database_ip = "172.21.0.2"
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

# Cleans the wrong time data in the datatime columns of the original database and creates a new dataframe
df_date = df.select('DR_NO', 'Date_Rptd', 'Date_Occ', 'Time_OCC')

df_date = df_date.withColumn('Date_Rptd', to_date(split(regexp_replace('Date_Rptd', '/', '-'), ' ').getItem(0), 'MM-dd-yyyy')) \
    .withColumn('Date_OCC', to_date(split(regexp_replace('Date_OCC', '/', '-'), ' ').getItem(0), 'MM-dd-yyyy')) \
    .withColumn('Time_OCC_hour', substring(col('Time_OCC'), 1, 2)) \
    .withColumn('Time_OCC_minute', substring(col('Time_OCC'), 3, 4)) \
    .withColumn('DateTime_OCC', concat('Date_OCC', lit(' '), concat(col('Time_OCC_hour'), lit(":"), col('Time_OCC_minute'), lit(':00'))).cast("timestamp"))

df_date = df_date.drop('Date_OCC', 'Time_OCC', 'Time_OCC_hour', 'Time_OCC_minute')

# creates a new dataframe with area data
df_area = df.select('Area', 'Area_Name').distinct()

# creates a new dataframe with only the id and description of a crime
df_crimes = df.select('Crm_Cd', 'Crm_Cd_Desc').distinct()

# creates a new dataframe with all the crime codes from an occurence
df_crimes_list = df.select('DR_NO', 'Crm_Cd', 'Crm_Cd_2', 'Crm_Cd_3', 'Crm_Cd_4')

# creates a new dataframe with victims data
df_victims = df.select('DR_NO', 'Vict_Age', 'Vict_Sex', 'Vict_Descent')

# cleans data
df_victims = df_victims.fillna({'Vict_Sex': 'X', 'Vict_Descent': 'X'})
df_victims = df_victims.na.replace(['-', 'H'], 'X', 'Vict_Sex')
df_victims = df_victims.na.replace('-', 'X', 'Vict_Descent')

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

#cleans premis data
df = df.fillna({'Premis_Cd': 0, 'Premis_Desc': 'Unknown'})
df = df.withColumn('Premis_Cd' , when(df.Premis_Desc == 'Unknown', 0)
                                               .otherwise(df.Premis_Cd))


# creates a new dataframe with premis data
df_premis = df.select('Premis_Cd', 'Premis_Desc').distinct()

# creates a new dataframe with weapon data
df_weapons = df.select('Weapon_Used_Cd', 'Weapon_Desc').distinct()

# cleans weapon data
df_weapons = df_weapons.fillna({'Weapon_Used_Cd': 500, 'Weapon_Desc': 'UNKNOWN WEAPON/OTHER WEAPON'})
df = df.fillna({'Weapon_Used_Cd': 500})

# creates a new dataframe with status data
df_status = df.select('Status', 'Status_Desc').distinct()

# creates a new dataframe for all distinct locations giving it an ID and its description
df_location_desc = df.select('Location').distinct()

# creates a incremental id for each of the locations
df_location_desc = df_location_desc.withColumn('id', monotonically_increasing_id() + 1)

# Changes the order of the columns
df_location_desc = df_location_desc.select('id', 'Location')

df.createOrReplaceTempView("location_temp")
df_location_desc.createOrReplaceTempView("location_desc_temp")

# creates a new dataframe with a select from the temporary view. We also clean the data of LAT and LON that were 0 replacing for the avg of the values of LAT and LON of the same location.
# If the LAT and LON are still 0 after the cleaning it means there is not enough data in the dataset to guess their values
df_location = spark.sql("""
                        SELECT *
                        FROM (
                        SELECT DR_NO, Area, ldt.id Location_Id, Cross_Street, Premis_Cd, CASE WHEN LAT = 0 THEN COALESCE(AVG_LAT, 0) ELSE LAT END LAT, CASE WHEN LON = 0 THEN COALESCE(AVG_LON, 0) ELSE LON END LON
                        FROM location_temp lt
                        LEFT JOIN (SELECT location, round(avg(lat), 4) avg_lat, round(avg(lon), 4) avg_lon FROM location_temp WHERE lat <> 0 GROUP BY location) sq
                        ON lt.location = sq.location
                        INNER JOIN location_desc_temp ldt
                        ON lt.location = ldt.location
                        )
                    """)

# changes the number of partitions of the dataframe to 1
df_location = df_location.coalesce(1)


# Drop columns from original dataframe
df = df.drop('Date_Rptd', 'Rpt_Dist_No', 'Part_1_2', 'Date_OCC', 'Time_OCC', 'Area', 'Area_Name', 'Crm_Cd', 'Crm_Cd_Desc', 'Mocodes', 'Vict_Age', 'Vict_Sex', 'Vict_Descent',
             'Premis_Cd', 'Premis_Desc', 'Weapon_Desc', 'Status_Desc', 'Crm_Cd_1', 'Crm_Cd_2', 'Crm_Cd_3', 'Crm_Cd_4', 'Location', 'Cross_Street', 'LAT', 'LON')

# write the files on hadoop
df.write.mode('overwrite').option("header", "true").csv('transformed_data/crimes_la')
df2.write.mode('overwrite').option("header", "true").csv('transformed_data/mocodes_desc')
df_date.write.mode('overwrite').option("header", "true").csv('transformed_data/crimes_date')
df_area.write.mode('overwrite').option("header", "true").csv('transformed_data/areas')
df_crimes.write.mode('overwrite').option("header", "true").csv('transformed_data/crimes')
df_crimes_list.write.mode('overwrite').option("header", "true").csv('transformed_data/crimes_list')
df_victims.write.mode('overwrite').option("header", "true").csv('transformed_data/victims')
df_mocodes_crimes.write.mode('overwrite').option("header", "true").csv('transformed_data/mocodes_crimes')
df_descent.write.mode('overwrite').option("header", "true").csv('transformed_data/descent')
df_premis.write.mode('overwrite').option("header", "true").csv('transformed_data/premis')
df_weapons.write.mode('overwrite').option("header", "true").csv('transformed_data/weapons')
df_status.write.mode('overwrite').option("header", "true").csv('transformed_data/status')
df_location.write.mode('overwrite').option("header", "true").csv('transformed_data/location')
df_location_desc.write.mode('overwrite').option("header", "true").csv('transformed_data/location_desc')