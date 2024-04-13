/* CREATES DATABASE */
CREATE DATABASE crimes_la
GO
USE crimes_la

/* CREATES TABLE */
CREATE TABLE crimes (
	DR_NO int,
	Date_Rptd varchar(50),
	DATE_OCC varchar(50),
	TIME_OCC varchar(4),
	AREA int,
	AREA_NAME varchar(30),
	Rpt_Dist_No int,
	Part_1_2 int,
	Crm_Cd int,
	Crm_Cd_Desc varchar(500),
	Mocodes varchar(250),
	Vict_Age int,
	Vict_Sex char,
	Vict_Descent char,
	Premis_Cd int,
	Premis_Desc varchar(100),
	Weapon_Used_Cd int,
	Weapon_Desc varchar(250),
	Status VARCHAR(2),
	Status_Desc varchar(250),
	Crm_Cd_1 int, 
	Crm_Cd_2 int,
	Crm_Cd_3 int,
	Crm_Cd_4 int,
	LOCATION varchar(50),
	Cross_Street varchar(50),
	LAT float,
	LON float
);

CREATE TABLE mo_codes (
	MO_CODE int,
	Description nvarchar(200)
);


/* INSERTS DATA */
BULK INSERT crimes
FROM '/src/Crime_Data_from_2020_to_Present.csv'
WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', FORMAT = 'CSV', FIRSTROW= 2);

BULK INSERT mo_codes
FROM '/src/MO_CODES-MO_CODES_Numerical_20191119.csv'
WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\r\n', FORMAT = 'CSV', FIRSTROW= 1);


/* VALIDATION */
SELECT TOP 10 * FROM crimes;
SELECT COUNT(1) total_rows FROM crimes;

/* CREATES SPARK LOGIN AND ALLOWS DATABASE READ */
USE [master]
GO
CREATE LOGIN [spark_login] WITH PASSWORD=N'senha12345_' , DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO

USE [crimes_la]
GO
CREATE USER [spark_login] FOR LOGIN [spark_login]
GO
USE [crimes_la]
GO
ALTER ROLE [db_datareader] ADD MEMBER [spark_login]
GO
