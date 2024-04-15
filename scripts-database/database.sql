/* CREATES SPARK LOGIN AND ALLOWS DATABASE READ */
USE [master]
GO
CREATE LOGIN [spark_login] WITH PASSWORD=N'senha12345_' , DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF

/* RESTORES DATABASE */
RESTORE DATABASE [crimes_la] FROM  
DISK = N'/src/crimes_la_1.bak',
DISK = N'/src/crimes_la_2.bak',
DISK = N'/src/crimes_la_3.bak',
DISK = N'/src/crimes_la_4.bak',
DISK = N'/src/crimes_la_5.bak',
DISK = N'/src/crimes_la_6.bak',
DISK = N'/src/crimes_la_7.bak',
DISK = N'/src/crimes_la_8.bak'
WITH  FILE = 1,  MOVE N'crimes_la' TO N'/var/opt/mssql/data/crimes_la.mdf',
MOVE N'crimes_la_log' TO N'/var/opt/mssql/data/crimes_la_log.ldf',  NOUNLOAD,  STATS = 5

GO
