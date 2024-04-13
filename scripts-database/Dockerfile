FROM mcr.microsoft.com/mssql/server:2022-latest

ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=senha12345_
ENV MSSQL_PID=Developer
ENV MSSQL_TCP_PORT=1433

WORKDIR /src

COPY database.sql .
COPY ./Crime_Data_from_2020_to_Present.csv .
COPY ./MO_CODES-MO_CODES_Numerical_20191119.csv .

RUN /opt/mssql/bin/sqlservr & sleep 10 && /opt/mssql-tools/bin/sqlcmd -U sa -P 'senha12345_' -i database.sql

