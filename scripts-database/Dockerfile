FROM mcr.microsoft.com/mssql/server:2022-latest

ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=senha12345_
ENV MSSQL_PID=Developer
ENV MSSQL_TCP_PORT=1433

WORKDIR /src

COPY database.sql .
COPY ./crimes_la_1.bak .
COPY ./crimes_la_2.bak .
COPY ./crimes_la_3.bak .
COPY ./crimes_la_4.bak .
COPY ./crimes_la_5.bak .
COPY ./crimes_la_6.bak .
COPY ./crimes_la_7.bak .
COPY ./crimes_la_8.bak .

RUN /opt/mssql/bin/sqlservr & sleep 10 && /opt/mssql-tools/bin/sqlcmd -U sa -P 'senha12345_' -i database.sql

