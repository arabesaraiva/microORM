FROM boxfuse/flyway
WORKDIR /src
COPY ["./InitializeDB/*.sql", "./"]

RUN sleep 30

ENTRYPOINT flyway migrate -user=$SA_USER -password=$SA_PASSWORD -url="jdbc:sqlserver://db:1433;databaseName=master" -locations="filesystem:." -mixed="true"