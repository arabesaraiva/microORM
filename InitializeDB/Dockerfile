FROM boxfuse/flyway
WORKDIR /src
COPY ["./InitializeDB/*.sql", "./"]

ENTRYPOINT flyway migrate -user=$SA_USER -password=$SA_PASSWORD -url="jdbc:sqlserver://db:1433;databaseName=master" -locations="filesystem:." -mixed="true" -connectRetries=30