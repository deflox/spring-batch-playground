```
docker run --name billing-db -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_USER=postgres -e POSTGRES_DB=billing -d postgres:16-alpine

java -jar target/spring-batch-demo-0.0.1-SNAPSHOT.jar input.file=work-dirs/input/billing-2023-01.csv output.file=work-dirs/output/billing-report-2023-01.csv data.year=2023 data.month=1

mvn clean test -Dspring.batch.job.enabled=false

-Dmaven.test.skip=true
```

Spring Batch Schema files are stored in the JAR-Dependency.

```

```

Demos:
* Restartibility
* Skips
* Retries
* Testing