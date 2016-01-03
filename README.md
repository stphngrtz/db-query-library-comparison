# db-query-library-comparison

* sql2o (http://www.sql2o.org)
* QueryDSL (http://www.querydsl.com)
* JOOQ (http://www.jooq.org)

```
cd db/
docker build -t db-query-library-comparison-db .
docker run --name db -e POSTGRES_USER=stephan -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d db-query-library-comparison-db

cd ..
mvn clean test
```
