sparkSHARK
==========

The sparkSHARK library provides some convenience functions for working with the SmartSHARK data and Apache Spark.

Prerequisites
=============
- Java 8
- Maven

Features
========

DBUtils
-------
The DBUtils interface abstracts from a concrete representation of the SmartSHARK database. This allows the definition of Spark jobs without a direct dependency on the database. The DBUtils use JVM arguments to pass parameters to Apache Spark that describe the type and location of the database. 
The parameters are:

| Parameter                      | Meaning                                                               | Default    |
|--------------------------------|-----------------------------------------------------------------------|------------|
|spark.executorEnv.dbtuils.type | Type of database backend. Currently only supports "mongo" for MongoDB.| mongo      |
|spark.executorEnv.mongo.uri     | URI of the MongoDB                                                    | localhost  |
|spark.executorEnv.mongo.port    | Port of the MongoDB                                                   | 27017      |
|spark.executorEnv.mongo.dbname  | Name of the database within the MongoDB                               | smartshark |
|spark.executorEnv.mongo.useauth | `true` if the MongoDB requires authentication.                        | false      |
|spark.executorEnv.mongo.username| Username for the MongoDB. Ignored if useauth is false.                | user       |
|spark.executorEnv.mongo.authdb  | Database used for authenticaion                                       | admin      |
|spark.executorEnv.mongo.password| Password for the user.                                                | pwd        | 

To pass one of these parameters to the Spark configuration via the JVM, you need to prefix it with -D, e.g., `-DsparkExecutorEnv.mongo.uri=localhost`. 

Currently, there are only MongoDBUtils, however, in case a different backend is used, it could be easily replaced without touching analysis jobs using the DBUtils. 

To initialize the DBUtils, you can use the following code:
```Java
// Java
IDBUtils dbUtils = DBUtilFactory.getDBUtils(sparkSession);
```

```scala
// scala
var dbUtils = DBUtilFactory.getDBUtils(sparkSession);
```

```python
# python
TODO
```

For using the DBUtils with R, we provide support with the [rSHARK](https://github.com/smartshark/rSHARK). 
