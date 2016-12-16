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

The following code samples show how to use the DB utils:
```Java
// Java
IDBUtils dbUtils = DBUtilFactory.getDBUtils(sparkSession);
Dataset<Row> commits = dbUtils.loadData("commits");
```

```scala
// scala
TODO
```

```python
# python
TODO
```

The DBUtils also support more complex loading commands that allow a logical selection of fields from a collection. To this aim, 
a Disjunctive Normal Form (DNF) is defined using lists. The concrete logical types can be found in the description of the database schema of the SmartSHARK instance you are using.
```Java
// Java
/* Load all fields from entity_state that are:
 * - a reference
 * - an abstraction levels, or
 * - a product metric for java classes.
 */
Dataset<Row> entityState = dbUtils.loadDataLogical("entity_state",
    Arrays.asList(Arrays.asList("RID"), 
    Arrays.asList("AbstractionLevel"),
    Arrays.asList("ProductMetric", "JavaClass")))
```

```scala
// scala
TODO
```

```python
# python
TODO
```

The DBUtils can also be used with R using the [rSHARK](https://github.com/smartshark/rSHARK). 

AnalysisUtils
-------------
Utility functions for tasks that are recuring for different analysis. For now, please consult the Javadoc for further information. 

DataFrameUtils
--------------
Utility functions for manipulating dataframes. For now, please consult the Javadoc for further information. 
