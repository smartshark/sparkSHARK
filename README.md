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
import pyspark
from py4j.java_gateway import java_import
from pyspark.mllib.common import _java2py

# create a SparkContext and SparkConf for the MongoDB configuration used by the DBUtils
conf = pyspark.SparkConf()
conf.set('spark.executorEnv.mongo.uri', '127.0.0.1')
conf.set('spark.executorEnv.mongo.username', 'user')
conf.set('spark.executorEnv.mongo.password', 'pass')
conf.set('spark.executorEnv.mongo.useauth', 'true')
conf.set('spark.executorEnv.mongo.dbname', 'smartshark')
sc = pyspark.SparkContext(master='local[*]', appName='Test', conf=conf)

# import the DBUtilFactory and SparkSession
java_import(sc._gateway.jvm, 'de.ugoe.cs.smartshark.util.DBUtilFactory')
java_import(sc._gateway.jvm, 'org.apache.spark.sql.SparkSession')

# create a Java SparkSession
ss = sc._gateway.jvm.SparkSession.builder().appName('Test').getOrCreate()

# pass the Java SparkSession to the DBUtilFactory to get the DBUtils Object
dbUtils = sc._gateway.jvm.DBUtilFactory.getDBUtils(ss)

# load data and convert to python, this yields a pyspark DataFrame
cdf = _java2py(sc, dbUtils.loadData('commit'))

cdf.show()
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

# please see the full python example above for the necessary imports and session setup
dfc = _java2py(sc, t.loadDataLogical('code_entity_state', [['AbstractionLevel'], ['ProductMetric', 'Java']]))
```

The DBUtils can also be used with R using the [rSHARK](https://github.com/smartshark/rSHARK). 

AnalysisUtils
-------------
Utility functions for tasks that are recuring for different analysis. For now, please consult the Javadoc for further information. 

DataFrameUtils
--------------
Utility functions for manipulating dataframes. For now, please consult the Javadoc for further information. 
