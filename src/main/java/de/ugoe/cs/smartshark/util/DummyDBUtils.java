
package de.ugoe.cs.smartshark.util;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DummyDBUtils implements IDBUtils {

    final SparkSession sparkSession;

    public DummyDBUtils(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public Dataset<Row> loadData(String collectionName) {
        return sparkSession.read().json("src/main/resources/" + collectionName + ".json");
    }

    @Override
    public Dataset<Row> loadDataLogical(String collectionName, List<List<String>> types) {
        // TODO Auto-generated method stub
        return null;
    }

}
