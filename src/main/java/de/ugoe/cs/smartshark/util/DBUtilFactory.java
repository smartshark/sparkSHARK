
package de.ugoe.cs.smartshark.util;

import org.apache.spark.sql.SparkSession;

/**
 * <p>
 * Factory class for DBUtils.
 * </p>
 * 
 * @author Steffen Herbold
 */
public class DBUtilFactory {

    /**
     * <p>
     * Returns an instance of the DBUtils for the selected database type. The database type is
     * selected indirectly via the environment variable defined by {@link Constants#DBUTILS_TYPE}.
     * </p>
     *
     * @param sparkSession the spark session
     * @return the DBUtils
     */
    public static IDBUtils getDBUtils(SparkSession sparkSession) {
        String dbUtilsType = sparkSession.conf().get(Constants.DBUTILS_TYPE, "mongo");

        if (dbUtilsType.equals("mongo")) {
            return new MongoDBUtils(sparkSession);
        }
        else if (dbUtilsType.equals("dummy")) {
            return new DummyDBUtils(sparkSession);
        }
        else {
            throw new RuntimeException("Unknown type of database selected. Please check the value of the " +
                Constants.DBUTILS_TYPE + " parameter.");
        }
    }

}
