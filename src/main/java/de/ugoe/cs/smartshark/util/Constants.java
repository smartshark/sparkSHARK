
package de.ugoe.cs.smartshark.util;

/**
 * <p>
 * Helper class with constants. 
 * </p>
 * 
 * @author Steffen Herbold
 */
public class Constants {

    /**
     * Environment variable for the MongoDB URI.
     */
    public static final String MONGO_URI = "spark.executorEnv.mongo.uri";
    
    /**
     * Environment variable for the MongoDB port.
     */
    public static final String MONGO_PORT = "spark.executorEnv.mongo.port";
    
    /**
     * Environment variable for the MongoDB database name.
     */
    public static final String MONGO_DBNAME = "spark.executorEnv.mongo.dbname";
    
    /**
     * Environment variable for the MongoDB user authentication mode (true/false).
     */
    public static final String MONGO_USEAUTH = "spark.executorEnv.mongo.useauth";
    
    /**
     * Environment variable for the MongoDB username.
     */
    public static final String MONGO_USERNAME = "spark.executorEnv.mongo.username";
    
    /**
     * Environment variable for the MongoDB authentication database.
     */
    public static final String MONGO_AUTHDB = "spark.executorEnv.mongo.authdb";
    
    /**
     * Environment variable for the MongoDB password.
     */
    public static final String MONGO_PASSWORD = "spark.executorEnv.mongo.password";
    
    /**
     * Environment variable for the MongoDB plugin schema collection.
     */
    public static final String MONGO_PLUGINSCHEMA = "spark.executorEnv.mongo.pluginschema";
    
    /**
     * Environment variable for the type of DB utils to be used.
     */
    public static final String DBUTILS_TYPE = "spark.exectutorEnv.dbtuils.type";
}
