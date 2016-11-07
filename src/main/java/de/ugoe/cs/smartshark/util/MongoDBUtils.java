
package de.ugoe.cs.smartshark.util;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

/**
 * Database utilities for MongoDB usage.
 * 
 * @author Luqman Ul Khair, Steffen Herbold
 **/
public class MongoDBUtils implements IDBUtils {

    /**
     * spark session used for connection
     */
    private final SparkSession sparkSession;

    /**
     * name of the database
     */
    private final String dbname;

    /**
     * host of the database
     */
    private final String host;

    /**
     * port of the database
     */
    private final int port;

    /**
     * defines is user authentication is used
     */
    private final boolean useCredentials;

    /**
     * database username
     */
    private final String username;

    /**
     * authentication database
     */
    private final String authdb;

    /**
     * database password
     */
    private final char[] password;

    /**
     * name of the plugin schema collection
     */
    private final String pluginSchemaCollectionName;

    /**
     * <p>
     * Constructor. Fetches MongoDB connection information from spark context. The following
     * default-values are used:
     * <ul>
     * <li>dbname: smartshark</li>
     * <li>URI: localhost</li>
     * <li>port: 27017</li>
     * <li>use credentials: false</li>
     * <li>username: user</li>
     * <li>authentication database: admin</li>
     * <li>password: pwd</li>
     * <li>name of the plugin schema collection: plugin_schema</li>
     * </ul>
     * </p>
     *
     * @param sparkSession
     *            spark session used
     */
    public MongoDBUtils(SparkSession sparkSession) {
        this.sparkSession = sparkSession;

        // fetch mongo configuration from spark session
        dbname = sparkSession.conf().get(Constants.MONGO_DBNAME, "smartshark");
        host = sparkSession.conf().get(Constants.MONGO_URI, "localhost");
        port = Integer.parseInt(sparkSession.conf().get(Constants.MONGO_PORT, "27017"));
        useCredentials =
            Boolean.parseBoolean(sparkSession.conf().get(Constants.MONGO_USEAUTH, "false"));
        username = sparkSession.conf().get(Constants.MONGO_USERNAME, "user");
        authdb = sparkSession.conf().get(Constants.MONGO_AUTHDB, "admin");
        password = sparkSession.conf().get(Constants.MONGO_PASSWORD, "pwd").toCharArray();
        pluginSchemaCollectionName =
            sparkSession.conf().get(Constants.MONGO_PLUGINSCHEMA, "plugin_schema");
    }

    /**
     * <p>
     * Initializes the connection to the MongoDB.
     * </p>
     *
     * @return MongoClient for the connection
     */
    public MongoClient getMongoClient() {
        // setup server address
        ServerAddress serverAddress;
        try {
            serverAddress = new ServerAddress(host, port);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException("invalid MongoDB server address", e);
        }

        // create client
        MongoClient mongoClient;
        if (useCredentials) {
            MongoCredential credentials =
                MongoCredential.createCredential(username, authdb, password);
            mongoClient = new MongoClient(serverAddress, Arrays.asList(credentials));
        }
        else {
            mongoClient = new MongoClient(serverAddress);
        }
        return mongoClient;
    }

    @Override
    public Dataset<Row> loadData(String collectionName) {
        return loadDataLogical(collectionName, null);
    }

    @Override
    public Dataset<Row> loadDataLogical(String collectionName, List<String> types) {

        Dataset<Row> dataFrame = null;

        MongoClient mongoClient = getMongoClient();

        DB metricDB = mongoClient.getDB(dbname);
        DBCollection pluginSchemaCollection = metricDB.getCollection(pluginSchemaCollectionName);

        StructType pluginSchema;
        BasicDBObject query = new BasicDBObject();
        query.put("collections.collection_name", collectionName);
        if (types != null) {
            query.put("collections.fields.logical_type", new BasicDBObject("$in", types));
        }

        DBCursor pluginSchemaDocuments = pluginSchemaCollection.find(query);
        List<StructField> subSchema = new ArrayList<StructField>();

        for (DBObject pluginSchemaDocument : pluginSchemaDocuments) {
            BasicDBList collectionsList = (BasicDBList) pluginSchemaDocument.get("collections");
            BasicDBObject[] collections = collectionsList.toArray(new BasicDBObject[0]);
            for (BasicDBObject collection : collections) {

                String collection_name = (String) collection.getString("collection_name");
                if (collection_name.equalsIgnoreCase(collectionName)) {
                    BasicDBList fieldsList = (BasicDBList) collection.get("fields");
                    BasicDBObject[] fields = fieldsList.toArray(new BasicDBObject[0]);
                    // List<Row> fields = collection.getList(1);
                    subSchema.addAll(parseSchema(fields, types));

                }
            }

        }

        mongoClient.close();

        pluginSchema =
            DataTypes.createStructType(subSchema.toArray(new StructField[subSchema.size()]));
        // pluginSchema.printTreeString();

        Map<String, String> options = new HashMap<String, String>();
        options.put("host", host + ":" + port);
        options.put("database", dbname);
        options.put("collection", collectionName);
        options.put("credentials", username + "," + authdb + "," + String.valueOf(password));

        dataFrame = sparkSession.read().schema(pluginSchema)
            .format("com.stratio.datasource.mongodb").options(options).load();

        // dataFrame.show();

        return dataFrame;
    }

    private static ArrayList<StructField> parseSchema(BasicDBObject[] fields, List<String> types) {

        ArrayList<StructField> structFields = new ArrayList<StructField>();

        for (int i = 0; i < fields.length; i++) {

            BasicDBObject field = fields[i];
            String logical_type = (String) field.get("logical_type");

            if ((types == null) || (types.contains(logical_type))) {

                String type = (String) field.get("type");

                //// more types can be added here
                switch (type)
                {
                    case "StringType": {
                        structFields
                            .add(DataTypes.createStructField((String) field.get("field_name"),
                                                             DataTypes.StringType, true));
                        break;
                    }
                    case "IntegerType": {
                        structFields
                            .add(DataTypes.createStructField((String) field.get("field_name"),
                                                             DataTypes.IntegerType, true));
                        break;
                    }
                    case "DoubleType": {
                        structFields
                            .add(DataTypes.createStructField((String) field.get("field_name"),
                                                             DataTypes.DoubleType, true));
                        break;
                    }
                    case "BooleanType": {
                        structFields
                            .add(DataTypes.createStructField((String) field.get("field_name"),
                                                             DataTypes.BooleanType, true));
                        break;
                    }
                    case "DateType": {
                        structFields
                            .add(DataTypes.createStructField((String) field.get("field_name"),
                                                             DataTypes.DateType, true));
                        break;
                    }
                    case "StructType": {

                        BasicDBList subFieldsList = (BasicDBList) field.get("fields");
                        BasicDBObject[] subFields = subFieldsList.toArray(new BasicDBObject[0]);
                        ArrayList<StructField> subStructFields = parseSchema(subFields, types);
                        structFields.add(DataTypes
                            .createStructField((String) field.get("field_name"),
                                               DataTypes.createStructType(subStructFields), true));
                        break;
                    }
                    case "ArrayType": {
                        String sub_type = (String) field.get("sub_type");
                        switch (sub_type)
                        {
                            case "StringType": {
                                structFields.add(DataTypes
                                    .createStructField((String) field.get("field_name"), DataTypes
                                        .createArrayType(DataTypes.StringType, true), true));
                                break;
                            }
                            case "IntegerType": {
                                structFields.add(DataTypes
                                    .createStructField((String) field.get("field_name"), DataTypes
                                        .createArrayType(DataTypes.IntegerType, true), true));
                                break;
                            }
                            case "DoubleType": {
                                structFields.add(DataTypes
                                    .createStructField((String) field.get("field_name"), DataTypes
                                        .createArrayType(DataTypes.DoubleType, true), true));
                                break;
                            }
                            case "BooleanType": {
                                structFields.add(DataTypes
                                    .createStructField((String) field.get("field_name"), DataTypes
                                        .createArrayType(DataTypes.BooleanType, true), true));
                                break;
                            }
                            case "DateType": {
                                structFields.add(DataTypes
                                    .createStructField((String) field.get("field_name"), DataTypes
                                        .createArrayType(DataTypes.DateType, true), true));
                                break;
                            }

                        }

                        break;
                    }

                }

            }

        }

        return structFields;
    }

}
