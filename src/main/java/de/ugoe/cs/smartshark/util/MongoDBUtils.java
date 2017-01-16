
package de.ugoe.cs.smartshark.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

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
        serverAddress = new ServerAddress(host, port);

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
    public Dataset<Row> loadDataLogical(String collectionName, List<List<String>> typeClauses) {

        Dataset<Row> dataFrame = null;

        MongoClient mongoClient = getMongoClient();

        // DB metricDB = mongoClient.getDB(dbname);
        MongoDatabase metricDB = mongoClient.getDatabase(dbname);
        MongoCollection<Document> pluginSchemaCollection =
            metricDB.getCollection(pluginSchemaCollectionName);

        StructType pluginSchema = null;
        BasicDBObject query = new BasicDBObject();
        query.put("collections.collection_name", collectionName);

        FindIterable<Document> pluginSchemaDocuments = pluginSchemaCollection.find(query);

        for (Document pluginSchemaDocument : pluginSchemaDocuments) {
            @SuppressWarnings("unchecked")
            ArrayList<Document> collectionsList =
                (ArrayList<Document>) pluginSchemaDocument.get("collections");
            // BasicDBList collectionsList = (BasicDBList) pluginSchemaDocument.get("collections");
            Document[] collections = collectionsList.toArray(new Document[0]);
            for (Document collection : collections) {

                String collection_name = (String) collection.getString("collection_name");
                if (collection_name.equalsIgnoreCase(collectionName)) {
                    @SuppressWarnings("unchecked")
                    ArrayList<Document> fieldsList = (ArrayList<Document>) collection.get("fields");
                    Document[] fields = fieldsList.toArray(new Document[0]);
                    // List<Row> fields = collection.getList(1);
                    
                    List<StructField> subSchema = parseSchema(fields, typeClauses);
                    if( pluginSchema==null ) {
                        pluginSchema = DataTypes.createStructType(subSchema.toArray(new StructField[subSchema.size()]));
                    } else {
                        StructType toMerge = DataTypes.createStructType(subSchema.toArray(new StructField[subSchema.size()]));
                        pluginSchema = pluginSchema.merge(toMerge);
                    }
                    pluginSchema.printTreeString();
                }
            }

        }

        mongoClient.close();

        Map<String, String> options = new HashMap<>();
        options.put("spark.mongodb.input.uri", getConnectionUri(collectionName));

        dataFrame = sparkSession.read().schema(pluginSchema).format("com.mongodb.spark.sql")
            .options(options).load();

        return dataFrame;
    }

    private List<StructField> parseSchema(Document[] fields, List<List<String>> typeClauses) {

        ArrayList<StructField> structFields = new ArrayList<StructField>();

        for (int i = 0; i < fields.length; i++) {

            Document field = fields[i];
            Object logical_type = field.get("logical_type");

            if (checkLogicalType(typeClauses, logical_type)) {

                String type = (String) field.get("type");

                //// more types can be added here
                switch (type)
                {
                    case "ObjectIdType": {
                        ArrayList<StructField> subStructFields = new ArrayList<StructField>();
                        subStructFields
                            .add(DataTypes.createStructField("oid", DataTypes.StringType, true));
                        structFields.add(DataTypes
                            .createStructField((String) field.get("field_name"),
                                               DataTypes.createStructType(subStructFields), true));
                        break;
                    }
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
                    case "TimestampType": {
                        structFields
                            .add(DataTypes.createStructField((String) field.get("field_name"),
                                                             DataTypes.TimestampType, true));
                        break;
                    }
                    case "StructType": {

                        @SuppressWarnings("unchecked")
                        ArrayList<Document> subFieldsList =
                            (ArrayList<Document>) field.get("fields");
                        Document[] subFields = subFieldsList.toArray(new Document[0]);
                        List<StructField> subStructFields =
                            parseSchema(subFields, typeClauses);
                        if (subStructFields.size() > 0) {
                            // add only if there are any subfields
                            // empty is possible due to logical filtering
                            structFields
                                .add(DataTypes.createStructField((String) field.get("field_name"),
                                                                 DataTypes
                                                                     .createStructType(subStructFields),
                                                                 true));
                        }
                        break;
                    }
                    case "ArrayType": {
                        String sub_type = (String) field.get("sub_type");
                        switch (sub_type)
                        {
                            case "ObjectIdType": {
                                ArrayList<StructField> subStructFields =
                                    new ArrayList<StructField>();
                                subStructFields.add(DataTypes
                                    .createStructField("oid", DataTypes.StringType, true));
                                structFields.add(DataTypes
                                    .createStructField((String) field.get("field_name"),
                                                       DataTypes.createArrayType(DataTypes
                                                           .createStructType(subStructFields)),
                                                       true));
                                break;
                            }
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
                            case "TimestampType": {
                                structFields.add(DataTypes
                                    .createStructField((String) field.get("field_name"), DataTypes
                                        .createArrayType(DataTypes.TimestampType, true), true));
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

    /**
     * <p>
     * Checks if the logical type matches the defined clauses. The clauses define a DNF.
     * </p>
     *
     * @param typeClauses
     *            List of list of strings for DNF. Outer list is the disjunction, inner list the
     *            conjunction.
     * @param logicalType
     *            logical type object from the database. Can be both string or array type.
     * @return true if a match, i.e., for any of the inner lists all types are contained in the
     *         logical type object.
     */
    @SuppressWarnings("unchecked")
    private boolean checkLogicalType(List<List<String>> typeClauses, Object logicalType) {
        if (typeClauses == null || typeClauses.isEmpty()) {
            return true; // nothing to check, take everything
        }
        if ("nested".equals(logicalType) || "Nested".equals(logicalType)) {
            return true; // nothing to check for nested types
        }
        for (List<String> typeClause : typeClauses) {
            if (logicalType instanceof String) {
                if (typeClause.size() == 1) {
                    if (logicalType.equals(typeClause.get(0))) {
                        return true; // match to this clause
                    }
                }
            }
            if (logicalType instanceof ArrayList) {
                int numMatches = 0;
                for (String type : typeClause) {
                    if (((ArrayList<Object>) logicalType).contains(type)) {
                        // System.out.println(type);
                        numMatches++;
                    }
                }
                if (numMatches == typeClause.size()) {
                    return true; // match to this clause
                }
            }
        }
        return false; // no match found, return false
    }

    /**
     * <p>
     * Creates the DB connection URI.
     * </p>
     *
     * @param collectionName
     *            collection name for which the URI is created.
     * @return URI for DB connection
     */
    private String getConnectionUri(String collectionName) {
        String uri;
        if (useCredentials) {
            uri = "mongodb://" + username + ":" + String.valueOf(password) + "@" + host + ":" +
                port + "/" + dbname + "." + collectionName + "?authSource=" + authdb;
        }
        else {
            uri = "mongodb://" + host + "/" + dbname + "." + collectionName;
        }
        return uri;
    }

    /**
     * <p>
     * Writes the dataFrame to MongoDb.
     * </p>
     *
     * @param dataset
     *            DataFrame to be stored in MongoDb
     * @param collectionName
     *            collection where the dataFrame is stored
     */
    @Override
    public void writeData(Dataset<Row> dataset, String collectionName) {
        Map<String, String> options = new HashMap<>();
        options.put("spark.mongodb.output.uri", getConnectionUri(collectionName));
        dataset.write().format("com.mongodb.spark.sql").options(options).mode(SaveMode.Append)
            .save();
    }

}
