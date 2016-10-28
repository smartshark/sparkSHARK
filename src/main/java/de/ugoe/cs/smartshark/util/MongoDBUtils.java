
package de.ugoe.cs.smartshark.util;

import java.net.UnknownHostException;
import java.util.ArrayList;
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
import com.mongodb.MongoClientURI;

/**
 * @author Luqman Ul Khair
 * 
 **/
public class MongoDBUtils implements IDBUtils {

    @Override
    public Dataset<Row> loadData(String collectionName) {
        return loadDataLogical(collectionName, null);
    }

    @Override
    public Dataset<Row> loadDataLogical(String collectionName, List<String> types) {

        Dataset<Row> dataFrame = null;
        MongoClient mongoClient = null;

        try {

            mongoClient = new MongoClient(new MongoClientURI("mongodb://" + Constants.DBURI));

            DB metricDB = mongoClient.getDB(Constants.DBNAME);
            DBCollection pluginSchemaCollection = metricDB.getCollection(Constants.PLUGINSCHEMA);

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
            options.put("host", Constants.DBURI);
            options.put("database", Constants.DBNAME);
            options.put("collection", collectionName);

            SparkSession spark = SparkSession.builder().master(Constants.SPARKURI).getOrCreate();
            dataFrame = spark.read().schema(pluginSchema).format("com.stratio.datasource.mongodb")
                .options(options).load();

            // dataFrame.show();

        }
        catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

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
