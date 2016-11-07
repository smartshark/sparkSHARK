
package de.ugoe.cs.smartshark.util;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * <p>
 * Utility class with convenience functions for working with dataframes.
 * </p>
 * 
 * @author Steffen Herbold
 */
public class DataFrameUtils {

    /**
     * <p>
     * Converts children of a struct field into columns of the data frame
     * <li>
     *
     * @param dataframe
     *            the dataframe
     * @param structField
     *            name of the structField column
     * @return dataframe with exploded columns
     */
    public static Dataset<Row> explodeStructToCols(Dataset<Row> dataframe, String structFieldCol) {
        StructType schema = dataframe.schema();
        StructType metricsField =
            (StructType) schema.fields()[schema.fieldIndex(structFieldCol)].dataType();
        String[] metricNames = metricsField.fieldNames();
        for (String metricName : metricNames) {
            dataframe =
                dataframe.withColumn(metricName, dataframe.col(structFieldCol + "." + metricName));
        }
        return dataframe;
    }

    /**
     * <p>
     * Internally uses the {@link VectorAssembler} to create a feature vector using all field of the
     * structFieldCol.
     * </p>
     *
     * @param dataframe
     *            the dataframe
     * @param structFieldCol
     *            name of the structField column
     * @param featureCol
     *            name of the newly generated feature column
     * @return dataframe with feature column
     */
    public static Dataset<Row> structToFeatures(Dataset<Row> dataframe,
                                                String structFieldCol,
                                                String featureCol)
    {
        StructType schema = dataframe.schema();
        StructType metricsField =
            (StructType) schema.fields()[schema.fieldIndex(structFieldCol)].dataType();
        String[] metricNames = metricsField.fieldNames();
        /*
         * for( int i=0; i<metricNames.length; i++ ) { metricNames[i] = "metrics." + metricNames[i];
         * }
         */
        // required because of https://issues.apache.org/jira/browse/SPARK-18301
        dataframe = explodeStructToCols(dataframe, structFieldCol);
        VectorAssembler va =
            new VectorAssembler().setInputCols(metricNames).setOutputCol(featureCol);
        dataframe = va.transform(dataframe);
        for (String metricName : metricNames) {
            dataframe = dataframe.drop(metricName);
        }
        return dataframe;
    }
}
