
package de.ugoe.cs.smartshark.util;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * <p>
 * Interface for data loader for SmartSHARK from the underlying DB. All jobs should use this
 * interface, instead of directly accessing the database, even if the endpoint and credentials of
 * the DB are known.
 * </p>
 * 
 * @author Steffen Herbold
 */
public interface IDBUtils {

    /**
     * <p>
     * Loads all data from a collection into a dataframe.
     * </p>
     * 
     * @param collectionName
     *            name of the collection in the DB
     * @return the dataframe
     */
    public Dataset<Row> loadData(String collectionName);

    /**
     * <p>
     * Loads all data that matches the logical types into a data frame. The logical types are
     * matched using a DNF. This means, that there is a match, if for any of the inner lists, the
     * logical type description contains all elements.
     * </p>
     * 
     * @param collectionName
     *            name of the collection in the DB.
     * @param typeClauses
     *            List of list of strings for DNF. Outer list is the disjunction, inner list the
     *            conjunction.
     * @return the dataframe
     */
    public Dataset<Row> loadDataLogical(String collectionName, List<List<String>> typeClauses);
    
    /**
     * <p>
     * Writes the dataframe to the specified location in the database.
     * </p>
     *
     * @param dataset
     *            DataFrame to be stored in MongoDb
     * @param collectionName
     *            collection where the dataFrame is stored
     */
    public void writeData(Dataset<Row> dataset, String location);
}
