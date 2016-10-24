package de.ugoe.cs.smartshark.util;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * <p>
 * Interface for data loader for SmartSHARK from the underlying DB. All jobs should use this interface, instead
 * of directly accessing the database, even if the endpoint and credentials of the DB are known. 
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
	 * @param collectionName name of the collection in the DB
	 * @return the dataframe 
	 */
	public Dataset<Row> loadData(String collectionName);
	
	/**
	 * <p>
	 * Loads all data that matches the logical types into a data frame.
	 * </p>
	 * 
	 * @param collectionName name of the collection in the DB.
	 * @param types list of logical types. 
	 * @return the dataframe
	 */
	public Dataset<Row> loadDataLogical(String collectionName, List<String> types);
}
