
package de.ugoe.cs.smartshark.util;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 * Utility functions for analysis jobs.
 * </p>
 * 
 * @author Steffen Herbold
 */
public class AnalysisUtils {

    /**
     * Session used for the AnalysisUtils.
     */
    SparkSession sparkSession;

    /**
     * <p>
     * Creates a AnalysisUtils for a given spark session.
     * </p>
     *
     * @param sparkSession
     *            the spark session
     */
    public AnalysisUtils(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * <p>
     * Determines the Id of a project using its Url.
     * </p>
     *
     * @param projectUrl
     *            URL of the project
     * @return project Id
     */
    public String resolveProjectUrl(String projectUrl) {
        Dataset<Row> projects = DBUtilFactory.getDBUtils(sparkSession).loadData("project")
            .filter(col("url").like(projectUrl));
        long count = projects.count();
        if (count == 0) {
            throw new RuntimeException("Project not found");
        }
        else if (count > 1) {
            throw new RuntimeException("More than one project found!");
        }
        Row row = projects.collectAsList().get(0);
        String projectId = row.getString(row.fieldIndex("_id"));
        return projectId;
    }
}
