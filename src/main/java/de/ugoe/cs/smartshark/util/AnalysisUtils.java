
package de.ugoe.cs.smartshark.util;

import static org.apache.spark.sql.functions.col;

import java.util.LinkedList;
import java.util.List;

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
     * Determines the Id of a project using its name.
     * </p>
     *
     * @param projectName
     *            name of the project
     * @return project Id
     */
    public String getProjectId(String projectName) {
        Dataset<Row> projects = DBUtilFactory.getDBUtils(sparkSession).loadData("project")
            .filter(col("name").like(projectName));
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
    
    /**
     * <p>
     * Determins the Ids of the VCS systems of a project
     * </p>
     *
     * @param projectName name of the project
     * @return Ids of associated VCS systems
     */
    public List<String> getVCSIds(String projectName) {
        List<String> vcsIds = new LinkedList<>();
        String projectId = getProjectId(projectName);
        
        Dataset<Row> vcsSystems = DBUtilFactory.getDBUtils(sparkSession).loadData("vcs_system").filter(col("project_id").like(projectId));
        
        for( Row vcsRow : vcsSystems.collectAsList()) {
            vcsIds.add(vcsRow.getString(vcsRow.fieldIndex("_id")));
        }
        return vcsIds;
    }
}
