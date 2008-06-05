package mondrian.gui.validate.impl;

import mondrian.gui.JDBCMetaData;
import mondrian.gui.validate.JDBCValidator;

/**
 * Implementation of <code>JDBCValidator</code> for Workbench.
 * 
 * @author mlowery
 */
public class WorkbenchJDBCValidator implements JDBCValidator {

    private JDBCMetaData jdbcMetadata;

    public WorkbenchJDBCValidator(JDBCMetaData jdbcMetadata) {
        super();
        this.jdbcMetadata = jdbcMetadata;
    }

    public int getColumnDataType(String schemaName, String tableName,
                    String colName) {
        return jdbcMetadata.getColumnDataType(schemaName, tableName, colName);
    }

    public boolean isColExists(String schemaName, String tableName,
                    String colName) {
        return jdbcMetadata.isColExists(schemaName, tableName, colName);
    }

    public boolean isInitialized() {
        return jdbcMetadata.getErrMsg() == null;
    }

    public boolean isTableExists(String schemaName, String tableName) {
        return jdbcMetadata.isTableExists(schemaName, tableName);
    }

}
