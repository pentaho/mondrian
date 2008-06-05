package mondrian.gui.validate;

/**
 * Validation for database schema, table, and columns. Extracted interface from 
 * <code>mondrian.gui.JDBCMetaData</code>.
 * 
 * @author mlowery
 */
public interface JDBCValidator {
    /**
     * Returns the data type of given column.
     * 
     * @return SQL type from java.sql.Types
     */
    int getColumnDataType(String schemaName, String tableName, String colName);

    /**
     * Returns true if column exists.
     */
    boolean isColExists(String schemaName, String tableName, String colName);

    /**
     * Returns true if table exists.
     */
    boolean isTableExists(String schemaName, String tableName);

    /**
     * Returns true if this object successfully connected to database (and
     * validation methods can now be called).
     */
    boolean isInitialized();
}
