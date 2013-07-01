/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.test.loader;

import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapUtil;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;

import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is an abstract base class for the creation and load of one or more
 * database tables with data. Optionally, if a table already exists it can be
 * dropped or its rows deleted.
 * <p>
 * Within this class is the Table sub-class. There is an abstract method the
 * returns an array of Table instances. A Table instance is used to define
 * a database table and its content (its rows or data). A concrete subclass
 * of this class can define how the array of Tables are created (CSV files,
 * XML files, by reading another database, etc.) and then using this
 * class load thoses Tables into a database. In addition, rather than loading
 * a database, this class can be used to generate files containing the
 * sql that can be used to load the database.
 * <p>
 * To use this class the following must be specified:
 * <ul>
 * <li>
 * JDBC Driver: This is used both for the generation of the SQL (using the
 * SqlQuery.Dialect) and loading the database itself.
 * </li>
 * <li>
 * JDBC URL: How to connect to the database.
 * </li>
 * <li>
 * User Name: The database user name (optional).
 * </li>
 * <li>
 * Password: The database password (optional).
 * </li>
 * <li>
 * Output Directory: If specified, then rather than creating and loading the
 * tables in the database, files containing database specific SQL statements are
 * generated. The names of the files are based upon the table name plus
 * a suffix.
 * Each table has four files for the different SQL operations: drop the table,
 * drop the rows in the table, create the table and load the rows in the table.
 * The suffixes have default values which can be overriden the the value
 * of System properties:
 * <ul>
 * <li>
 * The property "mondrian.test.loader.drop.table.suffix" can be used to
 * define the drop table suffix. The default value is  "drop.sql".
 * </li>
 * <li>
 * The property "mondrian.test.loader.drop.table.rows.suffix" can be used to
 * define the drop rows table suffix. The default value is  "droprows.sql".
 * </li>
 * <li>
 * The property "mondrian.test.loader.create.table.suffix" can be used to
 * define the create table suffix. The default value is  "create.sql".
 * </li>
 * <li>
 * The property "mondrian.test.loader.table.rows.suffix" can be used to
 * define the create table suffix. The default value is  "loadrows.sql".
 * </li>
 * </ul>
 * </li>
 * <li>
 * Force: If files are being generated and if they already exist, setting the
 * force flag to true instructs this class to over-write the existing file.
 * If the force flag is false and a file already exists and exception is thrown.
 * </li>
 * </ul>
 * <p>
 * Each Table object created has a Controller object with four boolean instance
 * variables that control what actions are taken when the Table's
 * executeStatements method is called. Those instance variables are:
 * <ul>
 * <li>
 * DropTable: If true, the table is dropped from the database
 * (Default value is true).
 * </li>
 * <li>
 * DropRows: If true, the rows in the table are dropped
 * (Default value is true).
 * </li>
 * <li>
 * CreateTable: If true, the table is created in the database
 * (Default value is true).
 * </li>
 * <li>
 * LoadRows: If true, the rows are loaded into the table
 * (Default value is true).
 * </li>
 * </ul>
 * <p>
 * The Table.Controller must also have its RowStream object defined.
 * A RowStream produces a set of Row objects (see the Row interface
 * below) which in turn has an array of Objects that represent the
 * values of a row in the Table.
 * The default RowStream is an emtpy RowStrean, no rows. The user
 * must implement the RowStrean interface. One such implementation might
 * be a RowStrean containing a list of rows. In this case, all of the
 * rows would be in memory. Another implementation might read each row's
 * data from a file or another database. In this case, the row data is
 * not in memory allowing one to load much larger tables.
 * <p>
 * Each column must have one of the following SQL data type definitions:
 * <ul>
 * <li>
 * INTEGER
 * </li>
 * <li>
 * DECIMAL(*,*)
 * </li>
 * <li>
 * SMALLINT
 * </li>
 * <li>
 * VARCHAR(*)
 * </li>
 * <li>
 * REAL
 * </li>
 * <li>
 * BOOLEAN
 * </li>
 * <li>
 * BIGINT
 * </li>
 * <li>
 * DATE
 * </li>
 * <li>
 * TIMESTAMP
 * </li>
 * </ul>
 * <p>
 * NOTE: Much of the code appearing in this class came from the
 * MondrianFoodMartLoader class.
 *
 * @author Richard M. Emberson
 */
public abstract class DBLoader {
    protected static final Logger LOGGER = Logger.getLogger(DBLoader.class);
    public static final String nl = System.getProperty("line.separator");
    private static final int DEFAULT_BATCH_SIZE = 50;

    public static final String BATCH_SIZE_PROP =
        "mondrian.test.loader.batch.size";
    public static final String JDBC_DRIVER_PROP =
        "mondrian.test.loader.jdbc.driver";
    public static final String JDBC_URL_PROP =
        "mondrian.test.loader.jdbc.url";
    public static final String JDBC_USER_PROP =
        "mondrian.test.loader.jdbc.user";
    public static final String JDBC_PASSWORD_PROP =
        "mondrian.test.loader.jdbc.password";
    public static final String OUTPUT_DIRECTORY_PROP =
        "mondrian.test.loader.output.directory";
    public static final String FORCE_PROP =
        "mondrian.test.loader.force";

    // suffixes of output files
    public static final String DROP_TABLE_INDEX_PROP =
        "mondrian.test.loader.drop.table.index.suffix";
    public static final String DROP_TABLE_INDEX_SUFFIX_DEFAULT =
        "dropindex.sql";
    public static final String CREATE_TABLE_INDEX_PROP =
        "mondrian.test.loader.create.table.index.suffix";
    public static final String CREATE_TABLE_INDEX_SUFFIX_DEFAULT =
        "createindex.sql";

    public static final String DROP_TABLE_PROP =
        "mondrian.test.loader.drop.table.suffix";
    public static final String DROP_TABLE_SUFFIX_DEFAULT = "drop.sql";
    public static final String DROP_TABLE_ROWS_PROP =
        "mondrian.test.loader.drop.table.rows.suffix";
    public static final String DROP_TABLE_ROWS_SUFFIX_DEFAULT = "droprows.sql";
    public static final String CREATE_TABLE_PROP =
        "mondrian.test.loader.create.table.suffix";
    public static final String CREATE_TABLE_SUFFIX_DEFAULT = "create.sql";
    public static final String LOAD_TABLE_ROWS_PROP =
        "mondrian.test.loader.load.table.rows.suffix";
    public static final String LOAD_TABLE_ROWS_SUFFIX_DEFAULT = "loadrows.sql";

    static final Pattern decimalDataTypeRegex =
        Pattern.compile("DECIMAL\\((.*),(.*)\\)");
    static final Pattern varcharDataTypeRegex =
        Pattern.compile("VARCHAR\\((.*)\\)");
    static final DecimalFormat integerFormatter =
        new DecimalFormat(decimalFormat(15, 0));
    static final String dateFormatString = "yyyy-MM-dd";
    static final String oracleDateFormatString = "YYYY-MM-DD";
    static final DateFormat dateFormatter =
        new SimpleDateFormat(dateFormatString);

    /**
     * Generate an appropriate number format string for doubles etc
     * to be used to include a number in an SQL insert statement.
     *
     * Calls decimalFormat(int length, int places) to do the work.
     *
     * @param lengthStr  String representing integer: number of digits to format
     * @param placesStr  String representing integer: number of decimal places
     * @return number format, ie. length = 6, places = 2 => "####.##"
     */
    public static String decimalFormat(String lengthStr, String placesStr) {
        int length = Integer.parseInt(lengthStr);
        int places = Integer.parseInt(placesStr);
        return decimalFormat(length, places);
    }

    /**
     * Generate an appropriate number format string for doubles etc
     * to be used to include a number in an SQL insert statement.
     *
     * @param length  int: number of digits to format
     * @param places  int: number of decimal places
     * @return number format, ie. length = 6, places = 2 => "###0.00"
     */
    public static String decimalFormat(int length, int places) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if ((length - i) == places) {
                buf.append('.');
            }
            if ((length - i) <= (places + 1)) {
                buf.append("0");
            } else {
                buf.append("#");
            }
        }
        return buf.toString();
    }


    /**
     * The RowStream interface allows one to load large sets of
     * rows by streaming them in, one does not have to have all
     * of the row data in memory.
     */
    public interface RowStream {
        Iterator<Row> iterator();
    }

    static final RowStream EMPTY_ROW_STREAM = new RowStream() {
        public Iterator<Row> iterator() {
            List<Row> list = Collections.emptyList();
            return list.iterator();
        }
    };

    public class Table {
        public class Controller {
            private boolean dropTable;
            private boolean dropRows;
            private boolean createTable;
            private boolean loadRows;
            private RowStream rowStream;

            private Controller() {
                this.dropTable = true;
                this.dropRows = true;
                this.createTable = true;
                this.loadRows = true;
                this.rowStream = EMPTY_ROW_STREAM;
            }
            public boolean shouldDropTable() {
                return this.dropTable;
            }
            public void setShouldDropTable(boolean dropTable) {
                this.dropTable = dropTable;
            }
            public boolean shouldDropTableRows() {
                return this.dropRows;
            }
            public void setShouldDropTableRows(boolean dropRows) {
                this.dropRows = dropRows;
            }
            public boolean createTable() {
                return this.createTable;
            }
            public void setCreateTable(boolean createTable) {
                this.createTable = createTable;
            }
            public boolean loadRows() {
                return this.loadRows;
            }
            public void setloadRows(boolean loadRows) {
                this.loadRows = loadRows;
            }
            public void setRowStream(RowStream rowStream) {
                this.rowStream = (rowStream == null)
                    ? EMPTY_ROW_STREAM
                    : rowStream;
            }
            public Iterator<Row> rows() {
                return this.rowStream.iterator();
            }
        }

        private final String name;
        private final Column[] columns;
        private final Controller controller;
        private String dropTableStmt;
        private String dropTableRowsStmt;
        private String createTableStmt;
        private List<String> beforeActionList;
        private List<String> afterActionList;

        public Table(String name, Column[] columns) {
            this.name = name;
            this.columns = columns;
            this.controller = new Controller();
            this.beforeActionList = Collections.emptyList();
            this.afterActionList = Collections.emptyList();
        }

        public String getName() {
            return this.name;
        }

        public String getDropTableStmt() {
            return this.dropTableStmt;
        }
        public void setDropTableStmt(String dropTableStmt) {
            this.dropTableStmt = dropTableStmt;
        }
        public String getDropTableRowsStmt() {
            return this.dropTableRowsStmt;
        }
        public void setDropTableRowsStmt(String dropTableRowsStmt) {
            this.dropTableRowsStmt = dropTableRowsStmt;
        }
        public String getCreateTableStmt() {
            return this.createTableStmt;
        }
        public void setCreateTableStmt(String createTableStmt) {
            this.createTableStmt = createTableStmt;
        }
        public void setBeforeActions(List<String> beforeActionList) {
            if (! beforeActionList.isEmpty()) {
                if (this.beforeActionList == Collections.EMPTY_LIST) {
                    this.beforeActionList = new ArrayList<String>();
                }
                this.beforeActionList.addAll(beforeActionList);
            }
        }
        public void setAfterActions(List<String> afterActionList) {
            if (! afterActionList.isEmpty()) {
                if (this.afterActionList == Collections.EMPTY_LIST) {
                    this.afterActionList = new ArrayList<String>();
                }
                this.afterActionList.addAll(afterActionList);
            }
        }
        public List<String> getBeforeActions() {
            return this.beforeActionList;
        }
        public List<String> getAfterActions() {
            return this.afterActionList;
        }

        public Column[] getColumns() {
            return this.columns;
        }
        public Controller getController() {
            return this.controller;
        }

        public void executeStatements() throws Exception {
            DBLoader.this.executeStatements(this);
        }
    }

    public interface Row {
        Object[] values();
    }
    public static class RowDefault implements Row {
        private final Object[] values;
        public RowDefault(Object[] values) {
            this.values = values;
        }
        public Object[] values() {
            return this.values;
        }
    }

    public static class Column {
        private final String name;
        private final Type type;
        private String typeName;
        private final boolean canBeNull;

        public Column(String name, Type type, boolean canBeNull) {
            this.name = name;
            this.type = type;
            this.canBeNull = canBeNull;
        }

        public void init(Dialect dialect) {
            this.typeName = type.toPhysical(dialect);
        }
        public String getName() {
            return this.name;
        }
        public Type getType() {
            return this.type;
        }
        public String getTypeName() {
            return this.typeName;
        }
        public boolean canBeNull() {
            return this.canBeNull;
        }
        public String getConstraint() {
            return canBeNull ? "" : "NOT NULL";
        }
    }

    /**
     * Represents a logical type, such as "BOOLEAN".<p/>
     *
     * Specific databases will represent this their own particular physical
     * type, for example "TINYINT(1)", "BOOLEAN" or "BIT";
     * see {@link #toPhysical(mondrian.spi.Dialect)}.
     */
    public static class Type {
        public static final Type Integer = new Type("INTEGER");
        public static final Type Decimal = new Type("DECIMAL(10,4)");
        public static final Type Smallint = new Type("SMALLINT");
        public static final Type Varchar30 = new Type("VARCHAR(30)");
        public static final Type Varchar255 = new Type("VARCHAR(255)");
        public static final Type Varchar60 = new Type("VARCHAR(60)");
        public static final Type Real = new Type("REAL");
        public static final Type Boolean = new Type("BOOLEAN");
        public static final Type Bigint = new Type("BIGINT");
        public static final Type Date = new Type("DATE");
        // yyyy-mm-dd hh:mm:ss.fffffffff
        public static final Type Timestamp = new Type("TIMESTAMP");
        public static final Map<String, Type> extraTypes =
            new HashMap<String, Type>();

        public static Type getType(String typeName) {
            String upperCaseTypeName = typeName.toUpperCase();
            if (upperCaseTypeName.equals("INTEGER")) {
                return Type.Integer;
            } else if (upperCaseTypeName.equals("INT")) {
                return Type.Integer;
            } else if (upperCaseTypeName.equals("DECIMAL(10,4)")) {
                return Type.Decimal;
            } else if (upperCaseTypeName.equals("SMALLINT")) {
                return Type.Smallint;
            } else if (upperCaseTypeName.equals("VARCHAR(30)")) {
                return Type.Varchar30;
            } else if (upperCaseTypeName.equals("VARCHAR(255)")) {
                return Type.Varchar255;
            } else if (upperCaseTypeName.equals("VARCHAR(60)")) {
                return Type.Varchar60;
            } else if (upperCaseTypeName.equals("REAL")) {
                return Type.Real;
            } else if (upperCaseTypeName.equals("BOOLEAN")) {
                return Type.Boolean;
            } else if (upperCaseTypeName.equals("BOOL")) {
                return Type.Boolean;
            } else if (upperCaseTypeName.equals("BIGINT")) {
                return Type.Bigint;
            } else if (upperCaseTypeName.equals("DATE")) {
                return Type.Date;
            } else if (upperCaseTypeName.equals("TIMESTAMP")) {
                return Type.Timestamp;
            } else {
                return extraTypes.get(upperCaseTypeName);
            }
        }
        public static Type makeType(String typeName) {
            // only call after calling getType above fails
            // it must either be a DECIMAL or VARCHAR type
            Type type = null;
            String upperCaseTypeName = typeName.toUpperCase();
            Matcher matcher = decimalDataTypeRegex.matcher(upperCaseTypeName);
            if (matcher.matches()) {
                type = new Type(upperCaseTypeName);
                extraTypes.put(upperCaseTypeName, type);
                return type;
            }
            matcher = varcharDataTypeRegex.matcher(upperCaseTypeName);
            if (matcher.matches()) {
                type = new Type(upperCaseTypeName);
                extraTypes.put(upperCaseTypeName, type);
                return type;
            }
            // failed to create new type
            return type;
        }

        /**
         * The name of this type. Immutable, and independent of the RDBMS.
         */
        private final String name;

        public Type(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
        /**
         * Returns the physical type which a given RDBMS (dialect) uses to
         * represent this logical type.
         *
         * @param dialect Dialect
         * @return Physical type the dialect uses to represent this type
         */
        public String toPhysical(Dialect dialect) {
            if (this == Integer
                || this == Decimal
                || this == Smallint
                || this == Varchar30
                || this == Varchar60
                || this == Varchar255
                || this == Real)
            {
                return name;
            }
            if (this == Boolean) {
                switch (dialect.getDatabaseProduct()) {
                case POSTGRESQL:
                    return name;
                case MYSQL:
                    return "TINYINT(1)";
                case MSSQL:
                    return "BIT";
                default:
                    return Smallint.name;
                }
            }
            if (this == Bigint) {
                switch (dialect.getDatabaseProduct()) {
                case ORACLE:
                case FIREBIRD:
                    return "DECIMAL(15,0)";
                default:
                    return name;
                }
            }
            if (this == Date) {
                switch (dialect.getDatabaseProduct()) {
                case MSSQL:
                    return "DATETIME";
                default:
                    return name;
                }
            }
            if (this == Timestamp) {
                switch (dialect.getDatabaseProduct()) {
                case MSSQL:
                case MYSQL:
                    return "DATETIME";
                case INGRES:
                    return "DATE";
                default:
                    return name;
                }
            }
            // for extra types
            if (name.startsWith("DECIMAL(")) {
                switch (dialect.getDatabaseProduct()) {
                case ACCESS:
                    return "CURRENCY";
                }
                return name;
            } else if (name.startsWith("VARCHAR(")) {
                return name;
            }
            throw new AssertionError("unexpected type: " + name);
        }
    }

    private String jdbcDriver;
    private String jdbcURL;
    private String userName;
    private String password;
    private Connection connection;
    private File outputDirectory;
    private boolean force;
    private Writer fileWriter;
    private Dialect dialect;
    private int batchSize;
    private boolean initialize;

    protected DBLoader() {
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.fileWriter = null;
    }

    public abstract Table[] getTables() throws Exception;

    public void dropTables(Table[] tables) throws Exception {
        Exception firstEx = null;
        for (Table table : tables) {
            try {
                dropTable(table);
            } catch (Exception ex) {
                if (firstEx == null) {
                    firstEx = ex;
                }
            }
        }
        if (firstEx != null) {
            throw firstEx;
        }
    }

    public void dropTable(Table table) throws Exception {
        String dropTableStmt = table.getDropTableStmt();
        if (dropTableStmt != null) {
            executeDDL(dropTableStmt);
        }
    }

    public void setOutputDirectory(File outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public File getOutputDirectory() {
        return this.outputDirectory;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public boolean getForce() {
        return this.force;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getJdbcDriver() {
        return this.jdbcDriver;
    }

    public void setJdbcURL(String jdbcURL) {
        this.jdbcURL = jdbcURL;
    }

    public String getJdbcURL() {
        return this.jdbcURL;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserName() {
        return this.userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return this.password;
    }

    public Connection getConnection() {
        return this.connection;
    }

    public void initialize() throws Exception {
        if (this.initialize) {
            return;
        }

        check();
        if (this.connection == null) {
            RolapUtil.loadDrivers(this.jdbcDriver);

            if (this.userName == null) {
                this.connection = DriverManager.getConnection(this.jdbcURL);
            } else {
                this.connection = DriverManager.getConnection(
                    this.jdbcURL, this.userName, this.password);
            }
        }

        final DatabaseMetaData metaData = this.connection.getMetaData();

        String productName = metaData.getDatabaseProductName();
        String version = metaData.getDatabaseProductVersion();
        LOGGER.info("Output connection is " + productName + ", " + version);

        if (!metaData.supportsBatchUpdates()) {
            this.batchSize = 1;
        }
        this.dialect = DialectManager.createDialect(null, this.connection);
        this.initialize = true;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void generateStatements(Table[] tables) throws Exception {
        initialize();
        for (Table table : tables) {
            generateStatements(table);
        }
    }

    protected void generateStatements(Table table) throws Exception {
        Column[] columns = table.getColumns();
        initializeColumns(columns);

        generateBeforeActions(table);
        generateDropTable(table);
        generateDropTableRows(table);
        generateCreateTable(table);
        generateAfterActions(table);
    }


    protected void generateBeforeActions(Table table) {
        List<String> dropIndexList = table.getBeforeActions();
        if (dropIndexList.isEmpty()) {
            return;
        }
        String tableName = table.getName();
        String quotedTableName = quoteId(tableName);
        for (int i = 0; i < dropIndexList.size(); i++) {
            String indexName = dropIndexList.get(i);
            String quotedIndexName = quoteId(indexName);
            String dropIndexStmt =
                "DROP INDEX " + quotedIndexName + " ON " + quotedTableName;
            dropIndexList.set(i, dropIndexStmt);
        }
    }

    protected void generateDropTable(Table table) {
        String tableName = table.getName();
        String dropTableStmt = "DROP TABLE " + quoteId(tableName);
        table.setDropTableStmt(dropTableStmt);
    }

    protected void generateDropTableRows(Table table) {
        String tableName = table.getName();
        String dropTableRowsStmt = "DELETE FROM " + quoteId(tableName);
        table.setDropTableRowsStmt(dropTableRowsStmt);
    }

    protected void generateCreateTable(Table  table) {
        String tableName = table.getName();
        Column[] columns = table.getColumns();
        // Define the table.
        StringBuilder buf = new StringBuilder(50);
        buf.append("CREATE TABLE ");
        buf.append(quoteId(tableName));
        buf.append(" (");

        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(nl);
            buf.append("    ");
            buf.append(quoteId(column.name));
            buf.append(" ");
            buf.append(column.typeName);
            String constraint = column.getConstraint();
            if (!constraint.equals("")) {
                buf.append(" ");
                buf.append(constraint);
            }
        }
        buf.append(nl);
        buf.append(")");

        String ddl = buf.toString();
        table.setCreateTableStmt(ddl);
    }

    protected void generateAfterActions(Table table) {
        List<String> createIndexList = table.getAfterActions();
        if (createIndexList.isEmpty()) {
            return;
        }
        String tableName = table.getName();
        String quotedTableName = quoteId(tableName);
        for (int i = 0; i < createIndexList.size(); i++) {
            String indexAndColumnName = createIndexList.get(i);
            int index = indexAndColumnName.indexOf(' ');
            String indexName = indexAndColumnName.substring(0, index);
            String columnName = indexAndColumnName.substring(index + 1);
            String quotedIndexName = quoteId(indexName.trim());
            String quotedColumnName = quoteId(columnName.trim());
            String createIndexStmt =
                "CREATE INDEX " + quotedIndexName + " ON "
                + quotedTableName + " ( " + quotedColumnName + " )";
            createIndexList.set(i, createIndexStmt);
        }
    }

    public void executeStatements(Table[] tables) throws Exception {
        for (Table table : tables) {
            table.executeStatements();
        }
    }

    protected void executeStatements(Table table) throws Exception {
        executeBeforeActions(table);

        executeDropTable(table);

        executeDropTableRows(table);

        executeCreateTable(table);

        executeLoadTableRows(table);

        executeAfterActions(table);
    }

    protected boolean makeFileWriter(Table table, String suffix)
        throws Exception
    {
        if (this.outputDirectory != null) {
            String fileName = table.getName() + suffix;
            File file = new File(outputDirectory, fileName);
            if (file.exists()) {
                if (this.force) {
                    if (! file.delete()) {
                        throw new Exception(
                            "Table file \""
                            + fileName
                            + "\" could not be deleted");
                    }
                } else {
                    throw new Exception(
                        "Table file \""
                        + fileName
                        + "\" already exists"
                        + " - delete or use force flag");
                }
            }
            this.fileWriter = new FileWriter(file);
            return true;
        } else {
            return false;
        }
    }

    protected void closeFileWriter() {
        try {
            if (this.fileWriter != null) {
                this.fileWriter.flush();
                this.fileWriter.close();
                this.fileWriter = null;
            }
        } catch (IOException ex) {
            LOGGER.debug("Could not close file writer: " + ex);
        }
    }

    /**
     * Undoes all of the database table creations performed
     * when the load method was called.
     */
    public void clear() {
    }

    /**
     * Releases resources.
     *
     * <p>Call this method when the load process is finished and the connection
     * is no longer going to be used.
     */
    public void close() {
        if (connection != null) {
            Util.close(null, null, connection);
        }
    }

    protected void check() throws Exception {
        if (this.connection == null) {
            if (this.jdbcDriver == null) {
                throw new Exception("Not set: jdbcDriver");
            }
            if (this.jdbcURL == null) {
                throw new Exception("Not set: jdbcURL");
            }
        }
    }

    protected void initializeColumns(Column[] columns) {
        // Initialize columns
        for (Column column : columns) {
            column.init(this.dialect);
        }
    }

    protected void executeBeforeActions(Table table) throws Exception {
        List<String> beforeActionList = table.getBeforeActions();
        if (beforeActionList.isEmpty()) {
            return;
        }
        String suffix =
            System.getProperty(
                DROP_TABLE_INDEX_PROP,
                DROP_TABLE_INDEX_SUFFIX_DEFAULT);
        try {
            if (makeFileWriter(table, "." + suffix)) {
                for (String stmt : beforeActionList) {
                    writeDDL(stmt);
                }
            } else {
                for (String stmt : beforeActionList) {
                    executeDDL(stmt);
                }
            }
        } catch (SQLException e) {
            LOGGER.debug(
                "Before Table actions of " + table.getName()
                + " failed. Ignored");
        } finally {
            closeFileWriter();
        }
    }

    protected void executeAfterActions(Table table) throws Exception {
        List<String> afterActionList = table.getAfterActions();
        if (afterActionList.isEmpty()) {
            return;
        }
        String suffix =
            System.getProperty(
                CREATE_TABLE_INDEX_PROP,
                CREATE_TABLE_INDEX_SUFFIX_DEFAULT);
        try {
            if (makeFileWriter(table, "." + suffix)) {
                for (String stmt : afterActionList) {
                    writeDDL(stmt);
                }
            } else {
                for (String stmt : afterActionList) {
                    executeDDL(stmt);
                }
            }
        } catch (SQLException e) {
            LOGGER.debug(
                "After Table actions of " + table.getName()
                + " failed. Ignored");
        } finally {
            closeFileWriter();
        }
    }

    protected boolean executeDropTableRows(Table table) throws Exception {
        try {
            Table.Controller controller = table.getController();
            if (controller.shouldDropTableRows()) {
                String suffix =
                    System.getProperty(
                        DROP_TABLE_ROWS_PROP,
                        DROP_TABLE_ROWS_SUFFIX_DEFAULT);
                String dropTableRowsStmt = table.getDropTableRowsStmt();
                if (makeFileWriter(table, "." + suffix)) {
                    writeDDL(dropTableRowsStmt);
                } else {
                    executeDDL(dropTableRowsStmt);
                }
            }
            return true;
        } catch (SQLException e) {
            LOGGER.debug(
                "Drop Table row of " + table.getName()
                + " failed. Ignored");
        } finally {
            closeFileWriter();
        }
        return false;
    }

    protected boolean executeDropTable(Table table) {
        // If table does not exist, that is OK
        try {
            Table.Controller controller = table.getController();
            if (controller.shouldDropTable()) {
                String suffix =
                    System.getProperty(
                        DROP_TABLE_PROP,
                        DROP_TABLE_SUFFIX_DEFAULT);
                String dropTableStmt = table.getDropTableStmt();
                if (makeFileWriter(table, "." + suffix)) {
                    writeDDL(dropTableStmt);
                } else {
                    executeDDL(dropTableStmt);
                }
            }
            return true;
        } catch (Exception e) {
            LOGGER.debug("Drop of " + table.getName() + " failed. Ignored");
        } finally {
            closeFileWriter();
        }
        return false;
    }

    protected boolean executeCreateTable(Table  table) {
        try {
            Table.Controller controller = table.getController();
            if (controller.createTable()) {
                String suffix =
                    System.getProperty(
                        CREATE_TABLE_PROP,
                        CREATE_TABLE_SUFFIX_DEFAULT);
                String ddl = table.getCreateTableStmt();
                if (makeFileWriter(table, "." + suffix)) {
                    writeDDL(ddl);
                } else {
                    executeDDL(ddl);
                }
            }
            return true;
        } catch (Exception e) {
            throw MondrianResource.instance().CreateTableFailed.ex(
                table.getName(), e);
        } finally {
            closeFileWriter();
        }
    }

    protected int executeLoadTableRows(Table table) {
        try {
            String suffix =
                System.getProperty(
                    LOAD_TABLE_ROWS_PROP,
                    LOAD_TABLE_ROWS_SUFFIX_DEFAULT);
            makeFileWriter(table, "." + suffix);

            Table.Controller controller = table.getController();
            int rowsAdded = 0;
            if (controller.loadRows()) {
                String[] batch = new String[this.batchSize];
                int nosInBatch = 0;

                Iterator<Row> it = controller.rows();
                boolean displayedInsert = false;
                while (it.hasNext()) {
                    Row row = it.next();
                    Object[] values = row.values();
                    String insertStatement =
                        createInsertStatement(table, values);
                    if (!displayedInsert && LOGGER.isDebugEnabled()) {
                        LOGGER.debug(
                            "Example Insert statement: " + insertStatement);
                        displayedInsert = true;
                    }
                    batch[nosInBatch++] = insertStatement;
                    if (nosInBatch >= this.batchSize) {
                        rowsAdded += writeBatch(batch, nosInBatch);
                        nosInBatch = 0;
                    }
                }
                if (nosInBatch > 0) {
                    rowsAdded += writeBatch(batch, nosInBatch);
                }
            }
            return rowsAdded;
        } catch (Exception e) {
            throw Util.newError(e, "Load of " + table.getName() + " failed.");
        } finally {
            closeFileWriter();
        }
    }

    protected String createInsertStatement(Table table, Object[] values)
        throws Exception
    {
        Column[] columns = table.getColumns();
        if (columns.length != values.length) {
            int numberOfNullColumns = 0;
            for (Column c : columns) {
                if (c.canBeNull()) {
                    numberOfNullColumns++;
                }
            }
            if (numberOfNullColumns == 0) {
                StringBuilder buf = new StringBuilder();
                buf.append("For table ");
                buf.append(table.getName());
                buf.append(" the columns length ");
                buf.append(columns.length);
                buf.append(" does not equal the values length ");
                buf.append(values.length);
                throw new Exception(buf.toString());
            } else if (columns.length != values.length + numberOfNullColumns) {
                StringBuilder buf = new StringBuilder();
                buf.append("For table ");
                buf.append(table.getName());
                buf.append(" the columns length ");
                buf.append(columns.length);
                buf.append(" and number allowed to be null ");
                buf.append(numberOfNullColumns);
                buf.append(" does not equal the values length ");
                buf.append(values.length);
                throw new Exception(buf.toString());
            }
            Object[] vs = new Object[columns.length];
            for (int i = 0, j = 0; i < columns.length; i++) {
                if (! columns[i].canBeNull()) {
                    vs[i] = values[j++];
                }
            }
            values = vs;
        }

        StringBuilder buf = new StringBuilder();
        buf.append("INSERT INTO ");
        buf.append(quoteId(table.getName()));
        buf.append(" ( ");

        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(quoteId(column.getName()));
        }

        buf.append(" ) VALUES ( ");
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(columnValue(column, values[i]));
        }
        buf.append(" )");
        return buf.toString();
    }

    /**
     * Quote the given SQL identifier suitable for the output DBMS.
     *
     * @param name Identifier
     * @return Quoted identifier
     */
    protected String quoteId(String name) {
        return this.dialect.quoteIdentifier(name);
    }

    /**
     * Convert the columns value to a string based upon its column type.
     *
     * @param column Column
     * @param value Column value
     * @return Column value as a SQL string
     */
    protected String columnValue(Column column, Object value) {
        Type type = column.getType();
        String typeName = type.getName();

        if (value == null) {
            return "NULL";
        } else if ((value instanceof String)
                && (((String)value).length() == 0))
        {
            return "NULL";
        }

        /*
         * Output for an INTEGER column, handling Doubles and Integers
         * in the result set
         */
        if (type == Type.Integer) {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Double) {
                Double result = (Double) value;
                return integerFormatter.format(result.doubleValue());
            } else if (value instanceof Integer) {
                Integer result = (Integer) value;
                return result.toString();
            }


        /*
         * Output for an SMALLINT column, handling Integers
         * in the result set
         */
        } else if (type == Type.Smallint) {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Boolean) {
                return (Boolean) value ? "1" : "0";
            } else if (value instanceof Integer) {
                Integer result = (Integer) value;
                return result.toString();
            }

        /*
         * Output for an BIGINT column, handling Doubles and Longs
         * in the result set
         */
        } else if (type == Type.Bigint) {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Double) {
                Double result = (Double) value;
                return integerFormatter.format(result.doubleValue());
            } else if (value instanceof Long) {
                Long result = (Long) value;
                return result.toString();
            }

        /*
         * Output for a String, managing embedded quotes
         */
        } else if ((type == Type.Varchar30)
                   || (type == Type.Varchar255)
                   || (type == Type.Varchar60)
                   || typeName.startsWith("VARCHAR("))
        {
            if (value instanceof String) {
                return embedQuotes((String) value);
            }

        /*
         * Output for a TIMESTAMP
         */
        } else if (type == Type.Timestamp) {
            if (value instanceof String) {
                Timestamp ts = Timestamp.valueOf((String) value);
                switch (dialect.getDatabaseProduct()) {
                case ORACLE:
                case LUCIDDB:
                    return "TIMESTAMP '" + ts + "'";
                default:
                    return "'" + ts + "'";
                }
            } else if (value instanceof Timestamp) {
                Timestamp ts = (Timestamp) value;
                switch (dialect.getDatabaseProduct()) {
                case ORACLE:
                case LUCIDDB:
                    return "TIMESTAMP '" + ts + "'";
                default:
                    return "'" + ts + "'";
                }
            }

        /*
         * Output for a DATE
         */
        } else if (type == Type.Date) {
            if (value instanceof String) {
                Date dt = Date.valueOf((String) value);
                switch (dialect.getDatabaseProduct()) {
                case ORACLE:
                case LUCIDDB:
                    return "DATE '" + dateFormatter.format(dt) + "'";
                default:
                    return "'" + dateFormatter.format(dt) + "'";
                }
            } else if (value instanceof Date) {
                Date dt = (Date) value;
                switch (dialect.getDatabaseProduct()) {
                case ORACLE:
                case LUCIDDB:
                    return "DATE '" + dateFormatter.format(dt) + "'";
                default:
                    return "'" + dateFormatter.format(dt) + "'";
                }
            }

        /*
         * Output for a FLOAT
         */
        } else if (type == Type.Real) {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Float) {
                Float result = (Float) value;
                return result.toString();
            }

        /*
         * Output for a DECIMAL(length, places)
         */
        } else if ((type == Type.Decimal) || typeName.startsWith("DECIMAL(")) {
            if (value instanceof String) {
                return (String) value;
            } else {
                Matcher matcher = decimalDataTypeRegex.matcher(typeName);
                if (!matcher.matches()) {
                    throw new RuntimeException(
                        "Bad DECIMAL column type for " + typeName);
                 }
                DecimalFormat formatter = new DecimalFormat(
                    decimalFormat(matcher.group(1), matcher.group(2)));
                if (value instanceof Double) {
                    Double result = (Double) value;
                    return formatter.format(result.doubleValue());
                } else if (value instanceof BigDecimal) {
                    BigDecimal result = (BigDecimal) value;
                    return formatter.format(result);
                }
            }

        /*
         * Output for a BOOLEAN (Postgres) or BIT (other DBMSs)
         */
        } else if (type == Type.Boolean) {
            if (value instanceof String) {
                String trimmedValue = ((String) value).trim();
                switch (dialect.getDatabaseProduct()) {
                case MYSQL:
                case ORACLE:
                case DB2:
                case DB2_AS400:
                case DB2_OLD_AS400:
                case FIREBIRD:
                case MSSQL:
                case DERBY:
                case INGRES:
                    if (trimmedValue.equals("true")) {
                        return "1";
                    } else if (trimmedValue.equals("false")) {
                        return "0";
                    } else if (trimmedValue.equals("1")) {
                        return "1";
                    } else if (trimmedValue.equals("0")) {
                        return "0";
                    }
                default:
                    if (trimmedValue.equals("1")) {
                        return "true";
                    } else if (trimmedValue.equals("0")) {
                        return "false";
                    }
                }
            } else if (value instanceof Boolean) {
                Boolean result = (Boolean) value;
                return result.toString();
            }

        /*
         * Output for a BOOLEAN - TINYINT(1) (MySQL)
        } else if (columnType.startsWith("TINYINT(1)")) {
            Boolean result = (Boolean) obj;
            if (result.booleanValue()) {
                return "1";
            } else {
                return "0";
            }
         */
        }
        throw new RuntimeException(
            "Unknown column type: " + typeName
            + " for column: " + column.getName());
    }

    /**
     * Generate an appropriate string to use in an SQL insert statement for a
     * VARCHAR colummn, taking into account NULL strings and strings with
     * embedded quotes
     *
     * @param original  String to transform
     * @return NULL if null string, otherwise massaged string with doubled
     * quotes for SQL
     */
    protected String embedQuotes(String original) {
        if (original == null) {
            return "NULL";
        }

        StringBuilder buf = new StringBuilder();
        buf.append("'");
        for (int i = 0; i < original.length(); i++) {
            char ch = original.charAt(i);
            buf.append(ch);
            if (ch == '\'') {
                buf.append('\'');
            }
        }
        buf.append("'");
        return buf.toString();
    }

    /**
     * If we are outputting to JDBC,
     *      Execute the given set of SQL statements
     *
     * Otherwise,
     *      output the statements to a file.
     *
     * @param batch         SQL statements to execute
     * @param batchSize     # SQL statements to execute
     * @return              # SQL statements executed
     */
    protected int writeBatch(String[] batch, int batchSize)
        throws IOException, SQLException
    {
        if (this.fileWriter != null) {
            for (int i = 0; i < batchSize; i++) {
                this.fileWriter.write(batch[i]);
                this.fileWriter.write(';');
                this.fileWriter.write(nl);
            }
        } else {
            if (connection.getMetaData().supportsTransactions()) {
                connection.setAutoCommit(false);
            }
            Statement stmt = connection.createStatement();
            if (batchSize == 1) {
                // Don't use batching if there's only one item. This allows
                // us to work around bugs in the JDBC driver by setting
                // outputJdbcBatchSize=1.
                stmt.execute(batch[0]);
            } else {
                for (int i = 0; i < batchSize; i++) {
                    stmt.addBatch(batch[i]);
                }
                int [] updateCounts;

                try {
                    updateCounts = stmt.executeBatch();
                } catch (SQLException e) {
                    for (int i = 0; i < batchSize; i++) {
                        LOGGER.error("Error in SQL batch: " + batch[i]);
                    }
                    throw e;
                }
                int updates = 0;
                for (int i = 0;
                    i < updateCounts.length;
                    updates += updateCounts[i], i++)
                {
                    if (updateCounts[i] == 0) {
                        LOGGER.error("Error in SQL: " + batch[i]);
                    }
                }
                if (updates < batchSize) {
                    throw new RuntimeException(
                        "Failed to execute batch: " + batchSize
                        + " versus " + updates);
                }
            }
            if (connection.getMetaData().supportsTransactions()) {
                connection.commit();
            }
            stmt.close();
            if (connection.getMetaData().supportsTransactions()) {
                connection.setAutoCommit(true);
            }
        }
        return batchSize;
    }

    protected void writeDDL(String ddl) throws Exception {
        LOGGER.debug(ddl);

        this.fileWriter.write(ddl);
        this.fileWriter.write(';');
        this.fileWriter.write(nl);
    }

    protected void executeDDL(String ddl) throws Exception {
        LOGGER.debug(ddl);

        Statement statement = getConnection().createStatement();
        statement.execute(ddl);
    }
}

// End DBLoader.java
