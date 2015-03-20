/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho and others
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// Copyright (C) 2006-2007 JasperSoft
// All Rights Reserved.
*/

package mondrian.gui;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;

/**
 */
public class JdbcMetaData {

    private static final Logger LOGGER = Logger.getLogger(JdbcMetaData.class);

    // E.g. "org.postgresql.Driver"
    String jdbcDriverClassName = null;

    // E.g. "jdbc:postgresql://localhost:5432/hello?user=postgres&password=post"
    String jdbcUsername = null;

    String jdbcConnectionUrl = null;
    String jdbcPassword = null;
    String jdbcSchema = null;
    boolean requireSchema = false;

    Connection conn = null;
    DatabaseMetaData md = null;

    Workbench workbench;

    public static final String LEVEL_SEPARATOR = "->";

    private String errMsg = null;
    private Database db = new Database();

    public JdbcMetaData(
        Workbench wb,
        String jdbcDriverClassName,
        String jdbcConnectionUrl,
        String jdbcUsername,
        String jdbcPassword,
        String jdbcSchema,
        boolean requireSchema)
    {
        this.workbench = wb;
        this.jdbcConnectionUrl = jdbcConnectionUrl;
        this.jdbcDriverClassName = jdbcDriverClassName;
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;
        this.jdbcSchema = jdbcSchema;
        this.requireSchema = requireSchema;

        if (initConnection() == null) {
            setAllSchemas();
            closeConnection();
        }
    }

    public boolean getRequireSchema() {
        return requireSchema;
    }

    /**
     * @return the workbench i18n converter
     */
    public I18n getResourceConverter() {
        return workbench.getResourceConverter();
    }

    /**
     * Tests database connection. Called from Preferences dialog button test
     * connection.
     */
    public JdbcMetaData(
        String jdbcDriverClassName,
        String jdbcConnectionUrl,
        String jdbcUsername,
        String jdbcPassword)
    {
        this.jdbcConnectionUrl = jdbcConnectionUrl;
        this.jdbcDriverClassName = jdbcDriverClassName;
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;

        if (initConnection() == null) {
            closeConnection();
        }
    }

    /* Creates a database connection and initializes the meta data details */
    public String initConnection() {
        LOGGER.debug("JdbcMetaData: initConnection");

        try {
            if (jdbcDriverClassName == null
                || jdbcDriverClassName.trim().length() == 0
                || jdbcConnectionUrl == null
                || jdbcConnectionUrl.trim().length() == 0)
            {
                errMsg = getResourceConverter().getFormattedString(
                    "jdbcMetaData.blank.exception",
                    "Driver={0}\nConnection URL={1}\nUse Preferences to set Database Connection parameters first and then open a Schema",
                    jdbcDriverClassName,
                    jdbcConnectionUrl);
                return errMsg;
            }

            Class.forName(jdbcDriverClassName);

            if (jdbcUsername != null && jdbcUsername.length() > 0) {
                conn = DriverManager.getConnection(
                    jdbcConnectionUrl, jdbcUsername, jdbcPassword);
            } else {
                conn = DriverManager.getConnection(jdbcConnectionUrl);
            }

            LOGGER.debug("JDBC connection OPEN");
            md = conn.getMetaData();

            db.productName = md.getDatabaseProductName();
            db.productVersion = md.getDatabaseProductVersion();
            db.catalogName = conn.getCatalog();

            LOGGER.debug("Catalog name = " + db.catalogName);
            LOGGER.debug("Database Product Name: " + db.productName);
            LOGGER.debug("Database Product Version: " + db.productVersion);
            LOGGER.debug("JdbcMetaData: initConnection - no error");
            return null;
        } catch (Exception e) {
            errMsg =
                e.getClass().getSimpleName() + " : " + e.getLocalizedMessage();
            LOGGER.error("Database connection exception : " + errMsg, e);
            return errMsg;
            //e.printStackTrace();
        }
    }

    public void closeConnection() {
        if (conn == null) {
            return;
        }

        md = null;
        try {
            conn.close();
            LOGGER.debug("JDBC connection CLOSE");
        } catch (Exception e) {
            LOGGER.error(e);
        }
        conn = null;
    }

    /**
     * Check to see if the schemaName is in the list of allowed jdbc schemas
     *
     * @param schemaName the name of the schmea
     * @return true if found, or if jdbcSchema is null
     */
    private boolean inJdbcSchemas(String schemaName) {
        if (jdbcSchema == null || jdbcSchema.trim().length() == 0) {
            return true;
        }

        String schemas[] = jdbcSchema.split("[,;]");
        for (String schema : schemas) {
            if (schema.trim().equalsIgnoreCase(schemaName)) {
                return true;
            }
        }

        return false;
    }

    /* list all schemas in the currently connected database */
    public List<String> listAllSchemas() {
        LOGGER.debug("JdbcMetaData: listAllSchemas");

        if (initConnection() != null) {
            return null;
        }

        List<String> schemaNames = new ArrayList<String>();
        ResultSet rs = null;
        try {
            rs = md.getSchemas();

            while (rs.next()) {
                String schemaName = rs.getString("TABLE_SCHEM");
                schemaNames.add(schemaName);
            }
        } catch (Exception e) {
            LOGGER.debug(
                "Exception : Database does not support schemas." + e
                    .getMessage());
            return null;
        } finally {
            try {
                rs.close();
                closeConnection();
            } catch (Exception e) {
                // ignore
            }
        }


        return schemaNames;
    }

    /* set all schemas in the currently connected database */
    private void setAllSchemas() {
        LOGGER.debug("JdbcMetaData: setAllSchemas");

        ResultSet rs = null;
        boolean gotSchema = false;
        try {
            rs = md.getSchemas();
            while (rs.next()) {
                String schemaName = rs.getString("TABLE_SCHEM");
                if (inJdbcSchemas(schemaName)) {
                    DbSchema dbs = new DbSchema();
                    dbs.name = schemaName;
                    LOGGER.debug("JdbcMetaData: setAllTables - " + dbs.name);
                    setAllTables(dbs);
                    db.addDbSchema(dbs);
                    gotSchema = true;
                }
            }
        } catch (Exception e) {
            LOGGER.debug(
                "Exception : Database does not support schemas." + e
                    .getMessage());
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (!gotSchema) {
            LOGGER.debug(
                "JdbcMetaData: setAllSchemas - tables with no schema name");
            DbSchema dbs = new DbSchema();
            dbs.name = null;    //tables with no schema name
            setAllTables(dbs);
            db.addDbSchema(dbs);
        }
    }

    /* set all tables in the currently connected database */
    private void setAllTables(DbSchema dbs) {
        LOGGER.debug("JdbcMetaData: Loading schema: '" + dbs.name + "'");
        ResultSet rs = null;
        try {
            // Tables and views can be used
            try {
                rs = md.getTables(
                    null, dbs.name, null, new String[]{"TABLE", "VIEW", "MATERIALIZED VIEW"});
            } catch (Exception e) {
                // this is a workaround for databases that throw an exception
                // when views are requested.
                rs = md.getTables(null, dbs.name, null, new String[]{"TABLE"});
            }
            while (rs.next()) {
                // Oracle 10g Driver returns bogus BIN$ tables that cause
                // exceptions
                String tbname = rs.getString("TABLE_NAME");
                if (!tbname.matches("(?!BIN\\$).+")) {
                    continue;
                }

                DbTable dbt = null;

                /* Note: Imported keys are foreign keys which are primary keys
                 * of in some other tables; Exported keys are primary keys which
                 * are referenced as foreign keys in other tables.
                 */
                try {
                    ResultSet rs_fks = md.getImportedKeys(null, dbs.name, tbname);
                    try {
                        if (rs_fks.next()) {
                            dbt = new FactTable();
                            do {
                                ((FactTable) dbt).addFks(
                                    rs_fks.getString("FKCOLUMN_NAME"),
                                    rs_fks.getString("pktable_name"));
                            } while (rs_fks.next());
                        } else {
                            dbt = new DbTable();
                        }
                    } finally {
                        try {
                            rs_fks.close();
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                } catch (Exception e) {
                  // this fails in some cases (Redshift)
                  LOGGER.warn("unable to process foreign keys", e);
                  if (dbt == null) {
                    dbt = new FactTable();
                  }
                }
                dbt.schemaName = dbs.name;
                dbt.name = tbname;
                setPKey(dbt);
                // Lazy loading
                // setColumns(dbt);
                dbs.addDbTable(dbt);
                db.addDbTable(dbt);
            }
        } catch (Exception e) {
            LOGGER.error("setAllTables", e);
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Gets the Primary key name for a given table name.
     * This key may be a  composite key made of multiple columns.
     */
    private void setPKey(DbTable dbt) {
        ResultSet rs = null;
        try {
            rs = md.getPrimaryKeys(null, dbt.schemaName, dbt.name);
            if (rs.next()) {
                //   // a column may have been given a primary key name
                //===dbt.pk = rs.getString("PK_NAME");
                // We need the column name which is primary key for the given
                // table.
                dbt.pk = rs.getString("column_name");
            }
        } catch (Exception e) {
            LOGGER.error("setPKey", e);
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void setColumns(String schemaName, String tableName) {
        LOGGER.debug(
            "setColumns: <" + tableName + "> in schema <" + schemaName + ">");
        DbTable dbt = db.getTable(schemaName, tableName);
        if (dbt == null) {
            LOGGER.debug(
                "No table with name: <"
                + tableName
                + "> in schema <"
                + schemaName
                + ">");
            return;
        }
        if (initConnection() != null) {
            return;
        }
        try {
            setColumns(dbt);
            LOGGER.debug("got " + dbt.colsDataType.size() + " columns");
        } finally {
            closeConnection();
        }
    }

    /**
     * Gets all columns for a given table name.
     *
     * Assumes that the caller has acquired a connection using
     * {@link #initConnection()}.
     */
    private void setColumns(DbTable dbt) {
        ResultSet rs = null;
        try {
            rs = md.getColumns(null, dbt.schemaName, dbt.name, null);
            while (rs.next()) {
                DbColumn col = new DbColumn();

                col.dataType = rs.getInt("DATA_TYPE");
                col.name = rs.getString("COLUMN_NAME");
                col.typeName = rs.getString("TYPE_NAME");
                col.columnSize = rs.getInt("COLUMN_SIZE");
                col.decimalDigits = rs.getInt("DECIMAL_DIGITS");

                dbt.addColsDataType(col);
            }
        } catch (Exception e) {
            LOGGER.error("setColumns", e);
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // The following functions provide an interface to JdbcMetaData class to
    // retrieve the meta data details

    public List<String> getAllSchemas() {
        return db.getAllSchemas();
    }

    /**
     * Returns all tables in a given schema.
     */
    public List<String> getAllTables(String schemaName) {
        return db.getAllTables(schemaName);
    }

    /**
     * Returns all tables in given schema minus the given table name.
     */
    public List<String> getAllTables(String schemaName, String minusTable) {
        if (minusTable == null) {
            return getAllTables(schemaName);
        } else {
            List<String> allTablesMinusOne = new ArrayList<String>();
            for (String s : getAllTables(schemaName)) {
                if (s.endsWith(minusTable)) {
                    // startsWith and endsWith cannot be compared with
                    // null argument, throws exception
                    if ((schemaName == null) || s.startsWith(schemaName)) {
                        continue;
                    }
                }
                allTablesMinusOne.add(s);
            }
            return allTablesMinusOne;
        }
    }

    /* get all possible cases of fact tables in a schema */
    public List<String> getFactTables(String schemaName) {
        return db.getFactTables(schemaName);
    }

    /**
     * Gets all possible cases of dimension tables which are linked to given
     * fact table by foreign keys.
     */
    public List<String> getDimensionTables(
        String schemaName,
        String factTable)
    {
        List<String> dimeTables = new ArrayList<String>();
        if (factTable == null) {
            return dimeTables;
        } else {
            return db.getDimensionTables(schemaName, factTable);
        }
    }

    public boolean isTableExists(String schemaName, String tableName) {
        if (tableName == null) {
            return true;
        } else {
            return db.tableExists(schemaName, tableName);
        }
    }

    public boolean isColExists(
        String schemaName, String tableName, String colName)
    {
        if (tableName == null || colName == null) {
            return true;
        } else {
            if (!db.hasColumns(schemaName, tableName)) {
                setColumns(schemaName, tableName);
            }

            return db.colExists(schemaName, tableName, colName);
        }
    }

    /* get all foreign keys in given fact table */
    public List<String> getFactTableFKs(String schemaName, String factTable) {
        List<String> fks = new ArrayList<String>();
        if (factTable == null) {
            return fks;
        } else {
            return db.getFactTableFKs(schemaName, factTable);
        }
    }

    public String getTablePK(String schemaName, String tableName) {
        if (tableName == null) {
            return null;
        } else {
            return db.getTablePK(schemaName, tableName);
        }
    }

    /**
     * Gets all columns of given table in schema.
     * column string is formatted.
     */
    public List<String> getAllColumns(String schemaName, String tableName) {
        List<String> allcols = new ArrayList<String>();

        if (tableName == null) {
            List<String> allTables = getAllTables(schemaName);

            for (int i = 0; i < allTables.size(); i++) {
                String tab = allTables.get(i);
                List<String> cols;
                if (tab.indexOf(LEVEL_SEPARATOR) == -1) {
                    cols = getAllColumns(schemaName, tab);
                } else {
                    String[] names = tab.split(LEVEL_SEPARATOR);
                    cols = getAllColumns(names[0], names[1]);
                }
                for (int j = 0; j < cols.size(); j++) {
                    String col = cols.get(j);
                    allcols.add(tab + LEVEL_SEPARATOR + col);
                }
            }
            return allcols;
        } else {
            if (!db.hasColumns(schemaName, tableName)) {
                setColumns(schemaName, tableName);
            }
            return db.getAllColumns(schemaName, tableName);
        }
    }

    /**
     * Returns all columns of given table in schema.
     * Column string is formatted.
     */
    public List<DbColumn> getAllDbColumns(String schemaName, String tableName) {
        List<DbColumn> allcols = new ArrayList<DbColumn>();

        if (tableName == null) {
            List<String> allTables = getAllTables(schemaName);

            for (int i = 0; i < allTables.size(); i++) {
                String tab = allTables.get(i);
                List<DbColumn> cols;
                if (tab.indexOf(LEVEL_SEPARATOR) == -1) {
                    cols = getAllDbColumns(schemaName, tab);
                } else {
                    String[] names = tab.split(LEVEL_SEPARATOR);
                    cols = getAllDbColumns(names[0], names[1]);
                }
                allcols.addAll(cols);
            }
            return allcols;
        } else {
            if (!db.hasColumns(schemaName, tableName)) {
                setColumns(schemaName, tableName);
            }
            return db.getAllDbColumns(schemaName, tableName);
        }
    }

    // get column data type of given table and its col
    public int getColumnDataType(
        String schemaName, String tableName, String colName)
    {
        if (tableName == null || colName == null) {
            return -1;
        } else {
            if (!db.hasColumns(schemaName, tableName)) {
                setColumns(schemaName, tableName);
            }
            return db.getColumnDataType(schemaName, tableName, colName);
        }
    }

    /**
     * Gets column definition of given table and its col.
     *
     * @param schemaName Schema name
     * @param tableName Table name
     * @param colName Column name
     * @return Column definition
     */
    public DbColumn getColumnDefinition(
        String schemaName, String tableName, String colName)
    {
        if (tableName == null || colName == null) {
            return null;
        } else {
            if (!db.hasColumns(schemaName, tableName)) {
                setColumns(schemaName, tableName);
            }
            return db.getColumnDefinition(schemaName, tableName, colName);
        }
    }

    public String getDbCatalogName() {
        return db.catalogName;
    }

    public String getDatabaseProductName() {
        return db.productName;
    }

    public String getJdbcConnectionUrl() {
        return jdbcConnectionUrl;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException(
                "need at least 2 args: driver class and jdbcUrl");
        }

        String driverClass = args[0];
        String jdbcUrl = args[1];

        String username = null;
        String password = null;

        if (args.length > 2) {
            if (args.length != 4) {
                throw new RuntimeException(
                    "need 4 args: including user name and password");
            }
            username = args[2];
            password = args[3];
        }

        JdbcMetaData sb = new JdbcMetaData(
            null, driverClass, jdbcUrl, username, password, "", false);

        List<String> foundSchemas = sb.getAllSchemas();
        System.out.println("allSchemas = " + foundSchemas);

        for (String schemaName : foundSchemas) {
            List<String> foundTables = sb.getAllTables(schemaName);

            if (foundTables != null && foundTables.size() > 0) {
                System.out.println("schema = " + schemaName);
                for (String tableName : foundTables) {
                    System.out.println("\t" + tableName);

                    List<String> foundColumns = sb.getAllColumns(
                        schemaName, tableName);

                    for (String columnName : foundColumns) {
                        System.out.println("\t\t" + columnName);
                    }
                }
            }
        }
    }

    /**
     * Database metadata.
     */
    class Database {
        String catalogName = ""; // database name.
        String productName = "Unknown";
        String productVersion = "";

        // list of all schemas in database
        Map<String, DbSchema> schemas = new TreeMap<String, DbSchema>();
            //ordered collection, allows duplicates and null
        Map<String, TableTracker> tables = new TreeMap<String, TableTracker>();
            // list of all tables in all schemas in database

        List<String> allSchemas;

        private void addDbSchema(DbSchema dbs) {
            schemas.put(
                dbs.name != null
                    ? dbs.name
                    : "", dbs);
        }

        class TableTracker {
            List<DbTable> namedTable = new ArrayList<DbTable>();

            public void add(DbTable table) {
                namedTable.add(table);
            }

            public int count() {
                return namedTable.size();
            }
        }

        private void addDbTable(DbTable dbs) {
            TableTracker tracker = tables.get(dbs.name);

            if (tracker == null) {
                tracker = new TableTracker();
                tables.put(dbs.name, tracker);
            }
            tracker.add(dbs);
        }

        private boolean schemaNameEquals(String a, String b) {
            return (a != null && a.equals(b));
        }

        private DbSchema getSchema(String schemaName) {
            return schemas.get(
                schemaName != null
                    ? schemaName
                    : "");
        }

        private List<String> getAllSchemas() {
            if (allSchemas == null) {
                allSchemas = new ArrayList<String>();

                allSchemas.addAll(schemas.keySet());
            }
            return allSchemas;
        }

        private boolean tableExists(String sname, String tableName) {
            return getTable(sname, tableName) != null;
        }

        private DbTable getTable(String sname, String tableName) {
            if (sname == null || sname.equals("")) {
                TableTracker t = tables.get(tableName);
                if (t != null) {
                    return t.namedTable.get(0);
                } else {
                    return null;
                }
            } else {
                DbSchema s = schemas.get(sname);

                if (s == null) {
                    return null;
                }

                return s.getTable(tableName);
            }
        }

        private boolean hasColumns(String schemaName, String tableName) {
            DbTable table = getTable(schemaName, tableName);
            if (table != null) {
                return table.hasColumns();
            }
            return false;
        }

        private boolean colExists(
            String sname, String tableName, String colName)
        {
            DbTable t = getTable(sname, tableName);

            if (t == null) {
                return false;
            }

            return t.getColumn(colName) != null;
        }

        private List<String> getAllTables(String sname) {
            return getAllTables(sname, false);
        }

        private List<String> getFactTables(String sname) {
            return getAllTables(sname, true);
        }

        private List<String> getAllTables(String sname, boolean factOnly) {
            List<String> v = new ArrayList<String>();

            if (sname == null || sname.equals("")) {
                // return a list of "schemaname -> table name" string objects
                for (TableTracker tt : tables.values()) {
                    for (DbTable t : tt.namedTable) {
                        if (!factOnly || (factOnly && t instanceof FactTable)) {
                            if (t.schemaName == null) {
                                v.add(t.name);
                            } else {
                                v.add(t.schemaName + LEVEL_SEPARATOR + t.name);
                            }
                        }
                    }
                }
            } else {
                // return a list of "tablename" string objects
                DbSchema s = getSchema(sname);

                if (s != null) {
                    for (DbTable t : s.tables.values()) {
                        if (!factOnly || (factOnly && t instanceof FactTable)) {
                            v.add(t.name);
                        }
                    }
                }
            }
            return v;
        }

        /* get all foreign keys in given fact table */
        private List<String> getFactTableFKs(String sname, String factTable) {
            List<String> f = new ArrayList<String>();

            if (sname == null || sname.equals("")) {
                TableTracker tracker = tables.get(factTable);

                if (tracker == null) {
                    return f;
                }

                // return a list of "schemaname -> table name -> fk col" string
                // objects if schema is not given
                boolean duplicate = tracker.count() > 1;

                for (DbTable t : tracker.namedTable) {
                    if (t instanceof FactTable) {
                        if (duplicate) {
                            for (String fk : ((FactTable) t).fks.keySet()) {
                                if (t.schemaName == null) {
                                    f.add(t.name + LEVEL_SEPARATOR + fk);
                                } else {
                                    f.add(
                                        t.schemaName
                                        + LEVEL_SEPARATOR
                                        + t.name
                                        + LEVEL_SEPARATOR
                                        + fk);
                                }
                            }
                        } else {
                            f.addAll(((FactTable) t).fks.keySet());
                        }
                    }
                }
            } else {
                DbSchema s = getSchema(sname);

                if (s == null) {
                    return f;
                }

                DbTable t = s.getTable(factTable);

                if (t == null) {
                    return f;
                }

                // return a list of "fk col name" string objects if schema is
                // given
                if (t instanceof FactTable && t.name.equals(factTable)) {
                    f.addAll(((FactTable) t).fks.keySet());
                }
            }
            return f;
        }

        private List<String> getDimensionTables(
            String sname, String factTable)
        {
            List<String> f = new ArrayList<String>();

            if (sname == null || sname.equals("")) {
                TableTracker tracker = tables.get(factTable);

                if (tracker == null) {
                    return f;
                }

                // return a list of "schemaname -> table name -> fk col" string
                // objects if schema is not given
                boolean duplicate = tracker.count() > 1;

                for (DbTable t : tracker.namedTable) {
                    if (t instanceof FactTable) {
                        if (duplicate) {
                            for (String fkt : ((FactTable) t).fks.values()) {
                                if (t.schemaName == null) {
                                    f.add(t.name + LEVEL_SEPARATOR + fkt);
                                } else {
                                    f.add(
                                        t.schemaName
                                        + LEVEL_SEPARATOR
                                        + t.name
                                        + LEVEL_SEPARATOR
                                        + fkt);
                                }
                            }
                        } else {
                            f.addAll(((FactTable) t).fks.keySet());
                        }
                    }
                }
            } else {
                DbSchema s = getSchema(sname);

                if (s == null) {
                    return f;
                }

                DbTable t = s.getTable(factTable);

                if (t == null) {
                    return f;
                }

                // return a list of "fk col name" string objects if schema is
                // given
                if (t instanceof FactTable && t.name.equals(factTable)) {
                    f.addAll(((FactTable) t).fks.values());
                }
            }
            return f;
        }

        private String getTablePK(String sname, String tableName) {
            if (sname == null || sname.equals("")) {
                TableTracker tracker = tables.get(tableName);

                if (tracker == null) {
                    return null;
                }

                // return a list of "schemaname -> table name ->
                // dimension table name" string objects if schema is not given
                return tracker.namedTable.get(0).pk;
            } else {
                DbTable t = getTable(sname, tableName);

                if (t == null) {
                    return null;
                }

                return t.pk;
            }
        }

        private List<String> getAllColumns(String sname, String tableName) {
            List<String> f = new ArrayList<String>();

            if (sname == null || sname.equals("")) {
                TableTracker tracker = tables.get(tableName);

                if (tracker == null) {
                    return f;
                }

                // return a list of "schemaname -> table name -> cols"
                // string objects if schema is not given
                boolean duplicate = tracker.count() > 1;

                for (DbTable t : tracker.namedTable) {
                    for (Map.Entry<String, DbColumn> c : t.colsDataType
                        .entrySet())
                    {
                        StringBuffer sb = new StringBuffer();

                        if (t.schemaName != null && !duplicate) {
                            sb.append(t.schemaName).append(LEVEL_SEPARATOR);
                        }
                        sb.append(t.name)
                            .append(LEVEL_SEPARATOR)
                            .append(c.getKey())
                            .append(" - ")
                            .append(c.getValue().displayType());

                        f.add(sb.toString());
                    }
                }
            } else {
                DbTable t = getTable(sname, tableName);

                if (t == null) {
                    return f;
                }
                // return a list of "col name" string objects if schema is given
                f.addAll(t.colsDataType.keySet());
            }
            return f;
        }

        private List<DbColumn> getAllDbColumns(String sname, String tableName) {
            List<DbColumn> f = new ArrayList<DbColumn>();

            if (sname == null || sname.equals("")) {
                TableTracker tracker = tables.get(tableName);

                if (tracker == null) {
                    return f;
                }

                for (DbTable t : tracker.namedTable) {
                    for (Map.Entry<String, DbColumn> c : t.colsDataType
                        .entrySet())
                    {
                        f.add(c.getValue());
                    }
                }
            } else {
                DbTable t = getTable(sname, tableName);

                if (t == null) {
                    return f;
                }

                for (Map.Entry<String, DbColumn> c : t.colsDataType
                    .entrySet())
                {
                    f.add(c.getValue());
                }
            }
            return f;
        }

        private int getColumnDataType(
            String sname, String tableName, String colName)
        {
            DbColumn result = getColumnDefinition(sname, tableName, colName);

            if (result == null) {
                return -1;
            }

            return result.dataType;
        }

        private DbColumn getColumnDefinition(
            String sname, String tableName, String colName)
        {
            DbTable t = getTable(sname, tableName);

            if (t == null) {
                return null;
            }
            return t.colsDataType.get(colName);
        }
    }

    class DbSchema {
        String name;
        /**
         * ordered collection, allows duplicates and null
         */
        final Map<String, DbTable> tables = new TreeMap<String, DbTable>();

        private DbTable getTable(String tableName) {
            return tables.get(tableName);
        }

        private void addDbTable(DbTable dbt) {
            tables.put(dbt.name, dbt);
        }
    }

    public class DbColumn {
        public String name;
        public int dataType;
        public String typeName;
        public int columnSize;
        public int decimalDigits;

        public String displayType() {
            StringBuffer sb = new StringBuffer();
            switch (dataType) {
            case Types.ARRAY:
                sb.append("ARRAY(" + columnSize + ")");
                break;
            case Types.BIGINT:
                sb.append("BIGINT");
                break;
            case Types.BINARY:
                sb.append("BINARY(" + columnSize + ")");
                break;
            case Types.BLOB:
                sb.append("BLOB(" + columnSize + ")");
                break;
            case Types.BIT:
                sb.append("BIT");
                break;
            case Types.BOOLEAN:
                sb.append("BOOLEAN");
                break;
            case Types.CHAR:
                sb.append("CHAR");
                break;
            case Types.CLOB:
                sb.append("CLOB(" + columnSize + ")");
                break;
            case Types.DATE:
                sb.append("DATE");
                break;
            case Types.DECIMAL:
                sb.append("DECIMAL(" + columnSize + ", " + decimalDigits + ")");
                break;
            case Types.DISTINCT:
                sb.append("DISTINCT");
                break;
            case Types.DOUBLE:
                sb.append("DOUBLE(" + columnSize + ", " + decimalDigits + ")");
                break;
            case Types.FLOAT:
                sb.append("FLOAT(" + columnSize + ", " + decimalDigits + ")");
                break;
            case Types.INTEGER:
                sb.append("INTEGER(" + columnSize + ")");
                break;
            case Types.JAVA_OBJECT:
                sb.append("JAVA_OBJECT(" + columnSize + ")");
                break;
            /*
             * No Java 1.6 SQL types for now
            case Types.LONGNVARCHAR:
                sb.append("LONGNVARCHAR(" + columnSize + ")");
                break;
            case Types.LONGVARBINARY:
                sb.append("LONGVARBINARY(" + columnSize + ")");
                break;
            case Types.LONGVARCHAR:
                sb.append("LONGVARCHAR(" + columnSize + ")");
                break;
            case Types.NCHAR:
                sb.append("NCHAR(" + columnSize + ")");
                break;
            case Types.NCLOB:
                sb.append("NCLOB(" + columnSize + ")");
                break;
             */
            case Types.NULL:
                sb.append("NULL");
                break;
            case Types.NUMERIC:
                sb.append("NUMERIC(" + columnSize + ", " + decimalDigits + ")");
                break;
            /*
             * No Java 1.6 SQL types for now
            case Types.NVARCHAR:
                sb.append("NCLOB(" + columnSize + ")");
                break;
             */
            case Types.OTHER:
                sb.append("OTHER");
                break;
            case Types.REAL:
                sb.append("REAL(" + columnSize + ", " + decimalDigits + ")");
                break;
            case Types.REF:
                sb.append("REF");
                break;
            /*
             * No Java 1.6 SQL types for now
            case Types.ROWID:
                sb.append("ROWID");
                break;
             */
            case Types.SMALLINT:
                sb.append("SMALLINT(" + columnSize + ")");
                break;
            /*
             * No Java 1.6 SQL types for now
           case Types.SQLXML:
                sb.append("SQLXML(" + columnSize + ")");
                break;
             */
            case Types.STRUCT:
                sb.append("STRUCT");
                break;
            case Types.TIME:
                sb.append("TIME");
                break;
            case Types.TIMESTAMP:
                sb.append("TIMESTAMP");
                break;
            case Types.TINYINT:
                sb.append("TINYINT(" + columnSize + ")");
                break;
            case Types.VARBINARY:
                sb.append("VARBINARY(" + columnSize + ")");
                break;
            case Types.VARCHAR:
                sb.append("VARCHAR(" + columnSize + ")");
                break;
            }
            return sb.toString();
        }
    }

    class DbTable {
        String schemaName;
        String name;
        String pk;
        /**
         * sorted map key=column, value=data type of column
         */
        final Map<String, DbColumn> colsDataType =
            new TreeMap<String, DbColumn>();

        private void addColsDataType(DbColumn columnDefinition) {
            colsDataType.put(columnDefinition.name, columnDefinition);
        }

        private DbColumn getColumn(String cname) {
            return colsDataType.get(cname);
        }

        private boolean hasColumns() {
            return colsDataType.size() > 0;
        }
    }

    class FactTable extends DbTable {
        /**
         * Sorted map key = foreign key col, value=primary key table associated
         * with this fk.
         */
        final Map<String, String> fks = new TreeMap<String, String>();

        private void addFks(String fk, String pkt) {
            fks.put(fk, pkt);
        }
    }
}

// End JdbcMetaData.java
