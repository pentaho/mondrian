/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde and others
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// Copyright (C) 2006-2007 JasperSoft
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Created on April 28, 2006, 2:43 PM
 */
package mondrian.gui;

import java.util.*;
import java.sql.*;

import org.apache.log4j.Logger;

/**
 *
 * @version $Id$
 */
public class JDBCMetaData {

    private static final Logger LOGGER = Logger.getLogger(JDBCMetaData.class);

    String jdbcDriverClassName = null; //"org.postgresql.Driver"
    String jdbcConnectionUrl = null; // "jdbc:postgresql://localhost:5432/hello?user=postgres&password=post"
    String jdbcUsername = null;
    String jdbcPassword = null;
    String jdbcSchema = null;
    boolean requireSchema = false;

    Connection conn = null;
    DatabaseMetaData md = null;

    Workbench workbench;

    public static final String LEVEL_SEPARATOR = "->";

    private String errMsg = null;
    private Database db = new Database();

    public JDBCMetaData(Workbench wb, String jdbcDriverClassName,
            String jdbcConnectionUrl, String jdbcUsername,
            String jdbcPassword, String jdbcSchema, boolean requireSchema) {
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
     * tests database connection. Called from Preferences dialog button test connection
     */
    public JDBCMetaData(String jdbcDriverClassName, String jdbcConnectionUrl, String jdbcUsername, String jdbcPassword) {
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
        LOGGER.debug("JDBCMetaData: initConnection");

        try {
            if (jdbcDriverClassName == null || jdbcDriverClassName.trim().length() == 0 ||
                jdbcConnectionUrl == null || jdbcConnectionUrl.trim().length() == 0)
            {
                errMsg = getResourceConverter().getFormattedString("jdbcMetaData.blank.exception",
                        "Driver={0}\nConnection URL={1}\nUse Preferences to set Database Connection parameters first and then open a Schema",
                        new String[] { jdbcDriverClassName, jdbcConnectionUrl });
                return errMsg;
            }

            Class.forName(jdbcDriverClassName);

            if (jdbcUsername != null && jdbcUsername.length() > 0) {
                conn = DriverManager.getConnection(jdbcConnectionUrl, jdbcUsername, jdbcPassword);
            } else {
                conn = DriverManager.getConnection(jdbcConnectionUrl);
            }

            LOGGER.debug("JDBC connection OPEN");
            md = conn.getMetaData();

            db.productName      = md.getDatabaseProductName();
            db.productVersion   = md.getDatabaseProductVersion();
            db.catalogName      = conn.getCatalog();

            LOGGER.debug("Catalog name = " + db.catalogName);
            /*
            ResultSet rsd = md.getSchemas();
            while (rsd.next())
            {    System.out.println("   Schema ="+rsd.getString("TABLE_SCHEM"));
                 System.out.println("   Schema ="+rsd.getString("TABLE_CATALOG"));
            }
            rsd = md.getCatalogs();
            while (rsd.next())
                System.out.println("   Catalog ="+rsd.getString("TABLE_CAT"));
             */
            LOGGER.debug("Database Product Name: " + db.productName);
            LOGGER.debug("Database Product Version: " + db.productVersion);

            /*
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/demo","admin","admin");
             */
            LOGGER.debug("JDBCMetaData: initConnection - no error");
            return null;
        } catch (Exception e) {
            errMsg = e.getClass().getSimpleName() + " : " + e.getLocalizedMessage();
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
     *
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
        LOGGER.debug("JDBCMetaData: listAllSchemas");

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
            LOGGER.debug("Exception : Database does not support schemas." + e.getMessage());
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
        LOGGER.debug("JDBCMetaData: setAllSchemas");

        ResultSet rs = null;
        boolean gotSchema = false;

        try {
            rs = md.getSchemas();
            /*
            if (true)
            throw new Exception("Schema concept not found in database");
             */

            while (rs.next()) {
                String schemaName = rs.getString("TABLE_SCHEM");
                if (inJdbcSchemas(schemaName)) {
                    DbSchema dbs = new DbSchema();
                    dbs.name = schemaName;
                    LOGGER.debug("JDBCMetaData: setAllTables - " + dbs.name);
                    setAllTables(dbs);
                    db.addDbSchema(dbs);
                    gotSchema = true;
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Exception : Database does not support schemas." + e.getMessage());
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (!gotSchema) {
            LOGGER.debug("JDBCMetaData: setAllSchemas - tables with no schema name");
            DbSchema dbs = new DbSchema();
            dbs.name = null;    //tables with no schema name
            setAllTables(dbs);
            db.addDbSchema(dbs);
        }
    }

    /* set all tables in the currently connected database */
    private void setAllTables(DbSchema dbs) {
        LOGGER.debug("JDBCMetaData: Loading schema: '" + dbs.name + "'");
        ResultSet rs = null;
        try {
            // Tables and views can be used
            try {
                rs = md.getTables(null, dbs.name, null, new String[]{"TABLE", "VIEW"});
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

                DbTable dbt;

                /* Note  : Imported keys are foreign keys which are primary keys of in some other tables
                 *       : Exported keys are primary keys which are referenced as foreign keys in other tables.
                 */
                ResultSet rs_fks = md.getImportedKeys(null, dbs.name, tbname);
                try {
                    if (rs_fks.next()) {
                        dbt = new FactTable();
                        do  {
                            ((FactTable) dbt).addFks(rs_fks.getString("FKCOLUMN_NAME"),rs_fks.getString("pktable_name"));
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

    /* get the Primary key name for a given table name
     * This key may be a  composite key made of multiple columns.
     */
    private void setPKey(DbTable dbt) {
        ResultSet rs = null;
        try {
            rs = md.getPrimaryKeys(null, dbt.schemaName, dbt.name);
            /*
            while(rs.next()) {
                primKeys.add(rs.getString("COLUMN_NAME"));
            }
             **/
            if (rs.next()) {
                //===dbt.pk = rs.getString("PK_NAME");  // a column may have been given a primary key name
                dbt.pk = rs.getString("column_name");   // we need the column name which is primary key for the given table.
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
        LOGGER.debug("setColumns: <" + tableName + "> in schema <" + schemaName + ">");

        DbSchema dbs = db.getSchema(schemaName);

        if (dbs == null) {
            throw new RuntimeException("No schema with name: <" + schemaName + ">");
        }

        DbTable dbt = dbs.getTable(tableName);

        if (dbt == null) {
            throw new RuntimeException("No table with name: <" + tableName + "> in schema <" + schemaName + ">");
        }

        setColumns(dbt);

        LOGGER.debug("got " + dbt.colsDataType.size() + " columns");
    }

    /* get all columns for a given table name */
    private void setColumns(DbTable dbt) {
        if (initConnection() != null) {
            return;
        }

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
        closeConnection();
    }

/* ===================================================================================================
 *  The following functions provide an interface to JDBCMetaData class to retrieve the meta data details
 * =================================================================================================== */

    public Vector<String> getAllSchemas() {
        return db.getAllSchemas();
    }


    /* get all tables in a given schema */
    public Vector<String> getAllTables(String schemaName) {
        return db.getAllTables(schemaName);
    }

    /* get all tables in given schema minus the given table name */
    public Vector<String> getAllTables(String schemaName, String minusTable) {
        if (minusTable == null) {
            return getAllTables(schemaName);
        } else {
            Vector<String> allTablesMinusOne = new Vector<String>();
            for (String s : getAllTables(schemaName)) {
                if (s
                    .endsWith(minusTable)) {   // startsWith and endsWith cannot be compared with null argument, throws exception
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
    public Vector<String> getFactTables(String schemaName) {
        return db.getFactTables(schemaName);
    }

    /* get all possible cases of dimension tables which are linked to given fact table by foreign keys */
    public Vector<String> getDimensionTables(String schemaName, String factTable) {
        Vector<String> dimeTables = new Vector<String>();

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

    public boolean isColExists(String schemaName, String tableName, String colName) {
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
    public Vector<String> getFactTableFKs(String schemaName, String factTable) {
        Vector<String> fks = new Vector<String>();

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

    /* get all columns of given table in schema */
    public Vector<String> getAllColumns(String schemaName, String tableName) {
        Vector<String> allcols = new Vector<String>();

        if (tableName == null) {
                Vector<String> allTables = getAllTables(schemaName);

                for (int i = 0; i < allTables.size(); i++) {
                    String tab = allTables.get(i);
                    Vector<String> cols;
                    if (tab.indexOf(LEVEL_SEPARATOR) == -1) {
                        cols = getAllColumns(schemaName, tab);
                    } else {
                        String [] names = tab.split(LEVEL_SEPARATOR);
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

    // get column data type of given table and its col
    public int getColumnDataType(String schemaName, String tableName, String colName) {
        if (tableName == null || colName == null) {
            return -1;
        } else {
            if (!db.hasColumns(schemaName, tableName)) {
                setColumns(schemaName, tableName);
            }
            return db.getColumnDataType(schemaName, tableName, colName);
        }
    }

    // get column definition of given table and its col
    public DbColumn getColumnDefinition(String schemaName, String tableName, String colName) {
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
            throw new RuntimeException("need at least 2 args: driver class and jdbcUrl");
        }

        String driverClass = args[0];
        String jdbcUrl = args[1];

        String username = null;
        String password = null;

        if (args.length > 2) {

            if (args.length != 4) {
                throw new RuntimeException("need 4 args: including user name and password");
            }
            username = args[2];
            password = args[3];
        }

        JDBCMetaData sb = new JDBCMetaData(null, driverClass, jdbcUrl, username, password, "", false);

        Vector<String> foundSchemas = sb.getAllSchemas();
        System.out.println("allSchemas = " + foundSchemas);

        for (String schemaName : foundSchemas) {
            Vector<String> foundTables = sb.getAllTables(schemaName);

            if (foundTables != null && foundTables.size() > 0) {
                System.out.println("schema = " + schemaName);
                for (String tableName : foundTables) {
                    System.out.println("\t" + tableName);

                    Vector<String> foundColumns = sb.getAllColumns(schemaName, tableName);

                    for (String columnName : foundColumns) {
                        System.out.println("\t\t" + columnName);
                    }
                }
            }
        }
        //System.out.println("allTablesCols="+sb.allTablesCols);
        //System.out.println("allTablesPKs="+sb.allTablesPKs);
        //System.out.println("allFactTableDimensions="+sb.allFactTableDimensions);
        //System.out.println("getAllTables(null, part)="+sb.getAllTables(null, "part"));
        //System.out.println("sb.getColumnDataType(null, part,part_nbr)="+sb.getColumnDataType(null, "part","part_nbr"));
    }

/* ===================================================================================================
 *  class structure for storing database metadata
 * =================================================================================================== */
    class Database {
        String catalogName = ""; // database name.
        String productName = "Unknown";
        String productVersion =    "";

        // list of all schemas in database
        Map<String, DbSchema> schemas = new TreeMap<String, DbSchema>(); //ordered collection, allows duplicates and null
        Map<String, TableTracker> tables  = new TreeMap<String, TableTracker>(); // list of all tables in all schemas in database

        Vector<String> allSchemas ;

        private void addDbSchema(DbSchema dbs) {
            schemas.put(dbs.name != null ? dbs.name : "", dbs);
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
            return schemas.get(schemaName != null ? schemaName : "");
        }

        private Vector<String> getAllSchemas() {
            if (allSchemas == null) {
                allSchemas = new Vector<String>();

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
            DbSchema dbs = getSchema(schemaName);

            if (dbs != null) {
                DbTable t = dbs.getTable(tableName);

                if (t != null) {
                    return t.hasColumns();
                }
            }
            return false;
        }

        private boolean colExists(String sname, String tableName, String colName) {
            DbTable t = getTable(sname, tableName);

            if (t == null) {
                return false;
            }

            return t.getColumn(colName) != null;
        }

        private Vector<String> getAllTables(String sname) {
            return getAllTables(sname, false);
        }

        private Vector<String> getFactTables(String sname) {
            return getAllTables(sname, true);
        }

        private Vector<String> getAllTables(String sname, boolean factOnly) {
            Vector<String> v = new Vector<String>();

            if (sname == null || sname.equals("")) {
                // return a vector of "schemaname -> table name" string objects
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
                // return a vector of "tablename" string objects

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
        private Vector<String> getFactTableFKs(String sname, String factTable) {
            Vector<String> f = new Vector<String>();

            if (sname == null || sname.equals("")) {
                TableTracker tracker = tables.get(factTable);

                if (tracker == null) {
                    return f;
                }

                // return a vector of "schemaname -> table name -> fk col" string objects if schema is not given
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

                // return a vector of "fk col name" string objects if schema is given
                if (t instanceof FactTable &&
                        t.name.equals(factTable)) {
                    f.addAll(((FactTable) t).fks.keySet());
                }
            }
            return f;
        }
        private Vector<String> getDimensionTables(String sname, String factTable) {
            Vector<String> f = new Vector<String>();

            if (sname == null || sname.equals("")) {
                TableTracker tracker = tables.get(factTable);

                if (tracker == null) {
                    return f;
                }

                // return a vector of "schemaname -> table name -> fk col" string objects if schema is not given
                boolean duplicate = tracker.count() > 1;

                for (DbTable t : tracker.namedTable) {
                    if (t instanceof FactTable) {
                        if (duplicate) {
                            for (String fkt : ((FactTable) t).fks.values()) {
                                if (t.schemaName == null) {
                                    f.add(t.name + LEVEL_SEPARATOR + fkt);
                                } else {
                                    f.add(
                                        t
                                            .schemaName
                                            + LEVEL_SEPARATOR
                                            + t
                                            .name
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

                // return a vector of "fk col name" string objects if schema is given
                if (t instanceof FactTable &&
                        t.name.equals(factTable)) {
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

                // return a vector of "schemaname -> table name ->
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

        private Vector<String> getAllColumns(String sname, String tableName) {
            Vector<String> f = new Vector<String>();

            if (sname == null || sname.equals("")) {
                TableTracker tracker = tables.get(tableName);

                if (tracker == null) {
                    return f;
                }

                // return a vector of "schemaname -> table name -> cols"
                // string objects if schema is not given
                boolean duplicate = tracker.count() > 1;

                for (DbTable t : tracker.namedTable) {
                    for (Map.Entry<String, DbColumn> c : t.colsDataType.entrySet()) {
                        StringBuffer sb = new StringBuffer();

                        if (t.schemaName != null && !duplicate) {
                            sb.append(t.schemaName)
                                    .append(LEVEL_SEPARATOR);
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
                // return a vector of "col name" string objects if schema is given
                f.addAll(t.colsDataType.keySet());
            }
            return f;
        }

        private int getColumnDataType(String sname, String tableName, String colName) {
            DbColumn result = getColumnDefinition(sname, tableName, colName);

            if (result == null) {
                return -1;
            }

            return result.dataType;
        }

        private DbColumn getColumnDefinition(String sname, String tableName, String colName) {
            DbTable t = getTable(sname, tableName);

            if (t == null) {
                return null;
            }
            return t.colsDataType.get(colName);
        }
    }

    class DbSchema {
        String name;
        /** ordered collection, allows duplicates and null */
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
            case Types.NULL:
                sb.append("NULL");
                break;
            case Types.NUMERIC:
                sb.append("NUMERIC(" + columnSize + ", " + decimalDigits + ")");
                break;
            case Types.NVARCHAR:
                sb.append("NCLOB(" + columnSize + ")");
                break;
            case Types.OTHER:
                sb.append("OTHER");
                break;
            case Types.REAL:
                sb.append("REAL(" + columnSize + ", " + decimalDigits + ")");
                break;
            case Types.REF:
                sb.append("REF");
                break;
            case Types.ROWID:
                sb.append("ROWID");
                break;
            case Types.SMALLINT:
                sb.append("SMALLINT(" + columnSize + ")");
                break;
            case Types.SQLXML:
                sb.append("SQLXML(" + columnSize + ")");
                break;
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
        /** sorted map key=column, value=data type of column */
        final Map<String, DbColumn> colsDataType = new TreeMap<String, DbColumn>();

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
        /** sorted map key = foreign key col, value=primary key table associated with this fk */
        final Map<String, String> fks = new TreeMap<String, String>();

        private void addFks(String fk, String pkt) {
            fks.put(fk, pkt);
        }
    }
}
// End JDBCMetaData.java