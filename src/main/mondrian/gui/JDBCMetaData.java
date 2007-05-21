/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde and others
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

    Connection conn = null;
    DatabaseMetaData md = null;
    
    Workbench workbench;

    /* Map of Schema and its fact tables ::
     * allFactTableDimensions = [Schema1, Schema2] -> [FactTableT8, FactTable9] -> [ForeignKeys -> PrimaryKeyTable]
     *
     * Map of Schema, its tables and their Primary Keys ::
     * allTablesPKs = [Schema1, Schema2] -> [Tables -> PrimaryKey]
     *
     * Map of Schemas, its tables and their columns with their data types
     * allTablesCols = [Schema1, Schema2] -> [Table1, Table2] -> [Columns -> DataType]
     *
     * Map of schemas and their tables
     * allSchemasMap = [Schema1, Schema2] -> [Table1, Table2]
     *
     */
    private Map allFactTableDimensions = new HashMap(); //unsynchronized, permits null values and null key
    private Map allTablesPKs        = new HashMap();
    private Map allTablesCols       = new HashMap();
    private Map allSchemasMap       = new HashMap();

    private Vector allSchemas = new Vector();   // Vector of all schemas in the connected database

    private String errMsg = null;
    private Database db = new Database();

    public JDBCMetaData(Workbench wb, String jdbcDriverClassName, String jdbcConnectionUrl, String jdbcUsername, String jdbcPassword) {
        this.workbench = wb;
        this.jdbcConnectionUrl = jdbcConnectionUrl;
        this.jdbcDriverClassName = jdbcDriverClassName;
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;

        if (initConnection() == null) {
            setAllSchemas();
            closeConnection();
        }
    }

    /**
     * @return the workbench i18n converter
     */
    public I18n getResourceConverter() {
        return workbench.getResourceConverter();
    }

    /* Creates a database connection and initializes the meta data details */
    public String initConnection(){
        LOGGER.debug("JDBCMetaData: initConnection");

        try {
            if (jdbcDriverClassName == null || jdbcDriverClassName.trim().length() == 0 ||
                    jdbcConnectionUrl == null|| jdbcConnectionUrl.trim().length() == 0) {
                errMsg = getResourceConverter().getFormattedString("jdbcMetaData.blank.exception", 
                        "Driver={0}\nConnection URL={1}\nUse Preferences to set Database Connection parameters first and then open a Schema", 
                        new String[] { jdbcDriverClassName, jdbcConnectionUrl });
                return errMsg;
            }

            Class.forName(jdbcDriverClassName);

            if (jdbcUsername != null && jdbcUsername.length() > 0 &&
                jdbcPassword != null && jdbcPassword.length() > 0) {
                conn = DriverManager.getConnection(jdbcConnectionUrl, jdbcUsername, jdbcPassword);
            } else {

                conn = DriverManager.getConnection(jdbcConnectionUrl);
            }

            LOGGER.debug("JDBC connection OPEN");
            md = conn.getMetaData();

            db.productName      = md.getDatabaseProductName();
            db.productVersion   = md.getDatabaseProductVersion();
            db.catalogName      = conn.getCatalog();

            LOGGER.debug("Catalog name = "+db.catalogName);
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
            LOGGER.error("Database connection exception : "+errMsg, e);
            return errMsg;
            //e.printStackTrace();
        }
    }

    public void closeConnection() {
        try {
            conn.close();
            LOGGER.debug("JDBC connection CLOSE");
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    /* set all schemas in the currently connected database */
    private void setAllSchemas(){
        LOGGER.debug("JDBCMetaData: setAllSchemas");

        ResultSet rs = null;
        boolean gotSchema = false;

        try{
            rs = md.getSchemas();
            /*
            if (true)
            throw new Exception("Schema concept not found in database");
             */

            while(rs.next()) {
                DbSchema dbs = new DbSchema();
                dbs.name = rs.getString("TABLE_SCHEM");
                LOGGER.debug("JDBCMetaData: setAllTables - " + dbs.name);
                setAllTables(dbs);
                db.addDbSchema(dbs);
                gotSchema = true;
            }
            rs.close();
        } catch (Exception e) {
            LOGGER.debug("Exception : Database does not support schemas."+e.getMessage());
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
    private void setAllTables(DbSchema dbs){
        LOGGER.debug("JDBCMetaData: Loading schema: '" + dbs.name + "'");
        ResultSet rs = null;
        try {
            // Tables and views can be used
            rs = md.getTables(null, dbs.name, null, new String[]{"TABLE", "VIEW"});
            while(rs.next()) {
                String tbname = rs.getString("TABLE_NAME");
                DbTable dbt;

                /* Note  : Imported keys are foreign keys which are primary keys of in some other tables
                 *       : Exported keys are primary keys which are referenced as foreign keys in other tables.
                 */
                ResultSet rs_fks = md.getImportedKeys(null, dbs.name, tbname);
                if (rs_fks.next()) {
                    dbt = new FactTable();
                    do  {
                        ((FactTable) dbt).addFks(rs_fks.getString("FKCOLUMN_NAME"),rs_fks.getString("pktable_name"));
                    } while(rs_fks.next());

                } else {
                    dbt = new DbTable();
                }
                rs_fks.close();

                dbt.schemaName = dbs.name;
                dbt.name = tbname;
                setPKey(dbt);
                setColumns(dbt);
                dbs.addDbTable(dbt);
                db.addDbTable(dbt);
            }
            rs.close();
        } catch (Exception e) {
            LOGGER.error("setAllTables", e);
        }
    }

    /* get the Primary key name for a given table name
     * This key may be a  composite key made of multiple columns.
     */
    private void setPKey(DbTable dbt){
        ResultSet rs = null;
        try{
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
            rs.close();
        } catch (Exception e) {
            LOGGER.error("setPKey", e);
        }
    }

    /* get all columns for a given table name */
    private void setColumns(DbTable dbt){
        ResultSet rs = null;
        try{
            rs = md.getColumns(null, dbt.schemaName, dbt.name, null);
            while(rs.next()) {
                dbt.addColsDataType(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));
            }
            rs.close();
        } catch (Exception e) {
            LOGGER.error("setColumns", e);
        }
    }

/* ===================================================================================================
 *  The following functions provide an interface to JDBCMetaData class to retrieve the meta data details
 * =================================================================================================== */

    public Vector getAllSchemas() {
        return db.getAllSchemas();
    }


    /* get all tables in a given schema */
    public Vector getAllTables(String schemaName) {
        return db.getAllTables(schemaName);
    }

    /* get all tables in given schema minus the given table name */
    public Vector getAllTables(String schemaName, String minusTable) {

        if (minusTable == null) {
            return getAllTables(schemaName);
        } else {
            Vector allTablesMinusOne = new Vector();
            Iterator i = getAllTables(schemaName).iterator();
            while (i.hasNext()) {
                String s = (String) i.next();
                if (s.endsWith(minusTable)) {   // startsWith and endsWith cannot be compared with null argument, throws exception
                    if ((schemaName == null) || s.startsWith(schemaName) ) {
                        continue;
                    }
                }
                allTablesMinusOne.add(s);
            }
            return allTablesMinusOne;
        }
    }

    /* get all possible cases of fact tables in a schema */
    public Vector getFactTables(String schemaName) {
        return db.getFactTables(schemaName);
    }

    /* get all possible cases of dimension tables which are linked to given fact table by foreign keys */
    public Vector getDimensionTables(String schemaName, String factTable) {
        Vector dimeTables = new Vector();

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
            return db.colExists(schemaName, tableName, colName);
        }
    }

    /* get all foreign keys in given fact table */
    public Vector getFactTableFKs(String schemaName, String factTable) {
        Vector fks = new Vector();

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
    public Vector getAllColumns(String schemaName, String tableName) {
        Vector allcols = new Vector();
        
        if (tableName == null) {
                Vector allTables = getAllTables(schemaName);
            
                for (int i = 0; i < allTables.size(); i++) {
                    String tab = (String)allTables.get(i);
                    Vector cols;
                    if (tab.indexOf("->") == -1) {
                        cols = getAllColumns(schemaName, tab);
                    } else {
                        String [] names = tab.split("->");
                        cols = getAllColumns(names[0], names[1]);
                    }
                    for (int j = 0; j < cols.size(); j++) {
                        String col = (String)cols.get(j);
                        allcols.add(tab + "->"+ col);
                    }
                }
            return allcols;
        } else {
            return db.getAllColumns(schemaName, tableName);
        }
    }

    // get column data type of given table and its col
    public int getColumnDataType(String schemaName, String tableName, String colName) {
        if (tableName == null || colName==null) {
            return -1;
        } else {
            return db.getColumnDataType(schemaName, tableName, colName);
        }

    }
    public String getDbCatalogName() {
        return db.catalogName;
    }

    public String getDatabaseProductName() {
        return db.productName;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public static void main(String[] args) {
        /*
        JDBCMetaData sb = new JDBCMetaData("org.postgresql.Driver","jdbc:postgresql://localhost:5432/testdb?user=admin&password=admin");
        System.out.println("allSchemas="+sb.allSchemas);
        System.out.println("allSchemasMap="+sb.allSchemasMap);
        System.out.println("allTablesCols="+sb.allTablesCols);
        System.out.println("allTablesPKs="+sb.allTablesPKs);
        System.out.println("allFactTableDimensions="+sb.allFactTableDimensions);
        System.out.println("getAllTables(null, part)="+sb.getAllTables(null, "part"));
        System.out.println("sb.getColumnDataType(null, part,part_nbr)="+sb.getColumnDataType(null, "part","part_nbr"));
         */
        String s = "somita->namita";
        String [] p = s.split("->");
        if (LOGGER.isDebugEnabled()) {
            if (p.length >=2)
                LOGGER.debug("p0="+p[0]+", p1="+p[1]);
        }
    }

/* ===================================================================================================
 *  class structure for storing database metadata
 * =================================================================================================== */
    class Database {
        String catalogName = ""; // database name.
        String productName = "Unknown";
        String productVersion =    "";

        // list of all schemas in database
        List schemas = new ArrayList(); //ordered collection, allows duplicates and null
        List tables  = new ArrayList(); // list of all tables in all schemas in database
        Map tablesCount = new TreeMap(); // map of table names and the count of tables with this name in the database.

        Vector allSchemas ;

        private void addDbSchema(DbSchema dbs) {
            schemas.add(dbs);
        }

        private void addDbTable(DbTable dbs) {
            tables.add(dbs);
            Integer count = (Integer) tablesCount.get(dbs.name);
            if (count == null) {
                count = new Integer(1);
            } else {
                count = new Integer(count.intValue()+1);
            }
            tablesCount.put(dbs.name, count);
        }

        private Vector getAllSchemas() {
            if (allSchemas == null) {
                allSchemas = new Vector();
                if (schemas.size() > 0) {
                    Iterator i = schemas.iterator();
                    while (i.hasNext()) {
                        allSchemas.add( ((DbSchema) i.next()).name );
                    }
                }
            }
            return allSchemas;
        }

        private boolean tableExists(String sname, String tableName) {
            if (sname == null || sname.equals("")) {
                return tablesCount.containsKey(tableName);
            } else {
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            DbTable d = (DbTable) ti.next();
                            if (d.name.equals(tableName)) {
                                return true;
                            }
                        }
                        break;
                    }
                }
            }
            return false;
        }

        private boolean colExists(String sname, String tableName, String colName) {
            if (sname == null || sname.equals("")) {
                Iterator ti = tables.iterator();
                while (ti.hasNext()) {
                    DbTable t = (DbTable) ti.next();
                    if (t.name.equals(tableName)){
                        return t.colsDataType.containsKey(colName);
                    }
                }
            } else {
                // return a vector of "fk col name" string objects if schema is given
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            DbTable t = (DbTable) ti.next();
                            if (t.name.equals(tableName)){
                                return t.colsDataType.containsKey(colName);
                            }
                        }
                        break;
                    }
                }
            }

            return false;
        }

        private Vector getAllTables(String sname) {
            Vector v = new Vector();

            if (sname == null || sname.equals("")) {
                // return a vector of "schemaname -> table name" string objects
                Iterator i = tables.iterator();
                while (i.hasNext()) {
                    DbTable d = (DbTable) i.next();
                    if ( d.schemaName == null ) {
                        v.add(d.name);
                    } else {
                        v.add(d.schemaName + "->"+ d.name);
                    }
                }

            } else {
                // return a vector of "tablename" string objects
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            DbTable d = (DbTable) ti.next();
                            v.add(d.name);
                        }
                        break;
                    }
                }
            }
            return v;
        }

        private Vector getFactTables(String sname) {
            Vector f = new Vector();

            if (sname == null || sname.equals("")) {
                // return a vector of "schemaname -> table name" string objects if schema is not given
                Iterator ti = tables.iterator();
                while (ti.hasNext()) {
                    DbTable t = (DbTable) ti.next();
                    if (t instanceof FactTable){
                        if ( t.schemaName == null ) {
                            f.add(t.name);
                        } else {
                            f.add(t.schemaName + "->"+ t.name);
                        }
                    }
                }
            } else {
                // return a vector of "fact tablename" string objects if schema is given
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            Object t = ti.next();
                            if (t instanceof FactTable){
                                f.add(((FactTable)t).name);
                            }
                        }
                        break;
                    }
                }
            }

            return f;
        }

        /* get all foreign keys in given fact table */
        private Vector getFactTableFKs(String sname, String factTable) {
            Vector f = new Vector();

            if (sname == null || sname.equals("")) {
                // return a vector of "schemaname -> table name -> fk col" string objects if schema is not given
                boolean duplicate = (tablesCount.containsKey(factTable)) && (((Integer) tablesCount.get(factTable)).intValue() > 1);

                Iterator ti = tables.iterator();
                while (ti.hasNext()) {
                    DbTable t = (DbTable) ti.next();
                    if (t instanceof FactTable && t.name.equals(factTable)){
                        if (duplicate) {
                            Iterator fki = ((FactTable)t).fks.keySet().iterator();
                            while (fki.hasNext()) {
                                String fk = (String) fki.next();
                                if ( t.schemaName == null ) {
                                    f.add(t.name + "->" + fk);
                                } else {
                                    f.add(t.schemaName + "->"+ t.name + "->" + fk);
                                }
                            }
                        } else {
                            f.addAll( ((FactTable)t).fks.keySet());
                        }
                    }
                }
            } else {
                // return a vector of "fk col name" string objects if schema is given
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            DbTable t = (DbTable) ti.next();
                            if (t instanceof FactTable && t.name.equals(factTable)){
                                f.addAll(((FactTable)t).fks.keySet());
                                break;
                            }
                        }
                        break;
                    }
                }
            }
            return f;
        }

        private Vector getDimensionTables(String sname, String factTable) {
            Vector f = new Vector();

            if (sname == null || sname.equals("")) {
                // return a vector of "schemaname -> table name -> dimension table name" string objects if schema is not given
                boolean duplicate =  (tablesCount.containsKey(factTable))  &&  (((Integer) tablesCount.get(factTable)).intValue() > 1);

                Iterator ti = tables.iterator();
                while (ti.hasNext()) {
                    DbTable t = (DbTable) ti.next();
                    if (t instanceof FactTable && t.name.equals(factTable)){
                        if (duplicate) {
                            Iterator fki = ((FactTable)t).fks.values().iterator();
                            while (fki.hasNext()) {
                                String fkt = (String) fki.next();
                                if ( t.schemaName == null ) {
                                    f.add(t.name + "->" + fkt);
                                } else {
                                    f.add(t.schemaName + "->"+ t.name + "->" + fkt);
                                }
                            }
                        } else {
                            f.addAll(((FactTable)t).fks.values());
                            break;
                        }
                    }
                }
            } else {
                // return a vector of "fk col name" string objects if schema is given
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            DbTable t = (DbTable) ti.next();
                            if (t instanceof FactTable && t.name.equals(factTable)){
                                f.addAll(((FactTable)t).fks.values());
                                break;
                            }
                        }
                        break;
                    }
                }
            }
            return f;
        }

        private String getTablePK(String sname, String tableName) {

            if (sname == null || sname.equals("")) {
                // return a vector of "schemaname -> table name -> dimension table name" string objects if schema is not given
                Iterator ti = tables.iterator();
                while (ti.hasNext()) {
                    DbTable t = (DbTable) ti.next();
                    if (t.name.equals(tableName)){
                        return t.pk;
                    }
                }
            } else {
                // return a vector of "fk col name" string objects if schema is given
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            DbTable t = (DbTable) ti.next();
                            if (t.name.equals(tableName)){
                                return t.pk;
                            }
                        }
                        break;
                    }
                }
            }
            return null;
        }

        private Vector getAllColumns(String sname, String tableName) {
            Vector f = new Vector();

            if (sname == null || sname.equals("")) {
                // return a vector of "schemaname -> table name -> cols" string objects if schema is not given
                boolean duplicate =  (tablesCount.containsKey(tableName))  && (((Integer) tablesCount.get(tableName)).intValue() > 1);

                Iterator ti = tables.iterator();
                while (ti.hasNext()) {
                    DbTable t = (DbTable) ti.next();
                    if (t.name.equals(tableName)){
                        if (duplicate) {
                            Iterator ci = t.colsDataType.keySet().iterator();
                            while (ci.hasNext()) {
                                String c = (String) ci.next();
                                if ( t.schemaName == null ) {
                                    f.add(t.name + "->" + c);
                                } else {
                                    f.add(t.schemaName + "->"+ t.name + "->" + c);
                                }
                            }
                        } else {
                            f.addAll(t.colsDataType.keySet());      //display only col names
                            break;
                        }
                    }
                }
            } else {
                // return a vector of "col name" string objects if schema is given
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            DbTable t = (DbTable) ti.next();
                            if (t.name.equals(tableName)){
                                f.addAll(t.colsDataType.keySet());
                                break;
                            }
                        }
                        break;
                    }
                }
            }
            return f;
        }

        private int getColumnDataType(String sname, String tableName, String colName) {

            if (sname == null || sname.equals("")) {
                Iterator ti = tables.iterator();
                while (ti.hasNext()) {
                    DbTable t = (DbTable) ti.next();
                    if (t.name.equals(tableName)){
                        int dataType =  Integer.parseInt((String) t.colsDataType.get(colName));
                        return dataType;
                    }
                }
            } else {
                // return a vector of "fk col name" string objects if schema is given
                Iterator i = schemas.iterator();
                while (i.hasNext()) {
                    DbSchema s = (DbSchema) i.next();
                    if ( s.name.equals(sname) ) {
                        Iterator ti = s.tables.iterator();
                        while (ti.hasNext()) {
                            DbTable t = (DbTable) ti.next();
                            if (t.name.equals(tableName)){
                                int dataType =  Integer.parseInt((String) t.colsDataType.get(colName));
                                return dataType;
                            }
                        }
                        break;
                    }
                }
            }

            return -1;
        }

    }

    class DbSchema {
        String name;
        List tables = new ArrayList(); //ordered collection, allows duplicates and null

        private void addDbTable(DbTable dbt){
            tables.add(dbt);
        }
    }

    class DbTable {
        String schemaName;
        String name;
        String pk;
        Map colsDataType = new TreeMap(); // sorted map key=column, value=data type of column

        private void addColsDataType(String col, String dataType) {
            colsDataType.put(col, dataType);
        }

    }

    class FactTable extends DbTable {
        Map fks = new TreeMap(); // sorted map key = foreign key col, value=primary key table associated with this fk

        private void addFks(String fk, String pkt) {
            fks.put(fk, pkt);
        }
    }
}


// End JDBCMetaData.java
