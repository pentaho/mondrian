/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test.loader;

import mondrian.olap.MondrianResource;
import mondrian.olap.Util;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.sql.SqlQuery;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility to load the FoodMart dataset into an arbitrary JDBC database.
 *
 * <p>It is known to work for the following databases:<ul>
 *
 * <li>MySQL 3.23 using MySQL-connector/J 3.0.16
 * <p>On the command line:
 *
 * <blockquote><code>
 * $ mysqladmin create foodmart<br/>
 * $ java -cp 'classes;testclasses' mondrian.test.loader.MondrianFoodMartLoader
 *     -verbose -tables -data -indexes -jdbcDrivers=com.mysql.jdbc.Driver
 *     -outputJdbcURL=jdbc:mysql://localhost/foodmart
 * </code></blockquote>
 * </li>
 *
 * <li>MySQL 4.15 using MySQL-connector/J 3.0.16</li>
 *
 * <li>Postgres 8.0 beta using postgresql-driver-jdbc3-74-214.jar</li>
 *
 * </ul>
 *
 * @author jhyde
 * @since 23 December, 2004
 * @version $Id$
 */
public class MondrianFoodMartLoader {
    private String jdbcDrivers;
    private String jdbcURL;
    private String userName;
    private String password;
    private String inputJdbcURL;
    private String inputUserName;
    private String inputPassword;
    private String inputFile;
    private String outputDirectory;
    private boolean tables = false;
    private boolean indexes = false;
    private boolean data = false;
    private static final String nl = System.getProperty("line.separator");
    private boolean verbose = false;
    private boolean jdbcInput = false;
    private boolean jdbcOutput = false;
    private int inputBatchSize = 50;
    private Connection connection;
    private Connection inputConnection;
    private FileWriter fileOutput = null;

    private SqlQuery sqlQuery;
    private final HashMap mapTableNameToColumns = new HashMap();

    public MondrianFoodMartLoader(String[] args) {

        StringBuffer errorMessage = new StringBuffer();

        for ( int i=0; i<args.length; i++ )  {
            if (args[i].equals("-verbose")) {
                verbose = true;
            } else if (args[i].equals("-tables")) {
                tables = true;
            } else if (args[i].equals("-data")) {
                data = true;
            } else if (args[i].equals("-indexes")) {
                indexes = true;
            } else if (args[i].startsWith("-jdbcDrivers=")) {
                jdbcDrivers = args[i].substring("-jdbcDrivers=".length());
            } else if (args[i].startsWith("-outputJdbcURL=")) {
                jdbcURL = args[i].substring("-outputJdbcURL=".length());
            } else if (args[i].startsWith("-outputJdbcUser=")) {
                userName = args[i].substring("-outputJdbcUser=".length());
            } else if (args[i].startsWith("-outputJdbcPassword=")) {
                password = args[i].substring("-outputJdbcPassword=".length());
            } else if (args[i].startsWith("-inputJdbcURL=")) {
                inputJdbcURL = args[i].substring("-inputJdbcURL=".length());
            } else if (args[i].startsWith("-inputJdbcUser=")) {
                inputUserName = args[i].substring("-inputJdbcUser=".length());
            } else if (args[i].startsWith("-inputJdbcPassword=")) {
                inputPassword = args[i].substring("-inputJdbcPassword=".length());
            } else if (args[i].startsWith("-inputFile=")) {
                inputFile = args[i].substring("-inputFile=".length());
            } else if (args[i].startsWith("-outputDirectory=")) {
                outputDirectory = args[i].substring("-outputDirectory=".length());
            } else if (args[i].startsWith("-outputJdbcBatchSize=")) {
                inputBatchSize = Integer.parseInt(args[i].substring("-outputJdbcBatchSize=".length()));
            } else {
                errorMessage.append("unknown arg: " + args[i] + "\n");
            }
        }
        if (inputJdbcURL != null) {
            jdbcInput = true;
            if (inputFile != null) {
                errorMessage.append("Specified both an input JDBC connection and an input file");
            }
        }
        if (jdbcURL != null && outputDirectory == null) {
            jdbcOutput = true;
        }
        if (errorMessage.length() > 0) {
            usage();
            throw MondrianResource.instance().newMissingArg(errorMessage.toString());
        }
    }

    public void usage() {
        System.out.println("Usage: MondrianFoodMartLoader [-verbose] [-tables] [-data] [-indexes] -jdbcDrivers=<jdbcDriver> [-outputJdbcURL=<jdbcURL> [-outputJdbcUser=user] [-outputJdbcPassword=password] [-outputJdbcBatchSize=<batch size>] | -outputDirectory=<directory name>] [ [-inputJdbcURL=<jdbcURL> [-inputJdbcUser=user] [-inputJdbcPassword=password]] | [-inputfile=<file name>]]");
        System.out.println("");
        System.out.println("  <jdbcURL>         JDBC connect string for DB");
        System.out.println("  [user]            JDBC user name for DB");
        System.out.println("  [password]        JDBC password for user for DB");
        System.out.println("                    If no source DB parameters are given, assumes data comes from file");
        System.out.println("  [file name]       file containing test data - INSERT statements");
        System.out.println("                    If no input file name or input JDBC parameters are given, assume insert statements come from demo/FoodMartData.sql file");
        System.out.println("  [outputDirectory] Where FoodMartCreateTables.sql, FoodMartData.sql and FoodMartCreateIndexes.sql will be created");

        System.out.println("  <batch size>      size of JDBC batch updates - default to 50 inserts");
        System.out.println("  <jdbcDrivers>     Comma-separated list of JDBC drivers.");
        System.out.println("                    They must be on the classpath.");
        System.out.println("  -verbose          Verbose mode.");
        System.out.println("  -tables           If specified, drop and create the tables.");
        System.out.println("  -data             If specified, load the data.");
        System.out.println("  -indexes          If specified, drop and create the tables.");
    }

    public static void main(String[] args) {
        System.out.println("Starting load at: " + (new Date()));
        try {
            new MondrianFoodMartLoader(args).load();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("Finished load at: " + (new Date()));
    }

    private void load() throws Exception {
        RolapUtil.loadDrivers(jdbcDrivers);

        if (userName == null) {
            connection = DriverManager.getConnection(jdbcURL);
        } else {
            connection = DriverManager.getConnection(jdbcURL, userName, password);
        }

        if (jdbcInput) {
            if (inputUserName == null) {
                inputConnection = DriverManager.getConnection(inputJdbcURL);
            } else {
                inputConnection = DriverManager.getConnection(inputJdbcURL, inputUserName, inputPassword);
            }
        }
        final DatabaseMetaData metaData = connection.getMetaData();
        sqlQuery = new SqlQuery(metaData);
        try {
            createTables();  // This also initializes mapTableNameToColumns
            if (data) {
                if (jdbcInput) {
                    loadDataFromJdbcInput();
                } else {
                    loadDataFromFile();
                }
            }
            if (indexes) {
                createIndexes();
            }
        } finally {
            if (connection != null) {
                connection.close();
                connection = null;
            }
            if (inputConnection != null) {
                inputConnection.close();
                inputConnection = null;
            }
            if (fileOutput != null) {
                fileOutput.close();
                fileOutput = null;
            }
        }
    }

    private void loadDataFromFile() throws IOException, SQLException {
        final InputStream is = openInputStream();
        final InputStreamReader reader = new InputStreamReader(is);
        final BufferedReader bufferedReader = new BufferedReader(reader);
        final Pattern regex = Pattern.compile("INSERT INTO ([^ ]+)(.*)VALUES(.*)\\((.*)\\);");
        String line;
        int lineNumber = 0;
        int tableRowCount = 0;
        String prevTable = "";

        String[] batch = new String[inputBatchSize];
        int batchSize = 0;

        while ((line = bufferedReader.readLine()) != null) {
            ++lineNumber;
            if (line.startsWith("#")) {
                continue;
            }
            // Split the up the line. For example,
            //   INSERT INTO foo VALUES (1, 'bar');
            // would yield
            //   tableName = "foo"
            //   values = "1, 'bar'"
            final Matcher matcher = regex.matcher(line);
            if (!matcher.matches()) {
                throw MondrianResource.instance().newInvalidInsertLine(
                    new Integer(lineNumber), line);
            }
            final String tableName = matcher.group(1); // e.g. "foo"
            final String values = matcher.group(2); // e.g. "1, 'bar'"
            Util.discard(values); // Not needed now

            // If table just changed, flush the previous batch.
            if (!tableName.equals(prevTable)) {
                if (!prevTable.equals("")) {
                    System.out.println("Table " + prevTable +
                        ": loaded " + tableRowCount + " rows.");
                }
                tableRowCount = 0;
                writeBatch(batch, batchSize);
                batchSize = 0;
                prevTable = tableName;
            }

            // remove trailing ';'
            assert line.endsWith(";");
            line = line.substring(0, line.length() - 1);

            // this database represents booleans as integers
            if (sqlQuery.isMySQL()) {
                line = line.replaceAll("false", "0")
                    .replaceAll("true", "1");
            }

            ++tableRowCount;

            batch[batchSize++] = line;
            if (batchSize >= inputBatchSize) {
                writeBatch(batch, batchSize);
                batchSize = 0;
            }
        }
        // Print summary of the final table.
        if (!prevTable.equals("")) {
            System.out.println("Table " + prevTable +
                ": loaded " + tableRowCount + " rows.");
            tableRowCount = 0;
            writeBatch(batch, batchSize);
            batchSize = 0;
        }
    }

    private void loadDataFromJdbcInput() throws Exception {
        if (outputDirectory != null) {
            fileOutput = new FileWriter(new File(outputDirectory, "createData.sql"));
        }

        /*
         * For each input table,
         *  read specified columns for all rows in the input connection
         *
         * For each row, insert a row
         */

        for (Iterator it = mapTableNameToColumns.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry tableEntry = (Map.Entry) it.next();
            int rowsAdded = loadTable((String) tableEntry.getKey(), (Column[]) tableEntry.getValue());
            System.out.println("Table " + (String) tableEntry.getKey() +
                    ": loaded " + rowsAdded + " rows.");
        }

        if (outputDirectory != null) {
            fileOutput.close();
        }
    }

    private int loadTable(String name, Column[] columns) throws Exception {
        int rowsAdded = 0;
        StringBuffer buf = new StringBuffer();

        buf.append("select ");
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(quoteId(column.name));
        }
        buf.append(" from ")
            .append(quoteId(name));
        String ddl = buf.toString();
        Statement statement = inputConnection.createStatement();
        if (verbose) {
            System.out.println("Input table SQL: " + ddl);
        }
        ResultSet rs = statement.executeQuery(ddl);

        String[] batch = new String[inputBatchSize];
        int batchSize = 0;

        while (rs.next()) {
            /*
             * Get a batch of insert statements, then save a batch
             */

            batch[batchSize++] = createInsertStatement(rs, name, columns);
            if (batchSize >= inputBatchSize) {
                rowsAdded += writeBatch(batch, batchSize);
                batchSize = 0;
            }
        }

        if (batchSize > 0) {
            rowsAdded += writeBatch(batch, batchSize);
        }

        return rowsAdded;
    }

    private String createInsertStatement(ResultSet rs, String name, Column[] columns) throws Exception {
        StringBuffer buf = new StringBuffer();

        buf.append("INSERT INTO ")
            .append(quoteId(name))
            .append(" ( ");
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(quoteId(column.name));
        }
        buf.append(" ) VALUES(");
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(columnValue(rs, column));
        }
        buf.append(" )");
        return buf.toString();
    }

    private int writeBatch(String[] batch, int batchSize) throws IOException, SQLException {
        if (outputDirectory != null) {
            for (int i = 0; i < batchSize; i++) {
                fileOutput.write(batch[i]);
                fileOutput.write(";\n");
            }
        } else {
            connection.setAutoCommit(false);
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
                int [] updateCounts = stmt.executeBatch();
                int updates = 0;
                for (int i = 0; i < updateCounts.length; updates += updateCounts[i], i++) {
                    if (updateCounts[i] == 0) {
                        System.out.println("Error in SQL: " + batch[i]);
                    }
                }
                if (updates < batchSize) {
                    throw new RuntimeException("Failed to execute batch: " + batchSize + " versus " + updates);
                }
            }
            stmt.close();
            connection.setAutoCommit(true);
        }
        return batchSize;
    }

    private FileInputStream openInputStream() {
        final File file = (inputFile != null) ? new File(inputFile) : new File("demo", "FoodMartData.sql");
        if (file.exists()) {
            try {
                return new FileInputStream(file);
            } catch (FileNotFoundException e) {
            }
        } else {
            System.out.println("No input file: " + file);
        }
        return null;
    }

    private void createIndexes() throws Exception {
        if (outputDirectory != null) {
            fileOutput = new FileWriter(new File(outputDirectory, "createIndexes.sql"));
        }

        createIndex(true, "account", "i_account_id", new String[] {"account_id"});
        createIndex(false, "account", "i_account_parent", new String[] {"account_parent"});
        createIndex(true, "category", "i_category_id", new String[] {"category_id"});
        createIndex(false, "category", "i_category_parent", new String[] {"category_parent"});
        createIndex(true, "currency", "i_currency", new String[] {"currency_id", "date"});
        createIndex(false, "customer", "i_customer_account_num", new String[] {"account_num"});
        createIndex(false, "customer", "i_customer_fname", new String[] {"fname"});
        createIndex(false, "customer", "i_customer_lname", new String[] {"lname"});
        createIndex(false, "customer", "i_customer_children_at_home", new String[] {"num_children_at_home"});
        createIndex(true, "customer", "i_customer_id", new String[] {"customer_id"});
        createIndex(false, "customer", "i_customer_postal_code", new String[] {"postal_code"});
        createIndex(false, "customer", "i_customer_region_id", new String[] {"customer_region_id"});
        createIndex(true, "department", "i_department_id", new String[] {"department_id"});
        createIndex(true, "employee", "i_employee_id", new String[] {"employee_id"});
        createIndex(false, "employee", "i_employee_department_id", new String[] {"department_id"});
        createIndex(false, "employee", "i_employee_store_id", new String[] {"store_id"});
        createIndex(false, "employee", "i_employee_supervisor_id", new String[] {"supervisor_id"});
        createIndex(true, "employee_closure", "i_employee_closure", new String[] {"supervisor_id", "employee_id"});
        createIndex(false, "employee_closure", "i_employee_closure_emp", new String[] {"employee_id"});
        createIndex(false, "expense_fact", "i_expense_store_id", new String[] {"store_id"});
        createIndex(false, "expense_fact", "i_expense_account_id", new String[] {"account_id"});
        createIndex(false, "expense_fact", "i_expense_time_id", new String[] {"time_id"});
        createIndex(false, "inventory_fact_1997", "i_inv_1997_product_id", new String[] {"product_id"});
        createIndex(false, "inventory_fact_1997", "i_inv_1997_store_id", new String[] {"store_id"});
        createIndex(false, "inventory_fact_1997", "i_inv_1997_time_id", new String[] {"time_id"});
        createIndex(false, "inventory_fact_1997", "i_inv_1997_warehouse_id", new String[] {"warehouse_id"});
        createIndex(false, "inventory_fact_1998", "i_inv_1998_product_id", new String[] {"product_id"});
        createIndex(false, "inventory_fact_1998", "i_inv_1998_store_id", new String[] {"store_id"});
        createIndex(false, "inventory_fact_1998", "i_inv_1998_time_id", new String[] {"time_id"});
        createIndex(false, "inventory_fact_1998", "i_inv_1998_warehouse_id", new String[] {"warehouse_id"});
        createIndex(true, "position", "i_position_id", new String[] {"position_id"});
        createIndex(false, "product", "i_product_brand_name", new String[] {"brand_name"});
        createIndex(true, "product", "i_product_id", new String[] {"product_id"});
        createIndex(false, "product", "i_product_class_id", new String[] {"product_class_id"});
        createIndex(false, "product", "i_product_name", new String[] {"product_name"});
        createIndex(false, "product", "i_product_SKU", new String[] {"SKU"});
        createIndex(true, "promotion", "i_promotion_id", new String[] {"promotion_id"});
        createIndex(false, "promotion", "i_promotion_district_id", new String[] {"promotion_district_id"});
        createIndex(true, "reserve_employee", "i_reserve_employee_id", new String[] {"employee_id"});
        createIndex(false, "reserve_employee", "i_reserve_employee_dept_id", new String[] {"department_id"});
        createIndex(false, "reserve_employee", "i_reserve_employee_store_id", new String[] {"store_id"});
        createIndex(false, "reserve_employee", "i_reserve_employee_super_id", new String[] {"supervisor_id"});
        createIndex(false, "sales_fact_1997", "i_sales_1997_customer_id", new String[] {"customer_id"});
        createIndex(false, "sales_fact_1997", "i_sales_1997_product_id", new String[] {"product_id"});
        createIndex(false, "sales_fact_1997", "i_sales_1997_promotion_id", new String[] {"promotion_id"});
        createIndex(false, "sales_fact_1997", "i_sales_1997_store_id", new String[] {"store_id"});
        createIndex(false, "sales_fact_1997", "i_sales_1997_time_id", new String[] {"time_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sales_dec_1998_customer_id", new String[] {"customer_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sales_dec_1998_product_id", new String[] {"product_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sales_dec_1998_promotion_id", new String[] {"promotion_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sales_dec_1998_store_id", new String[] {"store_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sales_dec_1998_time_id", new String[] {"time_id"});
        createIndex(false, "sales_fact_1998", "i_sales_1998_customer_id", new String[] {"customer_id"});
        createIndex(false, "sales_fact_1998", "i_sales_1998_product_id", new String[] {"product_id"});
        createIndex(false, "sales_fact_1998", "i_sales_1998_promotion_id", new String[] {"promotion_id"});
        createIndex(false, "sales_fact_1998", "i_sales_1998_store_id", new String[] {"store_id"});
        createIndex(false, "sales_fact_1998", "i_sales_1998_time_id", new String[] {"time_id"});
        createIndex(true, "store", "i_store_id", new String[] {"store_id"});
        createIndex(false, "store", "i_store_region_id", new String[] {"region_id"});
        if (outputDirectory != null) {
            fileOutput.close();
        }
    }

    private void createIndex(
        boolean isUnique,
        String tableName,
        String indexName,
        String[] columnNames)
    {
        try {
            StringBuffer buf = new StringBuffer();
            buf.append(isUnique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ")
                .append(quoteId(indexName)).append(" ON ")
                .append(quoteId(tableName)).append(" (");
            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(quoteId(columnName));
            }
            buf.append(")");
            final String ddl = buf.toString();
            if (verbose) {
                System.out.println(ddl);
            }
            if (jdbcOutput) {
                final Statement statement = connection.createStatement();
                statement.execute(ddl);
            } else {
                fileOutput.write(ddl);
                fileOutput.write(";\n");
            }
        } catch (Exception e) {
            throw MondrianResource.instance().newCreateIndexFailed(indexName,
                tableName, e);
        }
    }

    /**
     * Also initializes mapTableNameToColumns
     *
     * @throws Exception
     */
    private void createTables() throws Exception  {
        if (outputDirectory != null) {
            fileOutput = new FileWriter(new File(outputDirectory, "createTables.sql"));
        }

        String booleanColumnType = "BIT";
        if (sqlQuery.isPostgres()) {
            booleanColumnType = "BOOLEAN";
        }
        createTable("sales_fact_1997", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", "NOT NULL"),
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("promotion_id", "INTEGER", "NOT NULL"),
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_sales", "DECIMAL(10,2)", "NOT NULL"),
          new Column("store_cost", "DECIMAL(10,2)", "NOT NULL"),
          new Column("unit_sales", "BIGINT", "NOT NULL"),
        });
        createTable("sales_fact_1998", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", "NOT NULL"),
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("promotion_id", "INTEGER", "NOT NULL"),
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_sales", "DECIMAL(10,2)", "NOT NULL"),
          new Column("store_cost", "DECIMAL(10,2)", "NOT NULL"),
          new Column("unit_sales", "BIGINT", "NOT NULL"),
        });
        createTable("sales_fact_dec_1998", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", "NOT NULL"),
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("promotion_id", "INTEGER", "NOT NULL"),
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_sales", "DECIMAL(10,2)", "NOT NULL"),
          new Column("store_cost", "DECIMAL(10,2)", "NOT NULL"),
          new Column("unit_sales", "BIGINT", "NOT NULL"),
        });
        createTable("inventory_fact_1997", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", ""),
          new Column("warehouse_id", "INTEGER", ""),
          new Column("store_id", "INTEGER", ""),
          new Column("units_ordered", "INTEGER", ""),
          new Column("units_shipped", "INTEGER", ""),
          new Column("warehouse_sales", "DECIMAL(10,2)", ""),
          new Column("warehouse_cost", "DECIMAL(10,2)", ""),
          new Column("supply_time", "SMALLINT", ""),
          new Column("store_invoice", "DECIMAL(10,2)", ""),
        });
        createTable("inventory_fact_1998", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", ""),
          new Column("warehouse_id", "INTEGER", ""),
          new Column("store_id", "INTEGER", ""),
          new Column("units_ordered", "INTEGER", ""),
          new Column("units_shipped", "INTEGER", ""),
          new Column("warehouse_sales", "DECIMAL(10,2)", ""),
          new Column("warehouse_cost", "DECIMAL(10,2)", ""),
          new Column("supply_time", "SMALLINT", ""),
          new Column("store_invoice", "DECIMAL(10,2)", ""),
        });
        createTable("account", new Column[] {
          new Column("account_id", "INTEGER", "NOT NULL"),
          new Column("account_parent", "INTEGER", ""),
          new Column("account_description", "VARCHAR(30)", ""),
          new Column("account_type", "VARCHAR(30)", "NOT NULL"),
          new Column("account_rollup", "VARCHAR(30)", "NOT NULL"),
          new Column("Custom_Members", "VARCHAR(255)", ""),
        });
        createTable("category", new Column[] {
          new Column("category_id", "VARCHAR(30)", "NOT NULL"),
          new Column("category_parent", "VARCHAR(30)", ""),
          new Column("category_description", "VARCHAR(30)", "NOT NULL"),
          new Column("category_rollup", "VARCHAR(30)", ""),
        });
        createTable("currency", new Column[] {
          new Column("currency_id", "INTEGER", "NOT NULL"),
          new Column("date", "DATE", "NOT NULL"),
          new Column("currency", "VARCHAR(30)", "NOT NULL"),
          new Column("conversion_ratio", "DECIMAL(10,4)", "NOT NULL"),
        });
        createTable("customer", new Column[] {
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("account_num", "BIGINT", "NOT NULL"),
          new Column("lname", "VARCHAR(30)", "NOT NULL"),
          new Column("fname", "VARCHAR(30)", "NOT NULL"),
          new Column("mi", "VARCHAR(30)", ""),
          new Column("address1", "VARCHAR(30)", ""),
          new Column("address2", "VARCHAR(30)", ""),
          new Column("address3", "VARCHAR(30)", ""),
          new Column("address4", "VARCHAR(30)", ""),
          new Column("city", "VARCHAR(30)", ""),
          new Column("state_province", "VARCHAR(30)", ""),
          new Column("postal_code", "VARCHAR(30)", "NOT NULL"),
          new Column("country", "VARCHAR(30)", "NOT NULL"),
          new Column("customer_region_id", "INTEGER", "NOT NULL"),
          new Column("phone1", "VARCHAR(30)", "NOT NULL"),
          new Column("phone2", "VARCHAR(30)", "NOT NULL"),
          new Column("birthdate", "DATE", "NOT NULL"),
          new Column("marital_status", "VARCHAR(30)", "NOT NULL"),
          new Column("yearly_income", "VARCHAR(30)", "NOT NULL"),
          new Column("gender", "VARCHAR(30)", "NOT NULL"),
          new Column("total_children", "SMALLINT", "NOT NULL"),
          new Column("num_children_at_home", "SMALLINT", "NOT NULL"),
          new Column("education", "VARCHAR(30)", "NOT NULL"),
          new Column("date_accnt_opened", "DATE", "NOT NULL"),
          new Column("member_card", "VARCHAR(30)", ""),
          new Column("occupation", "VARCHAR(30)", ""),
          new Column("houseowner", "VARCHAR(30)", ""),
          new Column("num_cars_owned", "INTEGER", ""),
        });
        createTable("days", new Column[] {
          new Column("day", "INTEGER", "NOT NULL"),
          new Column("week_day", "VARCHAR(30)", "NOT NULL"),
        });
        createTable("department", new Column[] {
          new Column("department_id", "INTEGER", "NOT NULL"),
          new Column("department_description", "VARCHAR(30)", "NOT NULL"),
        });
        createTable("employee", new Column[] {
          new Column("employee_id", "INTEGER", "NOT NULL"),
          new Column("full_name", "VARCHAR(30)", "NOT NULL"),
          new Column("first_name", "VARCHAR(30)", "NOT NULL"),
          new Column("last_name", "VARCHAR(30)", "NOT NULL"),
          new Column("position_id", "INTEGER", ""),
          new Column("position_title", "VARCHAR(30)", ""),
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("department_id", "INTEGER", "NOT NULL"),
          new Column("birth_date", "DATE", "NOT NULL"),
          new Column("hire_date", "TIMESTAMP", ""),
          new Column("end_date", "TIMESTAMP", ""),
          new Column("salary", "DECIMAL(10,2)", "NOT NULL"),
          new Column("supervisor_id", "INTEGER", ""),
          new Column("education_level", "VARCHAR(30)", "NOT NULL"),
          new Column("marital_status", "VARCHAR(30)", "NOT NULL"),
          new Column("gender", "VARCHAR(30)", "NOT NULL"),
          new Column("management_role", "VARCHAR(30)", ""),
        });
        createTable("employee_closure", new Column[] {
          new Column("employee_id", "INTEGER", "NOT NULL"),
          new Column("supervisor_id", "INTEGER", "NOT NULL"),
          new Column("distance", "INTEGER", ""),
        });
        createTable("expense_fact", new Column[] {
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("account_id", "INTEGER", "NOT NULL"),
          new Column("exp_date", "TIMESTAMP", "NOT NULL"),
          new Column("time_id", "INTEGER", "NOT NULL"),
          new Column("category_id", "VARCHAR(30)", "NOT NULL"),
          new Column("currency_id", "INTEGER", "NOT NULL"),
          new Column("amount", "DECIMAL(10,2)", "NOT NULL"),
        });
        createTable("position", new Column[] {
          new Column("position_id", "INTEGER", "NOT NULL"),
          new Column("position_title", "VARCHAR(30)", "NOT NULL"),
          new Column("pay_type", "VARCHAR(30)", "NOT NULL"),
          new Column("min_scale", "DECIMAL(10,2)", "NOT NULL"),
          new Column("max_scale", "DECIMAL(10,2)", "NOT NULL"),
          new Column("management_role", "VARCHAR(30)", "NOT NULL"),
        });
        createTable("product", new Column[] {
          new Column("product_class_id", "INTEGER", "NOT NULL"),
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("brand_name", "VARCHAR(60)", ""),
          new Column("product_name", "VARCHAR(60)", "NOT NULL"),
          new Column("SKU", "BIGINT", "NOT NULL"),
          new Column("SRP", "DECIMAL(10,2)", ""),
          new Column("gross_weight", "REAL", ""),
          new Column("net_weight", "REAL", ""),
          new Column("recyclable_package", booleanColumnType, ""),
          new Column("low_fat", booleanColumnType, ""),
          new Column("units_per_case", "SMALLINT", ""),
          new Column("cases_per_pallet", "SMALLINT", ""),
          new Column("shelf_width", "REAL", ""),
          new Column("shelf_height", "REAL", ""),
          new Column("shelf_depth", "REAL", ""),
        });
        createTable("product_class", new Column[] {
          new Column("product_class_id", "INTEGER", "NOT NULL"),
          new Column("product_subcategory", "VARCHAR(30)", ""),
          new Column("product_category", "VARCHAR(30)", ""),
          new Column("product_department", "VARCHAR(30)", ""),
          new Column("product_family", "VARCHAR(30)", ""),
        });
        createTable("promotion", new Column[] {
          new Column("promotion_id", "INTEGER", "NOT NULL"),
          new Column("promotion_district_id", "INTEGER", ""),
          new Column("promotion_name", "VARCHAR(30)", ""),
          new Column("media_type", "VARCHAR(30)", ""),
          new Column("cost", "BIGINT", ""),
          new Column("start_date", "TIMESTAMP", ""),
          new Column("end_date", "TIMESTAMP", ""),
        });
        createTable("region", new Column[] {
          new Column("region_id", "INTEGER", "NOT NULL"),
          new Column("sales_city", "VARCHAR(30)", ""),
          new Column("sales_state_province", "VARCHAR(30)", ""),
          new Column("sales_district", "VARCHAR(30)", ""),
          new Column("sales_region", "VARCHAR(30)", ""),
          new Column("sales_country", "VARCHAR(30)", ""),
          new Column("sales_district_id", "INTEGER", ""),
        });
        createTable("reserve_employee", new Column[] {
          new Column("employee_id", "INTEGER", "NOT NULL"),
          new Column("full_name", "VARCHAR(30)", "NOT NULL"),
          new Column("first_name", "VARCHAR(30)", "NOT NULL"),
          new Column("last_name", "VARCHAR(30)", "NOT NULL"),
          new Column("position_id", "INTEGER", ""),
          new Column("position_title", "VARCHAR(30)", ""),
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("department_id", "INTEGER", "NOT NULL"),
          new Column("birth_date", "TIMESTAMP", "NOT NULL"),
          new Column("hire_date", "TIMESTAMP", ""),
          new Column("end_date", "TIMESTAMP", ""),
          new Column("salary", "DECIMAL(10,2)", "NOT NULL"),
          new Column("supervisor_id", "INTEGER", ""),
          new Column("education_level", "VARCHAR(30)", "NOT NULL"),
          new Column("marital_status", "VARCHAR(30)", "NOT NULL"),
          new Column("gender", "VARCHAR(30)", "NOT NULL"),
        });
        createTable("salary", new Column[] {
          new Column("pay_date", "TIMESTAMP", "NOT NULL"),
          new Column("employee_id", "INTEGER", "NOT NULL"),
          new Column("department_id", "INTEGER", "NOT NULL"),
          new Column("currency_id", "INTEGER", "NOT NULL"),
          new Column("salary_paid", "DECIMAL(10,2)", "NOT NULL"),
          new Column("overtime_paid", "DECIMAL(10,2)", "NOT NULL"),
          new Column("vacation_accrued", "INTEGER", "NOT NULL"),
          new Column("vacation_used", "INTEGER", "NOT NULL"),
        });
        createTable("store", new Column[] {
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_type", "VARCHAR(30)", ""),
          new Column("region_id", "INTEGER", ""),
          new Column("store_name", "VARCHAR(30)", ""),
          new Column("store_number", "BIGINT", ""),
          new Column("store_street_address", "VARCHAR(30)", ""),
          new Column("store_city", "VARCHAR(30)", ""),
          new Column("store_state", "VARCHAR(30)", ""),
          new Column("store_postal_code", "VARCHAR(30)", ""),
          new Column("store_country", "VARCHAR(30)", ""),
          new Column("store_manager", "VARCHAR(30)", ""),
          new Column("store_phone", "VARCHAR(30)", ""),
          new Column("store_fax", "VARCHAR(30)", ""),
          new Column("first_opened_date", "TIMESTAMP", ""),
          new Column("last_remodel_date", "TIMESTAMP", ""),
          new Column("store_sqft", "BIGINT", ""),
          new Column("grocery_sqft", "BIGINT", ""),
          new Column("frozen_sqft", "BIGINT", ""),
          new Column("meat_sqft", "BIGINT", ""),
          new Column("coffee_bar", booleanColumnType, ""),
          new Column("video_store", booleanColumnType, ""),
          new Column("salad_bar", booleanColumnType, ""),
          new Column("prepared_food", booleanColumnType, ""),
          new Column("florist", booleanColumnType, ""),
        });
        createTable("store_ragged", new Column[] {
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_type", "VARCHAR(30)", ""),
          new Column("region_id", "INTEGER", ""),
          new Column("store_name", "VARCHAR(30)", ""),
          new Column("store_number", "BIGINT", ""),
          new Column("store_street_address", "VARCHAR(30)", ""),
          new Column("store_city", "VARCHAR(30)", ""),
          new Column("store_state", "VARCHAR(30)", ""),
          new Column("store_postal_code", "VARCHAR(30)", ""),
          new Column("store_country", "VARCHAR(30)", ""),
          new Column("store_manager", "VARCHAR(30)", ""),
          new Column("store_phone", "VARCHAR(30)", ""),
          new Column("store_fax", "VARCHAR(30)", ""),
          new Column("first_opened_date", "TIMESTAMP", ""),
          new Column("last_remodel_date", "TIMESTAMP", ""),
          new Column("store_sqft", "BIGINT", ""),
          new Column("grocery_sqft", "BIGINT", ""),
          new Column("frozen_sqft", "BIGINT", ""),
          new Column("meat_sqft", "BIGINT", ""),
          new Column("coffee_bar", booleanColumnType, ""),
          new Column("video_store", booleanColumnType, ""),
          new Column("salad_bar", booleanColumnType, ""),
          new Column("prepared_food", booleanColumnType, ""),
          new Column("florist", booleanColumnType, ""),
        });
        createTable("time_by_day", new Column[] {
          new Column("time_id", "INTEGER", "NOT NULL"),
          new Column("the_date", "TIMESTAMP", ""),
          new Column("the_day", "VARCHAR(30)", ""),
          new Column("the_month", "VARCHAR(30)", ""),
          new Column("the_year", "SMALLINT", ""),
          new Column("day_of_month", "SMALLINT", ""),
          new Column("week_of_year", "INTEGER", ""),
          new Column("month_of_year", "SMALLINT", ""),
          new Column("quarter", "VARCHAR(30)", ""),
          new Column("fiscal_period", "VARCHAR(30)", ""),
        });
        createTable("warehouse", new Column[] {
          new Column("warehouse_id", "INTEGER", "NOT NULL"),
          new Column("warehouse_class_id", "INTEGER", ""),
          new Column("stores_id", "INTEGER", ""),
          new Column("warehouse_name", "VARCHAR(60)", ""),
          new Column("wa_address1", "VARCHAR(30)", ""),
          new Column("wa_address2", "VARCHAR(30)", ""),
          new Column("wa_address3", "VARCHAR(30)", ""),
          new Column("wa_address4", "VARCHAR(30)", ""),
          new Column("warehouse_city", "VARCHAR(30)", ""),
          new Column("warehouse_state_province", "VARCHAR(30)", ""),
          new Column("warehouse_postal_code", "VARCHAR(30)", ""),
          new Column("warehouse_country", "VARCHAR(30)", ""),
          new Column("warehouse_owner_name", "VARCHAR(30)", ""),
          new Column("warehouse_phone", "VARCHAR(30)", ""),
          new Column("warehouse_fax", "VARCHAR(30)", ""),
        });
        createTable("warehouse_class", new Column[] {
          new Column("warehouse_class_id", "INTEGER", "NOT NULL"),
          new Column("description", "VARCHAR(30)", ""),
        });
        if (outputDirectory != null) {
            fileOutput.close();
        }
    }

    private void createTable(String name, Column[] columns) {
        try {
            // Define the table.
            mapTableNameToColumns.put(name, columns);
            if (!tables) {
                if (data) {
                    // We're going to load the data without [re]creating
                    // the table, so let's remove the data.
                    final Statement statement = connection.createStatement();
                    try {
                        statement.execute("DELETE FROM " + quoteId(name));
                    } catch (SQLException e) {
                        throw MondrianResource.instance().newCreateTableFailed(name, e);
                    }
                }
                return;
            }
            StringBuffer buf = new StringBuffer();
            buf.append("CREATE TABLE ").append(quoteId(name)).append("(");
            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                if (i > 0) {
                    buf.append(",");
                }
                buf.append(nl);
                buf.append("    ").append(quoteId(column.name)).append(" ")
                    .append(column.type);
                if (!column.constraint.equals("")) {
                    buf.append(" ").append(column.constraint);
                }
            }
            buf.append(")");
            final String ddl = buf.toString();

            if (verbose) {
                System.out.println(ddl);
            }
            if (jdbcOutput) {
                final Statement statement = connection.createStatement();
                try {
                    statement.execute("DROP TABLE " + quoteId(name));
                } catch (SQLException e) {
                    // ignore 'table does not exist' error
                }
                statement.execute(ddl);
            } else {
                fileOutput.write(ddl);
                fileOutput.write(";\n");
            }
        } catch (Exception e) {
            throw MondrianResource.instance().newCreateTableFailed(name, e);
        }
    }

    private String quoteId(String name) {
        return sqlQuery.quoteIdentifier(name);
    }

    private String columnValue(ResultSet rs, Column column) throws Exception {
        String columnType = column.type;
        final Pattern regex = Pattern.compile("DECIMAL\\((.*),(.*)\\)");

        if (columnType.startsWith("INTEGER")) {
            int result = rs.getInt(column.name);
            return Integer.toString(result);
        }
        if (columnType.startsWith("SMALLINT")) {
            short result = rs.getShort(column.name);
            return Integer.toString(result);
        }
        if (columnType.startsWith("BIGINT")) {
            long result = rs.getLong(column.name);
            return Long.toString(result);
        }
        if (columnType.startsWith("VARCHAR")) {
            return embedQuotes(rs.getString(column.name));
        }
        if (columnType.startsWith("TIMESTAMP")) {
            Timestamp ts = rs.getTimestamp(column.name);
            if (ts == null) {
                return "NULL";
            } else {
                return "'" + ts + "'" ;
            }
        }
        if (columnType.startsWith("DATE")) {
            java.sql.Date dt = rs.getDate(column.name);
            if (dt == null) {
                return "NULL";
            } else {
                return "'" + dt + "'" ;
            }
        }
        if (columnType.startsWith("REAL")) {
            return Float.toString(rs.getFloat(column.name));
        }
        if (columnType.startsWith("DECIMAL")) {
            final Matcher matcher = regex.matcher(columnType);
            if (!matcher.matches()) {
                throw new Exception("Bad DECIMAL column type for " + columnType);
            }
            DecimalFormat formatter = new DecimalFormat(decimalFormat(matcher.group(1), matcher.group(2)));
            return formatter.format(rs.getDouble(column.name));
/*
            int places = Integer.parseInt(matcher.group(2));
            BigDecimal dec = rs.getBigDecimal(column.name);
            dec = dec.setScale(places, BigDecimal.ROUND_HALF_UP);
            return dec.toString();
*/        }
        if (columnType.startsWith("BIT")) {
            return Byte.toString(rs.getByte(column.name));
        }
        if (columnType.startsWith("BOOLEAN")) {
            return Boolean.toString(rs.getBoolean(column.name));
        }
        throw new Exception("Unknown column type: " + columnType + " for column: " + column.name);
    }

    private String embedQuotes(String original) {
        if (original == null) {
            return "NULL";
        }
        StringBuffer sb = new StringBuffer();

        sb.append("'");
        for (int i = 0; i < original.length(); i++) {
            char ch = original.charAt(i);
            sb.append(ch);
            if (ch == '\'') {
                sb.append('\'');
            }
        }
        sb.append("'");
        return sb.toString();
    }

    private String decimalFormat(String lengthStr, String placesStr) {
        StringBuffer sb = new StringBuffer();

        int length = Integer.parseInt(lengthStr);
        int places = Integer.parseInt(placesStr);
        for (int i = 0; i < length; i++) {
            if ((length - i) == places) {
                sb.append('.');
            }
            sb.append("#");
        }
        return sb.toString();
    }

    private static class Column {
        private final String name;
        private final String type;
        private final String constraint;

        public Column(String name, String type, String constraint) {
            this.name = name;
            this.type = type;
            this.constraint = constraint;
        }
    }
}
