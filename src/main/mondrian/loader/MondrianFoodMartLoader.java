/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2004 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.loader;

import mondrian.rolap.RolapConnection;
import mondrian.rolap.sql.SqlQuery;
import mondrian.olap.MondrianResource;

import java.sql.*;
import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashMap;

/**
 * Utility to load the FoodMart dataset into an arbitrary JDBC database.
 *
 * <p>It is known to work for the following databases:<ul>
 * <li>MySQL 3.23 using MySQL-connector/J 3.0.16
 * <li>
 * </ul>
 *
 * @author jhyde
 * @since 23 December, 2004
 * @version $Id$
 */
public class MondrianFoodMartLoader {
    private final String jdbcURL;
    private final String jdbcDrivers;
    private boolean tables = false;
    private boolean indexes = false;
    private boolean data = false;
    private static final String nl = System.getProperty("line.separator");
    private boolean verbose = false;
    private Connection connection;
    private SqlQuery sqlQuery;
    private final HashMap mapTableNameToColumns = new HashMap();

    public MondrianFoodMartLoader(String[] args) {
        int i = 0;
        if (i < args.length && args[i].equals("-verbose")) {
            ++i;
            verbose = true;
        }
        if (i < args.length && args[i].equals("-tables")) {
            ++i;
            tables = true;
        }
        if (i < args.length && args[i].equals("-data")) {
            ++i;
            data = true;
        }
        if (i < args.length && args[i].equals("-indexes")) {
            ++i;
            indexes = true;
        }
        if (i >= args.length) {
            usage();
            throw MondrianResource.instance().newMissingArg("jdbcURL");
        }
        jdbcURL = args[i++];
        if (i >= args.length) {
            usage();
            throw MondrianResource.instance().newMissingArg("jdbcDrivers");
        }
        jdbcDrivers = args[i++];
    }

    public void usage() {
        System.out.println("Usage: MondrianFoodMartLoader [-verbose] [-tables] [-data] [-indexes] <jdbcURL> <jdbcDriver>");
        System.out.println("");
        System.out.println("  <jdbcURL>     JDBC connect string");
        System.out.println("  <jdbcDrivers> Comma-separated list of JDBC drivers.");
        System.out.println("                They must be on the classpath.");
        System.out.println("  -verbose      Verbose mode.");
        System.out.println("  -tables       If specified, drop and create the tables.");
        System.out.println("  -data         If specified, load the data.");
        System.out.println("  -indexes      If specified, drop and create the tables.");
    }

    public static void main(String[] args) {
        try {
            new MondrianFoodMartLoader(args).load();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void load() throws SQLException, IOException {
        RolapConnection.loadDrivers(jdbcDrivers);
        connection = DriverManager.getConnection(jdbcURL);
        final DatabaseMetaData metaData = connection.getMetaData();
        sqlQuery = new SqlQuery(metaData);
        try {
            createTables();
            if (data) {
                loadData();
            }
            if (indexes) {
                createIndexes();
            }
        } finally {
            connection = null;
        }
        //To change body of created methods use File | Settings | File Templates.
    }

    private void loadData() throws IOException, SQLException {
        final InputStream is = openInputStream();
        final InputStreamReader reader = new InputStreamReader(is);
        final BufferedReader bufferedReader = new BufferedReader(reader);
//        final Pattern regex = Pattern.compile("INSERT INTO \\([^ ]+\\) VALUES (\\(.*\\));");
        final Pattern regex = Pattern.compile("INSERT INTO ([^ ]+) VALUES\\((.*)\\);");
//        final Pattern regex = Pattern.compile("INSERT INTO ([^ ]+) VALUES(.*);.*");
        String line;
        int lineNumber = 0;
        int tableRowCount = 0;
        String prevTable = "";
        final Statement statement = connection.createStatement();
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

            // Generate a statement appropriate for this database. Not very
            // efficient, but good enough.

            // remove trailing ';'
            line = line.substring(0, line.length() - 1);

            // this database represents booleans as integers
            if (sqlQuery.isMySQL()) {
                line = line.replaceAll("false", "0")
                    .replaceAll("true", "1");
            }

            ++tableRowCount;
            statement.execute(line);
            if (!tableName.equals(prevTable)) {
                System.out.println("Table " + prevTable +
                    ": loaded " + tableRowCount + " rows.");
                tableRowCount = 0;
                prevTable = tableName;
            }
        }
        // Print summary of the final table.
        if (!"".equals(prevTable)) {
            System.out.println("Table " + prevTable +
                ": loaded " + tableRowCount + " rows.");
            tableRowCount = 0;
        }
    }

    private FileInputStream openInputStream() {
        final File file = new File("demo", "FoodMartData.sql");
        if (file.exists()) {
            try {
                return new FileInputStream(file);
            } catch (FileNotFoundException e) {
            }
        }
        return null;
    }

    private void createIndexes() {
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
            final Statement statement = connection.createStatement();
            statement.execute(ddl);
        } catch (SQLException e) {
            throw MondrianResource.instance().newCreateIndexFailed(indexName,
                tableName, e);
        }
    }

    private void createTables() {
        createTable("account", new Column[] {
          new Column("account_id", "INTEGER", "NOT NULL"),
          new Column("account_parent", "INTEGER", ""),
          new Column("account_description", "VARCHAR(30)", ""),
          new Column("account_type", "VARCHAR(30)", "NOT NULL"),
          new Column("account_rollup", "VARCHAR(30)", "NOT NULL"),
          new Column("Custom_Members", "VARCHAR(30)", ""),
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
          new Column("conversion_ratio", "DECIMAL(10,2)", "NOT NULL"),
        });
        createTable("customer", new Column[] {
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("ordinal", "INTEGER", "NOT NULL"),
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
          new Column("recyclable_package", "BIT", ""),
          new Column("low_fat", "BIT", ""),
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
          new Column("coffee_bar", "BIT", ""),
          new Column("video_store", "BIT", ""),
          new Column("salad_bar", "BIT", ""),
          new Column("prepared_food", "BIT", ""),
          new Column("florist", "BIT", ""),
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
            final Statement statement = connection.createStatement();
            try {
                statement.execute("DROP TABLE " + quoteId(name));
            } catch (SQLException e) {
                // ignore 'table does not exist' error
            }
            statement.execute(ddl);
        } catch (SQLException e) {
            throw MondrianResource.instance().newCreateTableFailed(name, e);
        }
    }

    private String quoteId(String name) {
        return sqlQuery.quoteIdentifier(name);
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
