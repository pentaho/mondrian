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
import mondrian.rolap.RolapUtil;
import mondrian.rolap.sql.SqlQuery;

import java.io.*;
import java.math.BigDecimal;
//import java.math.BigDecimal;
import java.sql.*;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Utility to load the FoodMart dataset into an arbitrary JDBC database.
 *
 * <p>This is known to create test data for the following databases:</p>
 * <ul>
 *
 * <li>MySQL 3.23 using MySQL-connector/J 3.0.16</li>
 *
 * <li>MySQL 4.15 using MySQL-connector/J 3.0.16</li>
 *
 * <li>Postgres 8.0 beta using postgresql-driver-jdbc3-74-214.jar</li>
 *
 * <li>Oracle 10g using ojdbc14.jar</li>
 *
 * </ul>
 * 
 * <p>Output can be to a set of files with create table, insert and create index
 * statements, or directly to a JDBC connection with JDBC batches (lots faster!)</p>
 * 
 * <p>On the command line:</p>
 *
 * <blockquote>MySQL example<code>
 * $ mysqladmin create foodmart<br/>
 * $ java -cp 'classes;testclasses' mondrian.test.loader.MondrianFoodMartLoader
 *     -verbose -tables -data -indexes -jdbcDrivers=com.mysql.jdbc.Driver
 *     -inputJdbcURL=jdbc:odbc:MondrianFoodMart -outputJdbcURL=jdbc:mysql://localhost/foodmart
 * </code></blockquote>
 *
 * @author jhyde
 * @since 23 December, 2004
 * @version $Id$
 */
public class MondrianFoodMartLoader {
    final Pattern decimalDataTypeRegex = Pattern.compile("DECIMAL\\((.*),(.*)\\)");
    final DecimalFormat integerFormatter = new DecimalFormat(decimalFormat(15, 0));
    final String dateFormatString = "yyyy-MM-dd";
    final String oracleDateFormatString = "YYYY-MM-DD";
	final DateFormat dateFormatter = new SimpleDateFormat(dateFormatString);
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
        System.out.println("Usage: MondrianFoodMartLoader [-verbose] [-tables] [-data] [-indexes] " +
        		"-jdbcDrivers=<jdbcDriver> " +
        		"-outputJdbcURL=<jdbcURL> [-outputJdbcUser=user] [-outputJdbcPassword=password]" +
        		"[-outputJdbcBatchSize=<batch size>] " +
        		"| " +
        		"[-outputDirectory=<directory name>] " +
        		"[" +
        		"	[-inputJdbcURL=<jdbcURL> [-inputJdbcUser=user] [-inputJdbcPassword=password]]" +
        		"	| " +
        		"	[-inputfile=<file name>]" +
        		"]");
        System.out.println("");
        System.out.println("  <jdbcURL>         JDBC connect string for DB");
        System.out.println("  [user]            JDBC user name for DB");
        System.out.println("  [password]        JDBC password for user for DB");
        System.out.println("                    If no source DB parameters are given, assumes data comes from file");
        System.out.println("  [file name]       file containing test data - INSERT statements in MySQL format");
        System.out.println("                    If no input file name or input JDBC parameters are given, assume insert statements come from demo/FoodMartCreateData.zip file");
        System.out.println("  [outputDirectory] Where FoodMartCreateTables.sql, FoodMartCreateData.sql and FoodMartCreateIndexes.sql will be created");

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

    /**
     * Load output from the input, optionally creating tables,
     * populating tables and creating indexes
     * 
     * @throws Exception
     */
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

    /**
     * Parse a file of INSERT statements and output to the configured JDBC
     * connection or another file in the dialect of the target data source.
     * 
     * The assumption is that the input INSERT statements are out of MySQL, generated
     * by this loader by something like:
     * 
     * MondrianFoodLoader
     * -verbose -tables -data -indexes 
     * -jdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver,com.mysql.jdbc.Driver 
     * -inputJdbcURL=jdbc:odbc:MondrianFoodMart 
     * -outputJdbcURL=jdbc:mysql://localhost/textload?user=root&password=myAdmin 
     * -outputDirectory=C:\Temp\wip\Loader-Output
     * 
     * @throws Exception
     */
    private void loadDataFromFile() throws Exception {
        InputStream is = openInputStream();
        
        if (is == null) {
        	throw new Exception("No data file to process");
        }
        
        try {
			final InputStreamReader reader = new InputStreamReader(is);
			final BufferedReader bufferedReader = new BufferedReader(reader);
			final Pattern regex = Pattern.compile("INSERT INTO `([^ ]+)` \\((.*)\\) VALUES\\((.*)\\);");
			String line;
			int lineNumber = 0;
			int tableRowCount = 0;
			String prevTable = "";
			String quotedTableName = null;
			String quotedColumnNames = null;
			Column[] orderedColumns = null;
			
			String[] batch = new String[inputBatchSize];
			int batchSize = 0;
			
			while ((line = bufferedReader.readLine()) != null) {
			    ++lineNumber;
			    if (line.startsWith("#")) {
			        continue;
			    }
			    // Split the up the line. For example,
			    //   INSERT INTO `foo` ( `column1`,`column2` ) VALUES (1, 'bar');
			    // would yield
			    //   tableName = "foo"
			    //	 columnNames = " `column1`,`column2` "
			    //   values = "1, 'bar'"
			    final Matcher matcher = regex.matcher(line);
			    if (!matcher.matches()) {
			        throw MondrianResource.instance().newInvalidInsertLine(
			            new Integer(lineNumber), line);
			    }
			    String tableName = matcher.group(1); // e.g. "foo"
			    String columnNames = matcher.group(2); 
			    String values = matcher.group(3); 
//            Util.discard(values); // Not needed now

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
			        quotedTableName = quoteId(tableName);
			        quotedColumnNames = columnNames.replaceAll("`", sqlQuery.getQuoteIdentifierString());
			        String[] splitColumnNames = columnNames.replaceAll("`", "").replaceAll(" ", "").split(",");
			        Column[] columns = (Column[]) mapTableNameToColumns.get(tableName);
			    	
			        orderedColumns = new Column[columns.length];
			        
			    	for (int i = 0; i < splitColumnNames.length; i++) {
			    		Column thisColumn = null;
			    		for (int j = 0; j < columns.length && thisColumn == null; j++) {
			    			if (columns[j].name.equalsIgnoreCase(splitColumnNames[i])) {
			    				thisColumn = columns[j]; 
			    			}
			    		}
			    		if (thisColumn == null) {
			    			throw new Exception("Unknown column in INSERT statement from file: " + splitColumnNames[i]);
			    		} else {
			    			orderedColumns[i] = thisColumn; 
			    		}
			    	}

			        
			    }
			    
			    StringBuffer massagedLine = new StringBuffer();
			    
			    massagedLine
					.append("INSERT INTO ")
					.append(quotedTableName)
					.append(" (")
					.append(quotedColumnNames)
					.append(" ) VALUES(")
					.append(getMassagedValues(orderedColumns, values))
					.append(" )");
			    
			    line = massagedLine.toString();

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
		} finally {
			if (is != null) {
				is.close();
			}
		}
    }

    /**
     * @param splitColumnNames		the individual column names in the same order as the values 
     * @param columns				column metadata for the table
     * @param values				the contents of the INSERT VALUES clause ie. "34,67.89,'GHt''ab'". These are in MySQL form.
     * @return String				values for the destination dialect
     * @throws Exception
     */
    private String getMassagedValues(Column[] columns, String values) throws Exception {
    	StringBuffer sb = new StringBuffer();
    	
    	// Get the values out as individual elements
    	// Split the string at commas, and cope with embedded commas
    	String[] individualValues = new String[columns.length];
    	
    	String[] splitValues = values.split(",");
    	
    	// If these 2 are the same length, then there are no embedded commas
    	
    	if (splitValues.length == columns.length) {
    		individualValues = splitValues;
    	} else {
        	// "34,67.89,'GH,t''a,b'" => { "34", "67.89", "'GH", "t''a", "b'"
	    	int valuesPos = 0;
	    	boolean inQuote = false;
	    	for (int i = 0; i < splitValues.length; i++) {
	    		if (i == 0) {
	    			individualValues[valuesPos] = splitValues[i];
	    			inQuote = inQuote(splitValues[i], inQuote);
	    		} else {
	    			// at end
	    			if (inQuote) {
	    				individualValues[valuesPos] = individualValues[valuesPos] + "," + splitValues[i];
		    			inQuote = inQuote(splitValues[i], inQuote);
	    			} else {
	    				valuesPos++;
	    				individualValues[valuesPos] = splitValues[i];
		    			inQuote = inQuote(splitValues[i], inQuote);
	    			}
	    		}
	    	}
	    	
	    	assert(valuesPos + 1 == columns.length);
    	}
    	
    	for (int i = 0; i < columns.length; i++) {
    		if (i > 0) {
    			sb.append(",");
    		}
    	 	sb.append(columnValue(individualValues[i], columns[i]));
    	}
    	return sb.toString();
    	
    }
    
    private boolean inQuote(String str, boolean nowInQuote) {
    	if (str.indexOf('\'') == -1) {
    		// No quote, so stay the same
    		return nowInQuote;
    	}
    	int lastPos = 0;
    	while (lastPos <= str.length() && str.indexOf('\'', lastPos) != -1) {
    		int pos = str.indexOf('\'', lastPos);
    		nowInQuote = !nowInQuote;
    		lastPos = pos + 1;
    	}
    	return nowInQuote;
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

    /**
     * Read the given table from the input RDBMS and output to destination
     * RDBMS or file
     * 
     * @param name		name of table
     * @param columns	columns to be read/output
     * @return			#rows inserted
     * @throws Exception
     */
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
        boolean displayedInsert = false;

        while (rs.next()) {
            /*
             * Get a batch of insert statements, then save a batch
             */

            String insertStatement = createInsertStatement(rs, name, columns);
            if (!displayedInsert && verbose) {
            	System.out.println("Example Insert statement: " + insertStatement);
            	displayedInsert = true;
            }
            batch[batchSize++] = insertStatement;
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

    /**
     * Create a SQL INSERT statement in the dialect of the output RDBMS.
     *  
     * @param rs			ResultSet of input RDBMS
     * @param name			name of table
     * @param columns		column definitions for INSERT statement
     * @return String		the INSERT statement
     * @throws Exception
     */
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

    /**
     * If we are outputting to JDBC,
     * 		Execute the given set of SQL statements
     * 
     * Otherwise,
     * 		output the statements to a file.
     * 
     * @param batch			SQL statements to execute
     * @param batchSize		# SQL statements to execute
     * @return				# SQL statements executed
     * @throws IOException
     * @throws SQLException
     */
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
                int [] updateCounts = null;
                
                try {
                	updateCounts = stmt.executeBatch();
                } catch (SQLException e) {
                    for (int i = 0; i < batchSize; i++) {
                    	System.out.println("Error in SQL batch: " + batch[i]);
                    }
                    throw e;
                }
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

    /**
     * Open the file of INSERT statements to load the data. Default
     * file name is ./demo/FoodMartCreateData.zip
     *    
     * @return FileInputStream
     */
    private InputStream openInputStream() throws Exception {
    	final String defaultZipFileName = "FoodMartCreateData.zip";
    	final String defaultDataFileName = "FoodMartCreateData.sql";
        final File file = (inputFile != null) ? new File(inputFile) : new File("demo", defaultZipFileName);
        if (!file.exists()) {
            System.out.println("No input file: " + file);
            return null;
        }
    	if (file.getName().toLowerCase().endsWith(".zip")) {
    		ZipFile zippedData = new ZipFile(file);
    		ZipEntry entry = zippedData.getEntry(defaultDataFileName);
    		return zippedData.getInputStream(entry);
    	} else {
            return new FileInputStream(file);
    	}
    }

    /**
     * Create all indexes for the FoodMart database
     * 
     * @throws Exception
     */
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

    /**
     * 
     * If we are outputting to JDBC,
     * 		Execute the CREATE INDEX statement
     * 
     * Otherwise,
     * 		output the statement to a file.
     *
     * @param isUnique
     * @param tableName
     * @param indexName
     * @param columnNames
     */
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
     * Define all tables for the FoodMart database.
     * 
     * Also initializes mapTableNameToColumns
     *
     * @throws Exception
     */
    private void createTables() throws Exception  {
        if (outputDirectory != null) {
            fileOutput = new FileWriter(new File(outputDirectory, "createTables.sql"));
        }

        String booleanColumnType = "SMALLINT";
        if (sqlQuery.isPostgres()) {
            booleanColumnType = "BOOLEAN";
        } else if (sqlQuery.isMySQL()) {
            booleanColumnType = "BIT";
        }
        
        String bigIntColumnType = "BIGINT";
        if (sqlQuery.isOracle()) {
        	bigIntColumnType = "DECIMAL(15,0)";
        }

        createTable("sales_fact_1997", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", "NOT NULL"),
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("promotion_id", "INTEGER", "NOT NULL"),
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_sales", "DECIMAL(10,4)", "NOT NULL"),
          new Column("store_cost", "DECIMAL(10,4)", "NOT NULL"),
          new Column("unit_sales", "DECIMAL(10,4)", "NOT NULL"),
        });
        createTable("sales_fact_1998", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", "NOT NULL"),
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("promotion_id", "INTEGER", "NOT NULL"),
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_sales", "DECIMAL(10,4)", "NOT NULL"),
          new Column("store_cost", "DECIMAL(10,4)", "NOT NULL"),
          new Column("unit_sales", "DECIMAL(10,4)", "NOT NULL"),
        });
        createTable("sales_fact_dec_1998", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", "NOT NULL"),
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("promotion_id", "INTEGER", "NOT NULL"),
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_sales", "DECIMAL(10,4)", "NOT NULL"),
          new Column("store_cost", "DECIMAL(10,4)", "NOT NULL"),
          new Column("unit_sales", "DECIMAL(10,4)", "NOT NULL"),
        });
        createTable("inventory_fact_1997", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", ""),
          new Column("warehouse_id", "INTEGER", ""),
          new Column("store_id", "INTEGER", ""),
          new Column("units_ordered", "INTEGER", ""),
          new Column("units_shipped", "INTEGER", ""),
          new Column("warehouse_sales", "DECIMAL(10,4)", ""),
          new Column("warehouse_cost", "DECIMAL(10,4)", ""),
          new Column("supply_time", "SMALLINT", ""),
          new Column("store_invoice", "DECIMAL(10,4)", ""),
        });
        createTable("inventory_fact_1998", new Column[] {
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("time_id", "INTEGER", ""),
          new Column("warehouse_id", "INTEGER", ""),
          new Column("store_id", "INTEGER", ""),
          new Column("units_ordered", "INTEGER", ""),
          new Column("units_shipped", "INTEGER", ""),
          new Column("warehouse_sales", "DECIMAL(10,4)", ""),
          new Column("warehouse_cost", "DECIMAL(10,4)", ""),
          new Column("supply_time", "SMALLINT", ""),
          new Column("store_invoice", "DECIMAL(10,4)", ""),
        });
        createTable("currency", new Column[] {
                new Column("currency_id", "INTEGER", "NOT NULL"),
                new Column("date", "DATE", "NOT NULL"),
                new Column("currency", "VARCHAR(30)", "NOT NULL"),
                new Column("conversion_ratio", "DECIMAL(10,4)", "NOT NULL"),
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
        createTable("customer", new Column[] {
          new Column("customer_id", "INTEGER", "NOT NULL"),
          new Column("account_num", bigIntColumnType, "NOT NULL"),
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
          new Column("salary", "DECIMAL(10,4)", "NOT NULL"),
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
          new Column("amount", "DECIMAL(10,4)", "NOT NULL"),
        });
        createTable("position", new Column[] {
          new Column("position_id", "INTEGER", "NOT NULL"),
          new Column("position_title", "VARCHAR(30)", "NOT NULL"),
          new Column("pay_type", "VARCHAR(30)", "NOT NULL"),
          new Column("min_scale", "DECIMAL(10,4)", "NOT NULL"),
          new Column("max_scale", "DECIMAL(10,4)", "NOT NULL"),
          new Column("management_role", "VARCHAR(30)", "NOT NULL"),
        });
        createTable("product", new Column[] {
          new Column("product_class_id", "INTEGER", "NOT NULL"),
          new Column("product_id", "INTEGER", "NOT NULL"),
          new Column("brand_name", "VARCHAR(60)", ""),
          new Column("product_name", "VARCHAR(60)", "NOT NULL"),
          new Column("SKU", bigIntColumnType, "NOT NULL"),
          new Column("SRP", "DECIMAL(10,4)", ""),
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
          new Column("cost", "DECIMAL(10,4)", ""),
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
          new Column("salary", "DECIMAL(10,4)", "NOT NULL"),
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
          new Column("salary_paid", "DECIMAL(10,4)", "NOT NULL"),
          new Column("overtime_paid", "DECIMAL(10,4)", "NOT NULL"),
          new Column("vacation_accrued", "REAL", "NOT NULL"),
          new Column("vacation_used", "REAL", "NOT NULL"),
        });
        createTable("store", new Column[] {
          new Column("store_id", "INTEGER", "NOT NULL"),
          new Column("store_type", "VARCHAR(30)", ""),
          new Column("region_id", "INTEGER", ""),
          new Column("store_name", "VARCHAR(30)", ""),
          new Column("store_number", "INTEGER", ""),
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
          new Column("store_sqft", "INTEGER", ""),
          new Column("grocery_sqft", "INTEGER", ""),
          new Column("frozen_sqft", "INTEGER", ""),
          new Column("meat_sqft", "INTEGER", ""),
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
          new Column("store_number", "INTEGER", ""),
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
          new Column("store_sqft", "INTEGER", ""),
          new Column("grocery_sqft", "INTEGER", ""),
          new Column("frozen_sqft", "INTEGER", ""),
          new Column("meat_sqft", "INTEGER", ""),
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

    /**
     * If we are outputting to JDBC, and not creating tables, delete all rows.
     * 
     * Otherwise:
     * 
     * Generate the SQL CREATE TABLE statement.
     * 
     * If we are outputting to JDBC,
     * 		Execute a DROP TABLE statement
     * 		Execute the CREATE TABLE statement
     * 
     * Otherwise,
     * 		output the statement to a file.
     * 
     * @param name
     * @param columns
     */
    private void createTable(String name, Column[] columns) {
        try {
            // Define the table.
            mapTableNameToColumns.put(name, columns);
            if (!tables) {
                if (data && jdbcOutput) {
                    // We're going to load the data without [re]creating
                    // the table, so let's remove the data.
                    final Statement statement = connection.createStatement();
                    try {
                        if (verbose) {
                        	System.out.println("DELETE FROM " + quoteId(name));
                        }
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
            if (jdbcOutput) {
                final Statement statement = connection.createStatement();
                try {
                    if (verbose) {
                    	System.out.println("DROP TABLE " + quoteId(name));
                    }
                    statement.execute("DROP TABLE " + quoteId(name));
                } catch (SQLException e) {
                    // ignore 'table does not exist' error
                    if (verbose) {
                    	System.out.println("DROP TABLE exception: " + e.getMessage());
                    }
                }

                if (verbose) {
                    System.out.println(ddl);
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

    /**
     * Quote the given SQL identifier suitable for the output DBMS.
     * @param name
     * @return
     */
    private String quoteId(String name) {
        return sqlQuery.quoteIdentifier(name);
    }
    
    /**
     * String representation of the column in the result set, suitable for
     * inclusion in a SQL insert statement.
     * 
     * The column in the result set is transformed according to the type in
     * the column parameter.
     * 
     * Different DBMSs return different Java types for a given column.
     * ClassCastExceptions may occur.
     * 
     * @param rs  		ResultSet row to process
     * @param column	Column to process
     * @return			String representation of column value
     * @throws Exception
     */
    private String columnValue(ResultSet rs, Column column) throws Exception {

		Object obj = rs.getObject(column.name);
        String columnType = column.type;

        if (obj == null) {
            return "NULL";
        }
        
        /*
         * Output for an INTEGER column, handling Doubles and Integers 
         * in the result set 
         */
        if (columnType.startsWith("INTEGER")) {
            if (obj.getClass() == Double.class) {
            	try {
	            	Double result = (Double) obj;
		            return integerFormatter.format(result.doubleValue());
            	} catch (ClassCastException cce) {
            		System.out.println("CCE: "  + column.name + " to Long from: " + obj.getClass().getName() + " - " + obj.toString());
            		throw cce;
            	}
            } else {
            	try {
            		Integer result = (Integer) obj;
            		return result.toString();
	        	} catch (ClassCastException cce) {
	        		System.out.println("CCE: "  + column.name + " to Integer from: " + obj.getClass().getName() + " - " + obj.toString());
	        		throw cce;
	        	}
            }
            
        /*
         * Output for an SMALLINT column, handling Integers 
         * in the result set 
         */
        } else if (columnType.startsWith("SMALLINT")) {
            if (obj.getClass() == Boolean.class) {
               	Boolean result = (Boolean) obj;
               	if (result.booleanValue()) {
               		return "1";
               	} else {
               		return "0";
               	}
            } else {
	        	try {
		        	Integer result = (Integer) obj;
		            return result.toString();
	        	} catch (ClassCastException cce) {
	        		System.out.println("CCE: "  + column.name + " to Integer from: " + obj.getClass().getName() + " - " + obj.toString());
	        		throw cce;
	        	}
            }
        /*
         * Output for an BIGINT column, handling Doubles and Longs 
         * in the result set 
         */
        } else if (columnType.startsWith("BIGINT")) {
            if (obj.getClass() == Double.class) {
            	try {
	            	Double result = (Double) obj;
		            return integerFormatter.format(result.doubleValue());
            	} catch (ClassCastException cce) {
            		System.out.println("CCE: "  + column.name + " to Double from: " + obj.getClass().getName() + " - " + obj.toString());
            		throw cce;
            	}
            } else {
            	try {
	            	Long result = (Long) obj;
	                return result.toString();
            	} catch (ClassCastException cce) {
            		System.out.println("CCE: "  + column.name + " to Long from: " + obj.getClass().getName() + " - " + obj.toString());
            		throw cce;
            	}
            }
            
        /*
         * Output for a String, managing embedded quotes 
         */
        } else if (columnType.startsWith("VARCHAR")) {
            return embedQuotes((String) obj);

        /*
         * Output for a TIMESTAMP
         */
        } else if (columnType.startsWith("TIMESTAMP")) {
            Timestamp ts = (Timestamp) obj;
            if (sqlQuery.isOracle()) {
            	return "TIMESTAMP '" + ts + "'";
            } else {
            	return "'" + ts + "'";
            }
            //return "'" + ts + "'" ;
            
        /*
         * Output for a DATE
         */
        } else if (columnType.startsWith("DATE")) {
            Date dt = (Date) obj;
            if (sqlQuery.isOracle()) {
            	return "DATE '" + dateFormatter.format(dt) + "'";
            } else {
            	return "'" + dateFormatter.format(dt) + "'";
            }
            
        /*
         * Output for a FLOAT
         */
        } else if (columnType.startsWith("REAL")) {
        	Float result = (Float) obj;
            return result.toString();
            
        /*
         * Output for a DECIMAL(length, places)
         */
        } else if (columnType.startsWith("DECIMAL")) {
            final Matcher matcher = decimalDataTypeRegex.matcher(columnType);
            if (!matcher.matches()) {
                throw new Exception("Bad DECIMAL column type for " + columnType);
            }
            DecimalFormat formatter = new DecimalFormat(decimalFormat(matcher.group(1), matcher.group(2)));
            if (obj.getClass() == Double.class) {
	            try {
	            	Double result = (Double) obj;
		            return formatter.format(result.doubleValue());
	        	} catch (ClassCastException cce) {
	        		System.out.println("CCE: "  + column.name + " to Double from: " + obj.getClass().getName() + " - " + obj.toString());
	        		throw cce;
	        	}
            } else {
            	// should be (obj.getClass() == BigDecimal.class)
	            try {
	            	BigDecimal result = (BigDecimal) obj;
		            return formatter.format(result);
	        	} catch (ClassCastException cce) {
	        		System.out.println("CCE: "  + column.name + " to BigDecimal from: " + obj.getClass().getName() + " - " + obj.toString());
	        		throw cce;
	        	}
            }
            
        /*
         * Output for a BOOLEAN (Postgres) or BIT (other DBMSs)
         */
        } else if (columnType.startsWith("BOOLEAN") || columnType.startsWith("BIT")) {
           	Boolean result = (Boolean) obj;
            return result.toString();
        }
        throw new Exception("Unknown column type: " + columnType + " for column: " + column.name);
    }

    private String columnValue(String columnValue, Column column) throws Exception {
        String columnType = column.type;

        if (columnValue == null) {
            return "NULL";
        }

        /*
         * Output for a TIMESTAMP
         */
        if (columnType.startsWith("TIMESTAMP")) {
            if (sqlQuery.isOracle()) {
            	return "TIMESTAMP " + columnValue;
            }
            
        /*
         * Output for a DATE
         */
        } else if (columnType.startsWith("DATE")) {
            if (sqlQuery.isOracle()) {
            	return "DATE " + columnValue;
            }
            
        /*
         * Output for a BOOLEAN (Postgres) or BIT (other DBMSs)
         */
        } else if (columnType.startsWith("BOOLEAN") || columnType.startsWith("BIT")) {
            if (!sqlQuery.isMySQL()) {
            	if (columnValue.trim().equals("1")) {
            		return "true";
            	} else if (columnValue.trim().equals("0")) {
            		return "false";
            	} 
            }
        }
    	return columnValue;
        //throw new Exception("Unknown column type: " + columnType + " for column: " + column.name);
    }
    
    /**
     * Generate an appropriate string to use in an SQL insert statement for
     * a VARCHAR colummn, taking into account NULL strings and strings with embedded
     * quotes
     * 
     * @param original  String to transform
     * @return NULL if null string, otherwise massaged string with doubled quotes
     * 		   for SQL
     */
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
    private static String decimalFormat(String lengthStr, String placesStr) {

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
    private static String decimalFormat(int length, int places) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if ((length - i) == places) {
                sb.append('.');
            }
            if ((length - i) <= (places + 1)) {
            	sb.append("0");
            } else {
            	sb.append("#");
            }
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
