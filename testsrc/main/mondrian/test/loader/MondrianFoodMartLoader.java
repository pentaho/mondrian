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

import org.apache.log4j.Logger;

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
 * <li>FirebirdSQL 1.0.2 and JayBird 1.5 (JDBC)</li>
 *
 * </ul>
 *
 * <p>Output can be to a set of files with create table, insert and create
 * index statements, or directly to a JDBC connection with JDBC batches
 * (lots faster!)</p>
 *
 * <h3>Command line examples for specific databases</h3>
 *
 * <h4>MySQL</h4>
 *
 * <blockquote><code>
 * $ mysqladmin create foodmart<br/>
 * $ java -cp 'classes;testclasses' mondrian.test.loader.MondrianFoodMartLoader
 *     -verbose -tables -data -indexes -jdbcDrivers=com.mysql.jdbc.Driver
 *     -inputJdbcURL=jdbc:odbc:MondrianFoodMart -outputJdbcURL=jdbc:mysql://localhost/foodmart
 * </code></blockquote>
 *
 * <h4>FirebirdSQL</h4>
 *
 * <blockquote><code>
 * $ /firebird/bin/isql -u SYSDBA -p masterkey<br/>
 * Use CONNECT or CREATE DATABASE to specify a database<br/>
 * SQL&gt; CREATE DATABASE '/mondrian/foodmart.gdb';<br/>
 * SQL&gt; QUIT;<br/>
 * $ java -cp "/mondrian/lib/mondrian.jar:/mondrian/lib/log4j-1.2.9.jar:/mondrian/lib/eigenbase-xom.jar:/mondrian/lib/eigenbase-resgen.jar:/jdbc/fb/firebirdsql-full.jar"
 *    mondrian.test.loader.MondrianFoodMartLoader
 *    -verbose -tables -data -indexes
 *    -jdbcDrivers="org.firebirdsql.jdbc.FBDriver"
 *    -inputFile="/mondrian/demo/FoodMartCreateData.sql"
 *    -outputJdbcURL="jdbc:firebirdsql:localhost/3050:/mondrian/foodmart.gdb"
 *    -inputJdbcUser=SYSDBA
 *    -inputJdbcPassword=masterkey
 * </code></blockquote>
 *
 * @author jhyde
 * @since 23 December, 2004
 * @version $Id$
 */
public class MondrianFoodMartLoader {
    private static final Logger LOGGER = Logger.getLogger(MondrianFoodMartLoader.class);

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
    private boolean jdbcInput = false;
    private boolean jdbcOutput = false;
    private boolean populationQueries = false;
    private int inputBatchSize = 50;
    private Connection connection;
    private Connection inputConnection;

    private FileWriter fileOutput = null;

    private SqlQuery sqlQuery;
    private String booleanColumnType;
    private String bigIntColumnType;
    private final Map tableMetadataToLoad = new HashMap();
    private final Map aggregateTableMetadataToLoad = new HashMap();

    public MondrianFoodMartLoader(String[] args) {

        StringBuffer errorMessage = new StringBuffer();
        StringBuffer parametersMessage = new StringBuffer();

        for ( int i=0; i<args.length; i++ )  {
            if (args[i].equals("-tables")) {
                tables = true;
            } else if (args[i].equals("-data")) {
                data = true;
            } else if (args[i].equals("-indexes")) {
                indexes = true;
            } else if (args[i].equals("-populationQueries")) {
            	populationQueries = true;
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
                errorMessage.append("unknown arg: " + args[i] + nl);
            }

            if (LOGGER.isInfoEnabled()) {
            	parametersMessage.append("\t" + args[i] + nl);
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

        if (LOGGER.isInfoEnabled()) {
        	LOGGER.info("Parameters: " + nl + parametersMessage.toString());
        }
    }

    public void usage() {
        System.out.println("Usage: MondrianFoodMartLoader [-tables] [-data] [-indexes] [-populationQueries]" +
                "-jdbcDrivers=<jdbcDriver> " +
                "-outputJdbcURL=<jdbcURL> [-outputJdbcUser=user] [-outputJdbcPassword=password]" +
                "[-outputJdbcBatchSize=<batch size>] " +
                "| " +
                "[-outputDirectory=<directory name>] " +
                "[" +
                "   [-inputJdbcURL=<jdbcURL> [-inputJdbcUser=user] [-inputJdbcPassword=password]]" +
                "   | " +
                "   [-inputfile=<file name>]" +
                "]");
        System.out.println("");
        System.out.println("  <jdbcURL>         	JDBC connect string for DB");
        System.out.println("  [user]            	JDBC user name for DB");
        System.out.println("  [password]        	JDBC password for user for DB");
        System.out.println("                    	If no source DB parameters are given, assumes data comes from file");
        System.out.println("  [file name]       	file containing test data - INSERT statements in MySQL format");
        System.out.println("                    	If no input file name or input JDBC parameters are given, assume insert statements come from demo/FoodMartCreateData.zip file");
        System.out.println("  [outputDirectory] 	Where FoodMartCreateTables.sql, FoodMartCreateData.sql and FoodMartCreateIndexes.sql will be created");

        System.out.println("  <batch size>      	size of JDBC batch updates - default to 50 inserts");
        System.out.println("  <jdbcDrivers>     	Comma-separated list of JDBC drivers.");
        System.out.println("                    	They must be on the classpath.");
        System.out.println("  -verbose          	Verbose mode.");
        System.out.println("  -tables           	If specified, drop and create the tables.");
        System.out.println("  -data             	If specified, load the data.");
        System.out.println("  -indexes          	If specified, drop and create the tables.");
        System.out.println("  -populationQueries	If specified, run the data loading queries. Runs by default if -data is specified.");
    }

    public static void main(String[] args) {
        LOGGER.warn("Starting load at: " + (new Date()));
        try {
            new MondrianFoodMartLoader(args).load();
        } catch (Throwable e) {
            LOGGER.error("Main error", e);
        }
        LOGGER.warn("Finished load at: " + (new Date()));
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

        String productName = metaData.getDatabaseProductName();
        String version = metaData.getDatabaseProductVersion();

        LOGGER.info("Output connection is " + productName + ", " + version);

        sqlQuery = new SqlQuery(metaData);
        booleanColumnType = "SMALLINT";
        if (sqlQuery.getDialect().isPostgres()) {
            booleanColumnType = "BOOLEAN";
        } else if (sqlQuery.getDialect().isMySQL()) {
            booleanColumnType = "TINYINT(1)";
        }

        bigIntColumnType = "BIGINT";
        if (sqlQuery.getDialect().isOracle() ||
                sqlQuery.getDialect().isFirebird()) {
            bigIntColumnType = "DECIMAL(15,0)";
        }

        try {
            createTables();  // This also initializes tableMetadataToLoad
            if (data) {
            	if (!populationQueries) {
	                if (jdbcInput) {
	                    loadDataFromJdbcInput();
	                } else {
	                    loadDataFromFile();
	                }
            	}
                loadFromSQLInserts();
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
     * The assumption is that the input INSERT statements are generated
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
            final Pattern mySQLRegex = Pattern.compile("INSERT INTO `([^ ]+)` \\((.*)\\) VALUES\\((.*)\\);");
            final Pattern doubleQuoteRegex = Pattern.compile("INSERT INTO \"([^ ]+)\" \\((.*)\\) VALUES\\((.*)\\);");
            String line;
            int lineNumber = 0;
            int tableRowCount = 0;
            String prevTable = "";
            String quotedTableName = null;
            String quotedColumnNames = null;
            Column[] orderedColumns = null;

            String[] batch = new String[inputBatchSize];
            int batchSize = 0;

            Pattern regex = null;
            String quoteChar = null;

            while ((line = bufferedReader.readLine()) != null) {
                ++lineNumber;
                if (line.startsWith("#")) {
                    continue;
                }
                if (regex == null) {
                	Matcher m1 = mySQLRegex.matcher(line);
                	if (m1.matches()) {
                		regex = mySQLRegex;
                		quoteChar = "`";
                	} else {
                		regex = doubleQuoteRegex;
                		quoteChar = "\"";
                	}
                }
                // Split the up the line. For example,
                //   INSERT INTO `foo` ( `column1`,`column2` ) VALUES (1, 'bar');
                // would yield
                //   tableName = "foo"
                //   columnNames = " `column1`,`column2` "
                //   values = "1, 'bar'"
                final Matcher matcher = regex.matcher(line);
                if (!matcher.matches()) {
                    throw MondrianResource.instance().newInvalidInsertLine(
                        new Integer(lineNumber), line);
                }
                String tableName = matcher.group(1); // e.g. "foo"
                String columnNames = matcher.group(2);
                String values = matcher.group(3);

                // If table just changed, flush the previous batch.
                if (!tableName.equals(prevTable)) {
                    if (!prevTable.equals("")) {
                    	LOGGER.info("Table " + prevTable +
                            ": loaded " + tableRowCount + " rows.");
                    }
                    tableRowCount = 0;
                    writeBatch(batch, batchSize);
                    batchSize = 0;
                    prevTable = tableName;
                    quotedTableName = quoteId(tableName);
                    quotedColumnNames = columnNames.replaceAll(quoteChar,
                            sqlQuery.getDialect().getQuoteIdentifierString());
                    String[] splitColumnNames = columnNames.replaceAll(quoteChar, "")
                    						.replaceAll(" ", "").split(",");
                    Column[] columns = (Column[]) tableMetadataToLoad.get(tableName);

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
            	LOGGER.info("Table " + prevTable +
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
     * @param columns               column metadata for the table
     * @param values                the contents of the INSERT VALUES clause,
     *                              for example "34,67.89,'GHt''ab'".
     *                              These are in MySQL form.
     * @return String               values for the destination dialect
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
            String value = individualValues[i];
            if (value != null && value.trim().equals("NULL")) {
                value = null;
            }
            sb.append(columnValue(value, columns[i]));
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

        for (Iterator it = tableMetadataToLoad.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry tableEntry = (Map.Entry) it.next();
            int rowsAdded = loadTable((String) tableEntry.getKey(), (Column[]) tableEntry.getValue());
            LOGGER.info("Table " + (String) tableEntry.getKey() +
                    ": loaded " + rowsAdded + " rows.");
        }

        if (outputDirectory != null) {
            fileOutput.close();
        }
    }

    /**
     * After data has been loaded from a file or via JDBC, create any derived data
     *
     */
    private void loadFromSQLInserts() throws Exception {
    	InputStream is = getClass().getResourceAsStream("insert.sql");
        try {
            final InputStreamReader reader = new InputStreamReader(is);
            final BufferedReader bufferedReader = new BufferedReader(reader);

            String line;
            int lineNumber = 0;
            Util.discard(lineNumber);

            StringBuffer statement = new StringBuffer();

            String fromQuoteChar = null;
            String toQuoteChar = null;
        	if (sqlQuery.getDialect().isMySQL()) {
        		toQuoteChar = "`";
        	} else {
        		toQuoteChar = "\"";
        	}

            while ((line = bufferedReader.readLine()) != null) {
                ++lineNumber;

                line = line.trim();
                if (line.startsWith("#") || line.length() == 0) {
                    continue;
                }

                if (fromQuoteChar == null) {
                	if (line.indexOf('`') >=0) {
                		fromQuoteChar = "`";
                	} else if (line.indexOf('"') >=0) {
                		fromQuoteChar = "\"";
                	}
                }

                if (fromQuoteChar != null && fromQuoteChar != toQuoteChar) {
                	line = line.replaceAll(fromQuoteChar, toQuoteChar);
                }

                // End of statement
                if (line.charAt(line.length() - 1) == ';') {
                    statement.append(" ")
						.append(line.substring(0, line.length() - 1));
                    executeDDL(statement.toString());
                    statement = new StringBuffer();

                } else {
                    statement.append(" ")
					.append(line.substring(0, line.length()));
                }
            }

            if (statement.length() > 0) {
                executeDDL(statement.toString());
            }

        } finally {
            if (is != null) {
                is.close();
            }
        }

    }

    /**
     * Read the given table from the input RDBMS and output to destination
     * RDBMS or file
     *
     * @param name      name of table
     * @param columns   columns to be read/output
     * @return          #rows inserted
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
        LOGGER.debug("Input table SQL: " + ddl);

        ResultSet rs = statement.executeQuery(ddl);

        String[] batch = new String[inputBatchSize];
        int batchSize = 0;
        boolean displayedInsert = false;

        while (rs.next()) {
            /*
             * Get a batch of insert statements, then save a batch
             */

            String insertStatement = createInsertStatement(rs, name, columns);
            if (!displayedInsert && LOGGER.isDebugEnabled()) {
            	LOGGER.debug("Example Insert statement: " + insertStatement);
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
     * @param rs            ResultSet of input RDBMS
     * @param name          name of table
     * @param columns       column definitions for INSERT statement
     * @return String       the INSERT statement
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
     *      Execute the given set of SQL statements
     *
     * Otherwise,
     *      output the statements to a file.
     *
     * @param batch         SQL statements to execute
     * @param batchSize     # SQL statements to execute
     * @return              # SQL statements executed
     * @throws IOException
     * @throws SQLException
     */
    private int writeBatch(String[] batch, int batchSize) throws IOException, SQLException {
        if (outputDirectory != null) {
            for (int i = 0; i < batchSize; i++) {
                fileOutput.write(batch[i]);
                fileOutput.write(";" + nl);
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
                        LOGGER.error("Error in SQL batch: " + batch[i]);
                    }
                    throw e;
                }
                int updates = 0;
                for (int i = 0; i < updateCounts.length; updates += updateCounts[i], i++) {
                    if (updateCounts[i] == 0) {
                    	LOGGER.error("Error in SQL: " + batch[i]);
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
            LOGGER.error("No input file: " + file);
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
        createIndex(false, "customer", "i_cust_acct_num", new String[] {"account_num"});
        createIndex(false, "customer", "i_customer_fname", new String[] {"fname"});
        createIndex(false, "customer", "i_customer_lname", new String[] {"lname"});
        createIndex(false, "customer", "i_cust_child_home", new String[] {"num_children_at_home"});
        createIndex(true, "customer", "i_customer_id", new String[] {"customer_id"});
        createIndex(false, "customer", "i_cust_postal_code", new String[] {"postal_code"});
        createIndex(false, "customer", "i_cust_region_id", new String[] {"customer_region_id"});
        createIndex(true, "department", "i_department_id", new String[] {"department_id"});
        createIndex(true, "employee", "i_employee_id", new String[] {"employee_id"});
        createIndex(false, "employee", "i_empl_dept_id", new String[] {"department_id"});
        createIndex(false, "employee", "i_empl_store_id", new String[] {"store_id"});
        createIndex(false, "employee", "i_empl_super_id", new String[] {"supervisor_id"});
        createIndex(true, "employee_closure", "i_empl_closure", new String[] {"supervisor_id", "employee_id"});
        createIndex(false, "employee_closure", "i_empl_closure_emp", new String[] {"employee_id"});
        createIndex(false, "expense_fact", "i_expense_store_id", new String[] {"store_id"});
        createIndex(false, "expense_fact", "i_expense_acct_id", new String[] {"account_id"});
        createIndex(false, "expense_fact", "i_expense_time_id", new String[] {"time_id"});
        createIndex(false, "inventory_fact_1997", "i_inv_97_prod_id", new String[] {"product_id"});
        createIndex(false, "inventory_fact_1997", "i_inv_97_store_id", new String[] {"store_id"});
        createIndex(false, "inventory_fact_1997", "i_inv_97_time_id", new String[] {"time_id"});
        createIndex(false, "inventory_fact_1997", "i_inv_97_wrhse_id", new String[] {"warehouse_id"});
        createIndex(false, "inventory_fact_1998", "i_inv_98_prod_id", new String[] {"product_id"});
        createIndex(false, "inventory_fact_1998", "i_inv_98_store_id", new String[] {"store_id"});
        createIndex(false, "inventory_fact_1998", "i_inv_98_time_id", new String[] {"time_id"});
        createIndex(false, "inventory_fact_1998", "i_inv_98_wrhse_id", new String[] {"warehouse_id"});
        createIndex(true, "position", "i_position_id", new String[] {"position_id"});
        createIndex(false, "product", "i_prod_brand_name", new String[] {"brand_name"});
        createIndex(true, "product", "i_product_id", new String[] {"product_id"});
        createIndex(false, "product", "i_prod_class_id", new String[] {"product_class_id"});
        createIndex(false, "product", "i_product_name", new String[] {"product_name"});
        createIndex(false, "product", "i_product_SKU", new String[] {"SKU"});
        createIndex(true, "promotion", "i_promotion_id", new String[] {"promotion_id"});
        createIndex(false, "promotion", "i_promo_dist_id", new String[] {"promotion_district_id"});
        createIndex(true, "reserve_employee", "i_rsrv_empl_id", new String[] {"employee_id"});
        createIndex(false, "reserve_employee", "i_rsrv_empl_dept", new String[] {"department_id"});
        createIndex(false, "reserve_employee", "i_rsrv_empl_store", new String[] {"store_id"});
        createIndex(false, "reserve_employee", "i_rsrv_empl_sup", new String[] {"supervisor_id"});
        createIndex(false, "salary", "i_salary_pay_date", new String[] {"pay_date"});
        createIndex(false, "salary", "i_salary_employee", new String[] {"employee_id"});
        createIndex(false, "sales_fact_1997", "i_sls_97_cust_id", new String[] {"customer_id"});
        createIndex(false, "sales_fact_1997", "i_sls_97_prod_id", new String[] {"product_id"});
        createIndex(false, "sales_fact_1997", "i_sls_97_promo_id", new String[] {"promotion_id"});
        createIndex(false, "sales_fact_1997", "i_sls_97_store_id", new String[] {"store_id"});
        createIndex(false, "sales_fact_1997", "i_sls_97_time_id", new String[] {"time_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sls_dec98_cust", new String[] {"customer_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sls_dec98_prod", new String[] {"product_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sls_dec98_promo", new String[] {"promotion_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sls_dec98_store", new String[] {"store_id"});
        createIndex(false, "sales_fact_dec_1998", "i_sls_dec98_time", new String[] {"time_id"});
        createIndex(false, "sales_fact_1998", "i_sls_98_cust_id", new String[] {"customer_id"});
        createIndex(false, "sales_fact_1998", "i_sls_1998_prod_id", new String[] {"product_id"});
        createIndex(false, "sales_fact_1998", "i_sls_1998_promo", new String[] {"promotion_id"});
        createIndex(false, "sales_fact_1998", "i_sls_1998_store", new String[] {"store_id"});
        createIndex(false, "sales_fact_1998", "i_sls_1998_time_id", new String[] {"time_id"});
        createIndex(true, "store", "i_store_id", new String[] {"store_id"});
        createIndex(false, "store", "i_store_region_id", new String[] {"region_id"});
        createIndex(true, "store_ragged", "i_store_raggd_id", new String[] {"store_id"});
        createIndex(false, "store_ragged", "i_store_rggd_reg", new String[] {"region_id"});
        createIndex(true, "time_by_day", "i_time_id", new String[] {"time_id"});
        createIndex(true, "time_by_day", "i_time_day", new String[] {"the_date"});
        createIndex(false, "time_by_day", "i_time_year", new String[] {"the_year"});
        createIndex(false, "time_by_day", "i_time_quarter", new String[] {"quarter"});
        createIndex(false, "time_by_day", "i_time_month", new String[] {"month_of_year"});

        createIndex(false, "agg_pl_01_sales_fact_1997", "i_sls_97_pl_01_cust", new String[] {"customer_id"});
        createIndex(false, "agg_pl_01_sales_fact_1997", "i_sls_97_pl_01_prod", new String[] {"product_id"});
        createIndex(false, "agg_pl_01_sales_fact_1997", "i_sls_97_pl_01_time", new String[] {"time_id"});

        createIndex(false, "agg_ll_01_sales_fact_1997", "i_sls_97_ll_01_cust", new String[] {"customer_id"});
        createIndex(false, "agg_ll_01_sales_fact_1997", "i_sls_97_ll_01_prod", new String[] {"product_id"});
        createIndex(false, "agg_ll_01_sales_fact_1997", "i_sls_97_ll_01_time", new String[] {"time_id"});

        createIndex(false, "agg_l_05_sales_fact_1997", "i_sls_97_l_05_cust", new String[] {"customer_id"});
        createIndex(false, "agg_l_05_sales_fact_1997", "i_sls_97_l_05_prod", new String[] {"product_id"});
        createIndex(false, "agg_l_05_sales_fact_1997", "i_sls_97_l_05_promo", new String[] {"promotion_id"});
        createIndex(false, "agg_l_05_sales_fact_1997", "i_sls_97_l_05_store", new String[] {"store_id"});

        createIndex(false, "agg_c_14_sales_fact_1997", "i_sls_97_c_14_cust", new String[] {"customer_id"});
        createIndex(false, "agg_c_14_sales_fact_1997", "i_sls_97_c_14_prod", new String[] {"product_id"});
        createIndex(false, "agg_c_14_sales_fact_1997", "i_sls_97_c_14_promo", new String[] {"promotion_id"});
        createIndex(false, "agg_c_14_sales_fact_1997", "i_sls_97_c_14_store", new String[] {"store_id"});

        createIndex(false, "agg_lc_100_sales_fact_1997", "i_sls_97_lc_100_cust", new String[] {"customer_id"});
        createIndex(false, "agg_lc_100_sales_fact_1997", "i_sls_97_lc_100_prod", new String[] {"product_id"});

        createIndex(false, "agg_c_special_sales_fact_1997", "i_sls_97_spec_cust", new String[] {"customer_id"});
        createIndex(false, "agg_c_special_sales_fact_1997", "i_sls_97_spec_prod", new String[] {"product_id"});
        createIndex(false, "agg_c_special_sales_fact_1997", "i_sls_97_spec_promo", new String[] {"promotion_id"});
        createIndex(false, "agg_c_special_sales_fact_1997", "i_sls_97_spec_store", new String[] {"store_id"});

        if (outputDirectory != null) {
            fileOutput.close();
        }
    }

    /**
     *
     * If we are outputting to JDBC,
     *      Execute the CREATE INDEX statement
     *
     * Otherwise,
     *      output the statement to a file.
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

            // Only do aggregate tables
            if (populationQueries && !aggregateTableMetadataToLoad.containsKey(tableName)) {
            	return;
            }

            StringBuffer buf = new StringBuffer();

            // If we're [re]creating tables, no need to drop indexes.
            if (jdbcOutput && !tables) {
                try {
                    buf.append("DROP INDEX ")
                        .append(quoteId(indexName));
                    if (sqlQuery.getDialect().isMySQL()) {
                        buf.append(" ON ")
                            .append(quoteId(tableName));
                    }
                    final String deleteDDL = buf.toString();
                    executeDDL(deleteDDL);
                } catch (Exception e1) {
                	LOGGER.info("Index Drop failed for " + tableName + ", " + indexName + " : but continue");
                }
            }

            buf = new StringBuffer();
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
            final String createDDL = buf.toString();
            executeDDL(createDDL);
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

        //  Aggregate tables

        createTable("agg_pl_01_sales_fact_1997", new Column[] {
	        new Column("product_id", "INTEGER", "NOT NULL"),
	        new Column("time_id", "INTEGER", "NOT NULL"),
	        new Column("customer_id", "INTEGER", "NOT NULL"),
	        new Column("store_sales_sum", "DECIMAL(10,4)", "NOT NULL"),
	        new Column("store_cost_sum", "DECIMAL(10,4)", "NOT NULL"),
	        new Column("unit_sales_sum", "DECIMAL(10,4)", "NOT NULL"),
	        new Column("fact_count", "INTEGER", "NOT NULL"),
        }, false, true);
        createTable("agg_ll_01_sales_fact_1997", new Column[] {
	        new Column("product_id", "INTEGER", "NOT NULL"),
	        new Column("time_id", "INTEGER", "NOT NULL"),
	        new Column("customer_id", "INTEGER", "NOT NULL"),
	        new Column("store_sales", "DECIMAL(10,4)", "NOT NULL"),
	        new Column("store_cost", "DECIMAL(10,4)", "NOT NULL"),
	        new Column("unit_sales", "DECIMAL(10,4)", "NOT NULL"),
	        new Column("fact_count", "INTEGER", "NOT NULL"),
        }, false, true);
        createTable("agg_l_03_sales_fact_1997", new Column[] {
    	        new Column("time_id", "INTEGER", "NOT NULL"),
    	        new Column("customer_id", "INTEGER", "NOT NULL"),
    	        new Column("store_sales", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("store_cost", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("unit_sales", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("fact_count", "INTEGER", "NOT NULL"),
            }, false, true);
        createTable("agg_l_05_sales_fact_1997", new Column[] {
    	        new Column("product_id", "INTEGER", "NOT NULL"),
    	        new Column("customer_id", "INTEGER", "NOT NULL"),
    	        new Column("promotion_id", "INTEGER", "NOT NULL"),
    	        new Column("store_id", "INTEGER", "NOT NULL"),
    	        new Column("store_sales", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("store_cost", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("unit_sales", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("fact_count", "INTEGER", "NOT NULL"),
            }, false, true);
        createTable("agg_c_14_sales_fact_1997", new Column[] {
    	        new Column("product_id", "INTEGER", "NOT NULL"),
    	        new Column("customer_id", "INTEGER", "NOT NULL"),
    	        new Column("store_id", "INTEGER", "NOT NULL"),
    	        new Column("promotion_id", "INTEGER", "NOT NULL"),
    	        new Column("month_of_year", "SMALLINT", "NOT NULL"),
    	        new Column("quarter", "VARCHAR(30)", "NOT NULL"),
		        new Column("the_year", "SMALLINT", "NOT NULL"),
    	        new Column("store_sales", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("store_cost", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("unit_sales", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("fact_count", "INTEGER", "NOT NULL"),
            }, false, true);
        createTable("agg_lc_100_sales_fact_1997", new Column[] {
    	        new Column("product_id", "INTEGER", "NOT NULL"),
    	        new Column("customer_id", "INTEGER", "NOT NULL"),
    	        new Column("quarter", "VARCHAR(30)", "NOT NULL"),
		        new Column("the_year", "SMALLINT", "NOT NULL"),
    	        new Column("store_sales", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("store_cost", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("unit_sales", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("fact_count", "INTEGER", "NOT NULL"),
            }, false, true);
        createTable("agg_c_special_sales_fact_1997", new Column[] {
    	        new Column("product_id", "INTEGER", "NOT NULL"),
    	        new Column("promotion_id", "INTEGER", "NOT NULL"),
    	        new Column("customer_id", "INTEGER", "NOT NULL"),
    	        new Column("store_id", "INTEGER", "NOT NULL"),
    	        new Column("time_month", "SMALLINT", "NOT NULL"),
    	        new Column("time_quarter", "VARCHAR(30)", "NOT NULL"),
		        new Column("time_year", "SMALLINT", "NOT NULL"),
    	        new Column("store_sales_sum", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("store_cost_sum", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("unit_sales_sum", "DECIMAL(10,4)", "NOT NULL"),
    	        new Column("fact_count", "INTEGER", "NOT NULL"),
            }, false, true);

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
          new Column("fullname", "VARCHAR(60)", "NOT NULL"),
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
     *      Execute a DROP TABLE statement
     *      Execute the CREATE TABLE statement
     *
     * Otherwise,
     *      output the statement to a file.
     *
     * @param name
     * @param columns
     */

    private void createTable(String name, Column[] columns) {
    	createTable(name, columns,  true, false);
    }

    private void createTable(String name, Column[] columns,  boolean loadData, boolean aggregate) {
        try {

        	// Store this metadata if we are going to load the table
        	// from JDBC or a file

        	if (loadData) {
        		tableMetadataToLoad.put(name, columns);
        	}

        	if (aggregate) {
        		aggregateTableMetadataToLoad.put(name, columns);
        	}

            if (!tables) {
                if (data && jdbcOutput) {
                	if (populationQueries && !aggregate) {
                		return;
                	}
                    // We're going to load the data without [re]creating
                    // the table, so let's remove the data.
                    try {
                        executeDDL("DELETE FROM " + quoteId(name));
                    } catch (SQLException e) {
                        throw MondrianResource.instance().newCreateTableFailed(name, e);
                    }
                }
                return;

            } else if (populationQueries && !aggregate) {
                // only create the aggregate tables if we are running
            	// -tables -populationQueries
            	return;
            }
            // If table does not exist, that is OK
            try {
            	executeDDL("DROP TABLE " + quoteId(name));
            } catch (Exception e) {
            	LOGGER.debug("Drop of " + name + " failed. Ignored");
            }

            // Define the table.
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
            executeDDL(ddl);
        } catch (Exception e) {
            throw MondrianResource.instance().newCreateTableFailed(name, e);
        }
    }

    private void executeDDL(String ddl) throws Exception {
        LOGGER.info(ddl);

        if (jdbcOutput) {
            final Statement statement = connection.createStatement();
            statement.execute(ddl);
        } else {
            fileOutput.write(ddl);
            fileOutput.write(";" + nl);
        }

    }

    /**
     * Quote the given SQL identifier suitable for the output DBMS.
     * @param name
     * @return
     */
    private String quoteId(String name) {
        return sqlQuery.getDialect().quoteIdentifier(name);
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
     * @param rs        ResultSet row to process
     * @param column    Column to process
     * @return          String representation of column value
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
                    LOGGER.error("CCE: "  + column.name + " to Long from: " + obj.getClass().getName() + " - " + obj.toString());
                    throw cce;
                }
            } else {
                try {
                    Integer result = (Integer) obj;
                    return result.toString();
                } catch (ClassCastException cce) {
                	LOGGER.error("CCE: "  + column.name + " to Integer from: " + obj.getClass().getName() + " - " + obj.toString());
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
                	LOGGER.error("CCE: "  + column.name + " to Integer from: " + obj.getClass().getName() + " - " + obj.toString());
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
                	LOGGER.error("CCE: "  + column.name + " to Double from: " + obj.getClass().getName() + " - " + obj.toString());
                    throw cce;
                }
            } else {
                try {
                    Long result = (Long) obj;
                    return result.toString();
                } catch (ClassCastException cce) {
                	LOGGER.error("CCE: "  + column.name + " to Long from: " + obj.getClass().getName() + " - " + obj.toString());
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
            if (sqlQuery.getDialect().isOracle()) {
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
            if (sqlQuery.getDialect().isOracle()) {
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
                	LOGGER.error("CCE: "  + column.name + " to Double from: " + obj.getClass().getName() + " - " + obj.toString());
                    throw cce;
                }
            } else {
                // should be (obj.getClass() == BigDecimal.class)
                try {
                    BigDecimal result = (BigDecimal) obj;
                    return formatter.format(result);
                } catch (ClassCastException cce) {
                	LOGGER.error("CCE: "  + column.name + " to BigDecimal from: " + obj.getClass().getName() + " - " + obj.toString());
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
            if (sqlQuery.getDialect().isOracle()) {
                return "TIMESTAMP " + columnValue;
            }

        /*
         * Output for a DATE
         */
        } else if (columnType.startsWith("DATE")) {
            if (sqlQuery.getDialect().isOracle()) {
                return "DATE " + columnValue;
            }

        /*
         * Output for a BOOLEAN (Postgres) or BIT (other DBMSs)
         *
         * FIXME This code assumes that only a boolean column would
         * map onto booleanColumnType. It would be better if we had a
         * logical and physical type for each column.
         */
        } else if (columnType.equals(booleanColumnType)) {
            String trimmedValue = columnValue.trim();
            if (!sqlQuery.getDialect().isMySQL() &&
                    !sqlQuery.getDialect().isOracle() &&
                    !sqlQuery.getDialect().isFirebird()) {
                if (trimmedValue.equals("1")) {
                    return "true";
                } else if (trimmedValue.equals("0")) {
                    return "false";
                }
            } else {
                if (trimmedValue.equals("true")) {
                    return "1";
                } else if (trimmedValue.equals("false")) {
                    return "0";
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
     *         for SQL
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


// End MondrianFoodMartLoader.java
