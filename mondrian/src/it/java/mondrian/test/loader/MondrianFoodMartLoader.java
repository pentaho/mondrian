/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2016 Pentaho
// All Rights Reserved.
*/
package mondrian.test.loader;

import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapUtil;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.*;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.Date;
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
 * <li>LucidDB-0.9.2</li>
 *
 * <li>MySQL 3.23 using MySQL-connector/J 3.0.16</li>
 *
 * <li>MySQL 4.15 using MySQL-connector/J 3.0.16</li>
 *
 * <li>Oracle 10g using ojdbc14.jar</li>
 *
 * <li>Postgres 8.0 beta using postgresql-driver-jdbc3-74-214.jar</li>
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
 *     --aggregates -tables -data -indexes -jdbcDrivers=com.mysql.jdbc.Driver
 *     -inputJdbcURL=jdbc:odbc:MondrianFoodMart
 *     -outputJdbcURL=jdbc:mysql://localhost/foodmart
 * </code></blockquote>
 *
 * <h4>FirebirdSQL</h4>
 *
 * <blockquote><code>
 * $ /firebird/bin/isql -u SYSDBA -p masterkey<br/>
 * Use CONNECT or CREATE DATABASE to specify a database<br/>
 * SQL&gt; CREATE DATABASE '/mondrian/foodmart.gdb';<br/>
 * SQL&gt; QUIT;<br/>
 * $ java -cp "/mondrian/lib/mondrian.jar:/mondrian/lib/log4j.jar:
 * /mondrian/lib/eigenbase-xom.jar:/mondrian/lib/eigenbase-resgen.jar:
 * /mondrian/lib/eigenbase-properties.jar:/jdbc/fb/firebirdsql-full.jar"
 *    mondrian.test.loader.MondrianFoodMartLoader
 *    -tables -data -indexes
 *    -jdbcDrivers="org.firebirdsql.jdbc.FBDriver"
 *    -inputFile="/mondrian/demo/FoodMartCreateData.sql"
 *    -outputJdbcURL="jdbc:firebirdsql:localhost/3050:/mondrian/foodmart.gdb"
 *    -inputJdbcUser=SYSDBA
 *    -inputJdbcPassword=masterkey
 * </code></blockquote>
 *
 * <p>See {@code bin/loadFoodMart.sh} for examples of command lines for other
 * databases.
 *
 * @author jhyde
 * @since 23 December, 2004
 */
public class MondrianFoodMartLoader {
    // Constants

    private static final Logger LOGGER =
        Logger.getLogger(MondrianFoodMartLoader.class);
    private static final String nl = Util.nl;

    // Fields

    private static final Pattern decimalDataTypeRegex =
        Pattern.compile("DECIMAL\\((.*),(.*)\\)");
    private static final DecimalFormat integerFormatter =
        new DecimalFormat(decimalFormat(15, 0));
    private static final String dateFormatString = "yyyy-MM-dd";
    private static final DateFormat dateFormatter =
        new SimpleDateFormat(dateFormatString);

    private String jdbcDrivers;
    private String jdbcURL;
    private String userName;
    private String password;
    private String schema = null;
    private String inputJdbcURL;
    private String inputUserName;
    private String inputPassword;
    private String inputSchema = null;
    private String inputFile;
    private String outputDirectory;
    private boolean aggregates = false;
    private boolean tables = false;
    private boolean indexes = false;
    private boolean data = false;
    private long pauseMillis = 0;
    private boolean jdbcInput = false;
    private boolean jdbcOutput = false;
    private boolean populationQueries = false;
    private boolean analyze = false;
    private Pattern include = null;
    private Pattern exclude = null;
    private boolean generateUniqueConstraints = false;
    private int outputBatchSize = -1;
    private Connection connection;
    private Connection inputConnection;

    private FileWriter fileOutput = null;
    private File file;

    private final Map<String, Column[]> tableMetadataToLoad =
        new HashMap<String, Column[]>();
    private final Map<String, Column[]> aggregateTableMetadataToLoad =
        new HashMap<String, Column[]>();
    private final Map<String, List<UniqueConstraint>> tableConstraints =
        new HashMap<String, List<UniqueConstraint>>();
    private Dialect dialect;
    private boolean infobrightLoad;
    private long lastUpdate = 0;

    /**
     * Creates an instance of the loader and parses the command-line options.
     *
     * @param args Command-line options
     */
    public MondrianFoodMartLoader(String[] args) {
        if (args.length == 0) {
            usage();
            return;
        }

        StringBuilder errorMessage = new StringBuilder();
        StringBuilder parametersMessage = new StringBuilder();

        // Add a console appender for error messages.
        final ConsoleAppender consoleAppender =
            new ConsoleAppender(
                // Formats the message on its own line,
                // omits timestamp, priority etc.
                new PatternLayout("%m%n"),
                "System.out");
        consoleAppender.setThreshold(Level.ERROR);
        LOGGER.addAppender(consoleAppender);

        for (String arg : args) {
            if (arg.equals("-verbose")) {
                // Make sure the logger is passing at least debug events.
                consoleAppender.setThreshold(Level.DEBUG);
                if (!LOGGER.isDebugEnabled()) {
                    LOGGER.setLevel(Level.DEBUG);
                }
            } else if (arg.equals("-aggregates")) {
                aggregates = true;
            } else if (arg.equals("-tables")) {
                tables = true;
            } else if (arg.equals("-data")) {
                data = true;
            } else if (arg.startsWith("-pauseMillis=")) {
                pauseMillis =
                    Long.parseLong(
                        arg.substring("-pauseMillis=".length()));
            } else if (arg.equals("-indexes")) {
                indexes = true;
            } else if (arg.equals("-populationQueries")) {
                populationQueries = true;
            } else if (arg.equals("-analyze")) {
                analyze = true;
            } else if (arg.startsWith("-include=")) {
                include = Pattern.compile(arg.substring("-include=".length()));
            } else if (arg.startsWith("-exclude=")) {
                exclude = Pattern.compile(arg.substring("-exclude=".length()));
            } else if (arg.startsWith("-jdbcDrivers=")) {
                jdbcDrivers = arg.substring("-jdbcDrivers=".length());
            } else if (arg.startsWith("-outputJdbcURL=")) {
                jdbcURL = arg.substring("-outputJdbcURL=".length());
            } else if (arg.startsWith("-outputJdbcUser=")) {
                userName = arg.substring("-outputJdbcUser=".length());
            } else if (arg.startsWith("-outputJdbcPassword=")) {
                password = arg.substring("-outputJdbcPassword=".length());
            } else if (arg.startsWith("-outputJdbcSchema=")) {
                schema = arg.substring("-outputJdbcSchema=".length());
                if (schema.trim().length() == 0) {
                    schema = null;
                }
            } else if (arg.startsWith("-inputJdbcURL=")) {
                inputJdbcURL = arg.substring("-inputJdbcURL=".length());
            } else if (arg.startsWith("-inputJdbcUser=")) {
                inputUserName = arg.substring("-inputJdbcUser=".length());
            } else if (arg.startsWith("-inputJdbcPassword=")) {
                inputPassword = arg.substring("-inputJdbcPassword=".length());
            } else if (arg.startsWith("-inputJdbcSchema=")) {
                inputSchema = arg.substring("-inputJdbcSchema=".length());
                if (inputSchema.trim().length() == 0) {
                    inputSchema = null;
                }
            } else if (arg.startsWith("-inputFile=")) {
                inputFile = arg.substring("-inputFile=".length());
            } else if (arg.startsWith("-outputDirectory=")) {
                outputDirectory = arg.substring("-outputDirectory=".length());
            } else if (arg.startsWith("-outputJdbcBatchSize=")) {
                outputBatchSize =
                    Integer.parseInt(
                        arg.substring("-outputJdbcBatchSize=".length()));
            } else {
                errorMessage.append("unknown arg: ").append(arg).append(nl);
            }

            if (LOGGER.isInfoEnabled()) {
                parametersMessage.append("\t").append(arg).append(nl);
            }
        }
        if (inputJdbcURL != null) {
            jdbcInput = true;
            if (inputFile != null) {
                errorMessage.append(
                    "Specified both an input JDBC connection and an input file");
            }
        }
        if (jdbcURL != null && outputDirectory == null) {
            jdbcOutput = true;
        }
        if (errorMessage.length() > 0) {
            usage();
            throw MondrianResource.instance().MissingArg.ex(
                errorMessage.toString());
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Parameters: " + nl + parametersMessage.toString());
        }
    }

    /**
     * Prints help.
     */
    public void usage() {
        String[] lines = {
            "Usage: MondrianFoodMartLoader "
            + "[-verbose] [-tables] [-data] [-indexes] [-populationQueries] "
            + "[-pauseMillis=<n>] "
            + "[-include=<regexp>] "
            + "[-exclude=<regexp>] "
            + "[-pauseMillis=<n>] "
            + "-jdbcDrivers=<jdbcDrivers> "
            + "-outputJdbcURL=<jdbcURL> "
            + "[-outputJdbcUser=user] "
            + "[-outputJdbcPassword=password] "
            + "[-outputJdbcSchema=schema] "
            + "[-outputJdbcBatchSize=<batch size>] "
            + "| "
            + "[-outputDirectory=<directory name>] "
            + "["
            + "   [-inputJdbcURL=<jdbcURL> [-inputJdbcUser=user] "
            + " [-inputJdbcPassword=password] [-inputJdbcSchema=schema]]"
            + "   | "
            + "   [-inputfile=<file name>]"
            + "]"
            + "\n"
            + "  <jdbcURL>             JDBC connect string for DB.\n"
            + "  [user]                JDBC user name for DB.\n"
            + "  [password]            JDBC password for user for DB.\n"
            + "                        If no source DB parameters are given, assumes data\n"
            + "                        comes from file.\n"
            + "  [schema]              schema overriding connection defaults\n"
            + "  [file name]           File containing test data - INSERT statements in MySQL\n"
            + "                        format. If no input file name or input JDBC parameters\n"
            + "                        are given, assume insert statements come from\n"
            + "                        demo/FoodMartCreateData.zip file\n"
            + "  [outputDirectory]     Where FoodMartCreateTables.sql, FoodMartCreateData.sql\n"
            + "                        and FoodMartCreateIndexes.sql will be created.\n"
            + "  -outputJdbcBatchSize=<batch size>\n"
            + "                        Size of JDBC batch updates (default 50 records).\n"
            + "  -jdbcDrivers=<jdbcDrivers>\n"
            + "                        Comma-separated list of JDBC drivers;\n"
            + "                        they must be on the classpath.\n"
            + "  -verbose              Verbose mode.\n"
            + "  -aggregates           If specified, create aggregate tables and indexes for them.\n"
            + "  -tables               If specified, drop and create the tables.\n"
            + "  -data                 If specified, load the data.\n"
            + "  -indexes              If specified, drop and create the indexes.\n"
            + "  -populationQueries    If specified, run the data loading queries. Runs by\n"
            + "                        default if -data is specified.\n"
            + "  -analyze              If specified, analyze tables after populating and indexing\n"
            + "                        them.\n"
            + "  -pauseMillis=<n>      Pause n milliseconds between batches;\n"
            + "                        if not specified, or 0, do not pause.\n"
            + "  -include=<regexp>     Create, load, and index only tables whose name\n"
            + "                        matches regular expression\n"
            + "  -exclude=<regexp>     Create, load, and index only tables whose name\n"
            + "                        does not match regular expression\n"
            + "                        if not specified, or 0, do not pause.\n"
            + "\n"
            + "To load data in trickle mode, first run with '-exclude=sales_fact_1997'\n"
            + "then run with '-include=sales_fact_1997 -pauseMillis=1 -outputJdbcBatchSize=1\n"
        };
        for (String s : lines) {
            System.out.println(s);
        }
    }

    /**
     * Command-line entry point.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        // Set locale to English, so that '.' and ',' in numbers are parsed
        // correctly.
        Locale.setDefault(Locale.ENGLISH);

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
     */
    private void load() throws Exception {
        if (!StringUtils.isBlank(jdbcDrivers)) {
            RolapUtil.loadDrivers(jdbcDrivers);
        }

        if (userName == null) {
            connection = DriverManager.getConnection(jdbcURL);
        } else {
            connection =
                DriverManager.getConnection(jdbcURL, userName, password);
        }

        if (jdbcInput) {
            if (inputUserName == null) {
                inputConnection = DriverManager.getConnection(inputJdbcURL);
            } else {
                inputConnection = DriverManager.getConnection(
                    inputJdbcURL, inputUserName, inputPassword);
            }
        }
        final DatabaseMetaData metaData = connection.getMetaData();

        String productName = metaData.getDatabaseProductName();
        String version = metaData.getDatabaseProductVersion();

        LOGGER.info(
            "Output connection is " + productName
            + ", version: " + version);

        dialect = DialectManager.createDialect(null, connection);

        LOGGER.info(
            "Mondrian Dialect is " + dialect
            + ", detected database product: " + dialect.getDatabaseProduct());

        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.INFOBRIGHT
            && indexes)
        {
            System.out.println("Infobright engine detected: ignoring indexes");
            indexes = false;
        }

        if (outputBatchSize == -1) {
            // No explicit batch size was set by user, so assign a good
            // default now
            if (dialect.getDatabaseProduct()
                == Dialect.DatabaseProduct.LUCIDDB)
            {
                // LucidDB column-store writes perform better with large batches
                outputBatchSize = 1000;
            } else {
                outputBatchSize = 50;
            }
        }

        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.LUCIDDB) {
            // LucidDB doesn't support CREATE UNIQUE INDEX, but it
            // does support standard UNIQUE constraints
            generateUniqueConstraints = true;
        }

        try {
            final Condition<String> tableFilter;
            if (include != null || exclude != null) {
                tableFilter = new Condition<String>() {
                    public boolean test(String tableName) {
                        if (include != null) {
                            if (!include.matcher(tableName).matches()) {
                                return false;
                            }
                        }
                        if (exclude != null) {
                            if (!exclude.matcher(tableName).matches()) {
                                return true;
                            }
                        }
                        // Table name matched the inclusion criterion
                        // (or everything was included)
                        // and did not match the exclusion criterion
                        // (or nothing was excluded),
                        // therefore is included.
                        return true;
                    }
                };
            } else {
                tableFilter = new Condition<String>() {
                    public boolean test(String s) {
                        return true;
                    }
                };
            }

            if (generateUniqueConstraints) {
                // Initialize tableConstraints
                createIndexes(false, false, tableFilter);
            }

            // This also initializes tableMetadataToLoad
            createTables(tableFilter);

            if (data) {
                if (!populationQueries) {
                    if (jdbcInput) {
                        loadDataFromJdbcInput(
                            tableFilter, pauseMillis, outputBatchSize);
                    } else {
                        loadDataFromFile(
                            tableFilter, pauseMillis, outputBatchSize);
                    }
                }
                // Index the base tables before running queries to populate
                // the summary tables.
                if (indexes) {
                    createIndexes(true, false, tableFilter);
                }
                loadFromSqlInserts();
            } else {
                // Create indexes without loading data.
                if (indexes) {
                    createIndexes(true, false, tableFilter);
                }
            }

            if (indexes && aggregates) {
                createIndexes(false, true, tableFilter);
            }

            if (analyze) {
                analyzeTables();
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
     * Parses a file of INSERT statements and output to the configured JDBC
     * connection or another file in the dialect of the target data source.
     *
     * <p>Assumes that the input INSERT statements are generated
     * by this loader by something like:</p>
     *
     * <blockquote><pre>
     * MondrianFoodLoader
     * -verbose -tables -data -indexes -analyze
     * -jdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver,com.mysql.jdbc.Driver
     * -inputJdbcURL=jdbc:odbc:MondrianFoodMart
     * -outputJdbcURL=jdbc:mysql://localhost/textload?user=root&password=myAdmin
     * -outputDirectory=C:\Temp\wip\Loader-Output</pre></blockquote>
     *
     * @param tableFilter Condition whether to load rows from a given table
     * @param pauseMillis How many milliseconds to pause between rows
     * @param batchSize How often to write/commit to the database
     */
    private void loadDataFromFile(
        Condition<String> tableFilter,
        long pauseMillis,
        int batchSize)
        throws Exception
    {
        InputStream is = openInputStream();

        if (is == null) {
            throw new Exception("No data file to process");
        }

        if (dialect.getDatabaseProduct()
            == Dialect.DatabaseProduct.INFOBRIGHT)
        {
            infobrightLoad = true;
            file = File.createTempFile("tmpfile", ".csv");
            fileOutput = new FileWriter(file);
        } else {
            infobrightLoad = false;
            if (outputDirectory != null) {
                file = new File(outputDirectory, "createData.sql");
                fileOutput = new FileWriter(file);
            }
        }
        try {
            final InputStreamReader reader = new InputStreamReader(is);
            final BufferedReader bufferedReader = new BufferedReader(reader);
            final Pattern mySQLRegex =
                Pattern.compile(
                    "INSERT INTO `([^ ]+)` \\((.*)\\) VALUES\\((.*)\\);");
            final Pattern doubleQuoteRegex =
                Pattern.compile(
                    "INSERT INTO \"([^ ]+)\" \\((.*)\\) VALUES\\((.*)\\);");
            String line;
            int lineNumber = 0;
            int tableRowCount = 0;
            String prevTable = null;
            String quotedTableName = null;
            String quotedColumnNames = null;
            Column[] orderedColumns = null;
            StringBuilder massagedLine = new StringBuilder();
            final List<String> batch = new ArrayList<String>(batchSize);

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
                //   INSERT INTO `foo` (`column1`,`column2`) VALUES (1, 'bar');
                // would yield
                //   tableName = "foo"
                //   columnNames = " `column1`,`column2` "
                //   values = "1, 'bar'"
                final Matcher matcher = regex.matcher(line);
                if (!matcher.matches()) {
                    throw MondrianResource.instance().InvalidInsertLine.ex(
                        lineNumber, line);
                }
                String tableName = matcher.group(1); // e.g. "foo"
                String columnNames = matcher.group(2);
                String values = matcher.group(3);

                // This is a table we're not interested in. Ignore the line.
                if (!tableFilter.test(tableName)) {
                    continue;
                }

                // If table just changed, flush the previous batch.
                if (!tableName.equals(prevTable)) {
                    writeBatch(batch, pauseMillis);
                    batch.clear();
                    afterTable(prevTable, tableRowCount);
                    tableRowCount = 0;
                    prevTable = tableName;
                    quotedTableName = quoteId(schema, tableName);
                    quotedColumnNames =
                        columnNames.replaceAll(
                            quoteChar,
                            dialect.getQuoteIdentifierString());
                    String[] splitColumnNames =
                        columnNames.replaceAll(quoteChar, "")
                            .replaceAll(" ", "").split(",");
                    Column[] columns = tableMetadataToLoad.get(tableName);

                    orderedColumns = new Column[columns.length];

                    for (int i = 0; i < splitColumnNames.length; i++) {
                        Column thisColumn = null;
                        for (int j = 0;
                            j < columns.length
                            && thisColumn == null;
                            j++)
                        {
                            if (columns[j].name.equalsIgnoreCase(
                                    splitColumnNames[i]))
                            {
                                thisColumn = columns[j];
                            }
                        }
                        if (thisColumn == null) {
                            throw new Exception(
                                "Unknown column in INSERT statement from file: "
                                    + splitColumnNames[i]);
                        } else {
                            orderedColumns[i] = thisColumn;
                        }
                    }
                }

                ++tableRowCount;
                if (pauseMillis > 0) {
                    Thread.sleep(pauseMillis);
                }

                if (infobrightLoad) {
                    massagedLine.setLength(0);
                    getMassagedValues(massagedLine, orderedColumns, values);
                    fileOutput.write(
                        massagedLine.toString()
                            .replaceAll("\"", "\\\"")
                            .replace('\'', '"')
                            .trim());
                    fileOutput.write(nl);
                } else {
                    massagedLine.setLength(0);
                    massagedLine
                        .append("INSERT INTO ")
                        .append(quotedTableName)
                        .append(" (")
                        .append(quotedColumnNames)
                        .append(") VALUES(");
                    getMassagedValues(massagedLine, orderedColumns, values);
                    massagedLine.append(")");

                    line = massagedLine.toString();

                    batch.add(line);
                    if (batch.size() >= batchSize) {
                        writeBatch(batch, pauseMillis);
                        batch.clear();
                        if (pauseMillis > 0) {
                            // Every ten seconds print an update.
                            final long t = System.currentTimeMillis();
                            if (t - lastUpdate > 10000) {
                                lastUpdate = t;
                                LOGGER.debug(
                                    tableName + ": wrote row #"
                                    + tableRowCount + ".");
                            }
                            Thread.sleep(pauseMillis);
                        }
                    }
                }
            }

            // Print summary of the final table.
            writeBatch(batch, pauseMillis);
            afterTable(prevTable, tableRowCount);
            tableRowCount = 0;
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    /**
     * Called after the last row of a table has been read into the batch.
     *
     * @param table Table name, or null if there is no previous table
     * @param tableRowCount Number of rows in the table
     * @throws IOException
     */
    private void afterTable(
        String table,
        int tableRowCount)
        throws IOException
    {
        if (table == null) {
            return;
        }
        if (!infobrightLoad) {
            LOGGER.info(
                "Table " + table
                + ": loaded " + tableRowCount + " rows.");
            return;
        }
        fileOutput.close();
        LOGGER.info(
            "Infobright bulk load: Table " + table
            + ": loaded " + tableRowCount + " rows.");
        final String sql =
            "LOAD DATA INFILE '"
            + file.getAbsolutePath().replaceAll("\\\\", "\\\\\\\\")
            + "' INTO TABLE "
            + (schema != null ? schema + "." : "")
            + table
            + " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
            + " ESCAPED BY '\\\\'";
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(
                sql);
        } catch (SQLException e) {
            throw new RuntimeException(
                "Error while executing statement: " + sql,
                e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }

        // Re-open for the next table.
        fileOutput = new FileWriter(file);
    }

    /**
     * Converts column values for a destination dialect.
     *
     * @param columns               column metadata for the table
     * @param values                the contents of the INSERT VALUES clause,
     *                              for example "34,67.89,'GHt''ab'".
     *                              These are in MySQL form.
     * @param buf                   Buffer in which to write values
     */
    private void getMassagedValues(
        StringBuilder buf,
        Column[] columns,
        String values) throws Exception
    {
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
                        individualValues[valuesPos] =
                            individualValues[valuesPos] + "," + splitValues[i];
                        inQuote = inQuote(splitValues[i], inQuote);
                    } else {
                        valuesPos++;
                        individualValues[valuesPos] = splitValues[i];
                        inQuote = inQuote(splitValues[i], inQuote);
                    }
                }
            }

            assert valuesPos + 1 == columns.length;
        }

        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            String value = individualValues[i];
            if (value != null && value.trim().equals("NULL")) {
                value = null;
            }
            buf.append(columnValue(value, columns[i]));
        }
    }

    /**
     * Returns whether we are inside a quoted string after reading a string.
     *
     * @param str String
     * @param nowInQuote Whether we are inside a quoted string initially
     * @return whether we are inside a quoted string after reading the string
     */
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

    /**
     * Loads data from a JDBC source.
     *
     * @param tableFilter Condition whether to load rows from a given table
     * @param pauseMillis How many milliseconds to pause between batches
     * @param batchSize How often to write/commit to the database
     * @throws Exception
     */
    private void loadDataFromJdbcInput(
        Condition<String> tableFilter,
        long pauseMillis,
        int batchSize)
        throws Exception
    {
        if (outputDirectory != null) {
            file = new File(outputDirectory, "createData.sql");
            fileOutput = new FileWriter(file);
        }

        /*
         * For each input table,
         *  read specified columns for all rows in the input connection
         *
         * For each row, insert a row
         */

        for (Map.Entry<String, Column[]> tableEntry
            : tableMetadataToLoad.entrySet())
        {
            final String tableName = tableEntry.getKey();
            if (!tableFilter.test(tableName)) {
                continue;
            }
            final Column[] tableColumns = tableEntry.getValue();
            int rowsAdded = loadTable(
                tableName, tableColumns, pauseMillis, batchSize);
            LOGGER.info(
                "Table " + tableName + ": loaded " + rowsAdded + " rows.");
        }

        if (outputDirectory != null) {
            fileOutput.close();
        }
    }

    /**
     * After data has been loaded from a file or via JDBC, creates any derived
     * data.
     */
    private void loadFromSqlInserts() throws Exception {
        InputStream is = getClass().getResourceAsStream("insert.sql");
        if (is == null) {
            is = new FileInputStream(
                new File("src/test/resources/mondrian/test/loader/insert.sql"));
        }
        if (is == null) {
            throw new Exception("Cannot find insert.sql in class path");
        }
        try {
            final InputStreamReader reader = new InputStreamReader(is);
            final BufferedReader bufferedReader = new BufferedReader(reader);

            String line;
            int lineNumber = 0;
            Util.discard(lineNumber);

            StringBuilder buf = new StringBuilder();

            String fromQuoteChar = null;
            String toQuoteChar = dialect.getQuoteIdentifierString();
            while ((line = bufferedReader.readLine()) != null) {
                ++lineNumber;

                line = line.trim();
                if (line.startsWith("#") || line.length() == 0) {
                    continue;
                }

                if (fromQuoteChar == null) {
                    if (line.indexOf('`') >= 0) {
                        fromQuoteChar = "`";
                    } else if (line.indexOf('"') >= 0) {
                        fromQuoteChar = "\"";
                    }
                }

                if (fromQuoteChar != null
                    && !fromQuoteChar.equals(toQuoteChar))
                {
                    line = line.replaceAll(fromQuoteChar, toQuoteChar);
                }

                // End of buf
                if (line.charAt(line.length() - 1) == ';') {
                    buf.append(" ")
                        .append(line.substring(0, line.length() - 1));

                    buf = updateSQLLineForSchema(buf);

                    executeDDL(buf.toString());
                    buf.setLength(0);

                } else {
                    buf.append(" ")
                    .append(line.substring(0, line.length()));
                }
            }

            if (buf.length() > 0) {
                executeDDL(buf.toString());
            }
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    private StringBuilder updateSQLLineForSchema(StringBuilder buf) {
        if (schema == null) {
            return buf;
        }

        final String INSERT_INTO_CLAUSE = "INSERT INTO ";

        // Replace INSERT INTO "table" with
        // INSERT INTO "schema"."table"
        // Case has to match!

        StringBuilder insertSb = insertSchema(
            buf, INSERT_INTO_CLAUSE, true, true);

        // Prepend schema to all known table names.
        // These will be in the FROM clause
        // Case has to match!

        for (String tableName : tableMetadataToLoad.keySet()) {
            insertSb = insertSchema(
                insertSb, quoteId(tableName), false, false);
        }

        LOGGER.debug(insertSb.toString());
        return insertSb;
    }

    private StringBuilder insertSchema(
        StringBuilder sb,
        String toFind,
        boolean mandatory,
        boolean insertBefore)
    {
        int pos = sb.indexOf(toFind);

        if (pos < 0) {
            if (mandatory) {
                throw new RuntimeException(
                    "insert.sql error: No insert clause in " + sb.toString());
            } else {
                return sb;
            }
        }

        StringBuilder insertSb = new StringBuilder();

        if (insertBefore) {
            insertSb.append(sb.substring(0, pos))
                .append(toFind)
                .append(quoteId(schema))
                .append(".")
                .append(sb.substring(pos + toFind.length()));
        } else {
            insertSb.append(sb.substring(0, pos))
                .append(quoteId(schema))
                .append(".")
                .append(toFind)
                .append(sb.substring(pos + toFind.length()));
        }

        return insertSb;
    }

    /**
     * Read the given table from the input RDBMS and output to destination
     * RDBMS or file
     *
     * @param name      Name of table
     * @param columns   Columns to be read/output
     * @param pauseMillis How many milliseconds to pause between rows
     * @param batchSize How often to write/commit to the database
     * @return          #rows inserted
     */
    private int loadTable(
        String name,
        Column[] columns,
        long pauseMillis,
        int batchSize)
        throws Exception
    {
        int rowsAdded = 0;
        StringBuilder buf = new StringBuilder();

        buf.append("select ");
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(quoteId(dialect, column.name));
        }

        buf.append(" from ")
            .append(quoteId(dialect, inputSchema, name));

        String ddl = buf.toString();
        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = inputConnection.createStatement();
            LOGGER.debug("Input table SQL: " + ddl);

            rs = statement.executeQuery(ddl);

            List<String> batch = new ArrayList<String>(batchSize);
            boolean displayedInsert = false;

            while (rs.next()) {
                // Get a batch of insert statements, then save a batch
                String insertStatement =
                    createInsertStatement(rs, name, columns);
                if (!displayedInsert && LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "Example Insert statement: " + insertStatement);
                    displayedInsert = true;
                }
                batch.add(insertStatement);
                if (batch.size() >= batchSize) {
                    final int rowCount = writeBatch(batch, pauseMillis);
                    rowsAdded += rowCount;
                    batch.clear();
                    if (pauseMillis > 0) {
                        final long t = System.currentTimeMillis();
                        if (t - lastUpdate > 10000) {
                            lastUpdate = t;
                            LOGGER.debug(
                                name + ": wrote row #" + rowsAdded + ".");
                        }
                        Thread.sleep(pauseMillis);
                    }
                }
            }

            if (batch.size() > 0) {
                rowsAdded += writeBatch(batch, pauseMillis);
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }

        return rowsAdded;
    }

    /**
     * Creates a SQL INSERT statement in the dialect of the output RDBMS.
     *
     * @param rs            ResultSet of input RDBMS
     * @param name          name of table
     * @param columns       column definitions for INSERT statement
     * @return String       the INSERT statement
     */
    private String createInsertStatement(
        ResultSet rs,
        String name,
        Column[] columns)
        throws Exception
    {
        StringBuilder buf = new StringBuilder();

        buf.append("INSERT INTO ")
            .append(quoteId(schema, name))
            .append(" (");
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(quoteId(column.name));
        }
        buf.append(") VALUES(");
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (i > 0) {
                buf.append(",");
            }
            buf.append(columnValue(rs, column));
        }
        buf.append(")");
        return buf.toString();
    }

    /**
     * If we are outputting to JDBC,
     *      Execute the given set of SQL statements
     *
     * Otherwise,
     *      output the statements to a file.
     *
     * @param sqls          SQL statements to execute
     * @param pauseMillis   How many milliseconds to pause between batches
     * @return              # SQL statements executed
     */
    private int writeBatch(
        List<String> sqls,
        long pauseMillis)
        throws IOException, SQLException
    {
        if (sqls.size() == 0) {
            // nothing to do
            return sqls.size();
        }

        if (dialect.getDatabaseProduct()
            == Dialect.DatabaseProduct.INFOBRIGHT)
        {
            for (String sql : sqls) {
                fileOutput.write(sql);
                fileOutput.write(nl);
            }
        } else if (outputDirectory != null) {
            for (String sql : sqls) {
                fileOutput.write(sql);
                fileOutput.write(";" + nl);
            }
        } else {
            final boolean useTxn;
            if (dialect.getDatabaseProduct()
                == Dialect.DatabaseProduct.NEOVIEW)
            {
                // setAutoCommit can not changed to true again, throws
                // "com.hp.t4jdbc.HPT4Exception: SetAutoCommit not possible",
                // since a transaction is active
                useTxn = false;
            } else if (pauseMillis > 0) {
                // No point trickling in data if we don't commit it as we write.
                useTxn = false;
                connection.setAutoCommit(true);
            } else {
                useTxn = connection.getMetaData().supportsTransactions();
            }

            if (useTxn) {
                connection.setAutoCommit(false);
            }

            switch (dialect.getDatabaseProduct()) {
            case LUCIDDB:
            case NEOVIEW:
                // LucidDB doesn't perform well with single-row inserts,
                // and its JDBC driver doesn't support batch writes,
                // so collapse the batch into one big multi-row insert.
                // Similarly Neoview.
                String VALUES_TOKEN = "VALUES";
                StringBuilder sb = new StringBuilder(sqls.get(0));
                for (int i = 1; i < sqls.size(); i++) {
                    sb.append(",\n");
                    int valuesPos = sqls.get(i).indexOf(VALUES_TOKEN);
                    if (valuesPos < 0) {
                        throw new RuntimeException(
                            "Malformed INSERT:  " + sqls.get(i));
                    }
                    valuesPos += VALUES_TOKEN.length();
                    sb.append(sqls.get(i).substring(valuesPos));
                }
                sqls.clear();
                sqls.add(sb.toString());
            }

            Statement stmt = connection.createStatement();
            if (sqls.size() == 1) {
                // Don't use batching if there's only one item. This allows
                // us to work around bugs in the JDBC driver by setting
                // outputJdbcBatchSize=1.
                stmt.execute(sqls.get(0));
            } else {
                for (String sql : sqls) {
                    stmt.addBatch(sql);
                }
                int[] updateCounts;

                try {
                    updateCounts = stmt.executeBatch();
                } catch (SQLException e) {
                    for (String sql : sqls) {
                        LOGGER.error("Error in SQL batch: " + sql);
                    }
                    throw e;
                }
                int updates = 0;
                for (int i = 0; i < updateCounts.length;
                    updates += updateCounts[i], i++)
                {
                    if (updateCounts[i] == 0) {
                        LOGGER.error("Error in SQL: " + sqls.get(i));
                    }
                }
                if (updates < sqls.size()) {
                    throw new RuntimeException(
                        "Failed to execute batch: " + sqls.size() + " versus "
                        + updates);
                }
            }
            stmt.close();
            if (useTxn) {
                connection.setAutoCommit(true);
            }
        }
        return sqls.size();
    }

    /**
     * Open the file of INSERT statements to load the data. Default
     * file name is ./demo/FoodMartCreateData.zip
     *
     * @return FileInputStream
     */
    private InputStream openInputStream() throws Exception {
        String defaultZipFileName = getClass().getResource("FoodMartCreateData.zip").getPath();
        final String defaultDataFileName = "FoodMartCreateData.sql";
        final File file =
            (inputFile != null)
                ? new File(inputFile)
                : new File(defaultZipFileName);
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
     * Create all indexes for the FoodMart database.
     *
     * @param baseTables Whether to create indexes on base tables
     * @param summaryTables Whether to create indexes on agg tables
     */
    private void createIndexes(
        boolean baseTables,
        boolean summaryTables,
        Condition<String> tableFilter)
        throws Exception
    {
        if (outputDirectory != null) {
            file = new File(outputDirectory, "createIndexes.sql");
            fileOutput = new FileWriter(file);
        }

        createIndex(
            true,
            "account",
            "i_account_id",
            new String[] {"account_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "account",
            "i_account_parent",
            new String[] {"account_parent"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "category",
            "i_category_id",
            new String[] {"category_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "category",
            "i_category_parent",
            new String[] {"category_parent"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "currency",
            "i_currency",
            new String[] {"currency_id", "date"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "customer",
            "i_cust_acct_num",
            new String[] {"account_num"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "customer",
            "i_customer_fname",
            new String[] {"fname"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "customer",
            "i_customer_lname",
            new String[] {"lname"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "customer",
            "i_cust_child_home",
            new String[] {"num_children_at_home"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "customer",
            "i_customer_id",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "customer",
            "i_cust_postal_code",
            new String[] {"postal_code"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "customer",
            "i_cust_region_id",
            new String[] {"customer_region_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "department",
            "i_department_id",
            new String[] {"department_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "employee",
            "i_employee_id",
            new String[] {"employee_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "employee",
            "i_empl_dept_id",
            new String[] {"department_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "employee",
            "i_empl_store_id",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "employee",
            "i_empl_super_id",
            new String[] {"supervisor_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "employee_closure",
            "i_empl_closure",
            new String[] {"supervisor_id", "employee_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "employee_closure",
            "i_empl_closure_emp",
            new String[] {"employee_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "expense_fact",
            "i_expense_store_id",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "expense_fact",
            "i_expense_acct_id",
            new String[] {"account_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "expense_fact",
            "i_expense_time_id",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "inventory_fact_1997",
            "i_inv_97_prod_id",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "inventory_fact_1997",
            "i_inv_97_store_id",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "inventory_fact_1997",
            "i_inv_97_time_id",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "inventory_fact_1997",
            "i_inv_97_wrhse_id",
            new String[] {"warehouse_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "inventory_fact_1998",
            "i_inv_98_prod_id",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "inventory_fact_1998",
            "i_inv_98_store_id",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "inventory_fact_1998",
            "i_inv_98_time_id",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "inventory_fact_1998",
            "i_inv_98_wrhse_id",
            new String[] {"warehouse_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "position",
            "i_position_id",
            new String[] {"position_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "product",
            "i_prod_brand_name",
            new String[] {"brand_name"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "product",
            "i_product_id",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "product",
            "i_prod_class_id",
            new String[] {"product_class_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "product",
            "i_product_name",
            new String[] {"product_name"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "product",
            "i_product_SKU",
            new String[] {"SKU"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "promotion",
            "i_promotion_id",
            new String[] {"promotion_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "promotion",
            "i_promo_dist_id",
            new String[] {"promotion_district_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "reserve_employee",
            "i_rsrv_empl_id",
            new String[] {"employee_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "reserve_employee",
            "i_rsrv_empl_dept",
            new String[] {"department_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "reserve_employee",
            "i_rsrv_empl_store",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "reserve_employee",
            "i_rsrv_empl_sup",
            new String[] {"supervisor_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "salary",
            "i_salary_pay_date",
            new String[] {"pay_date"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "salary",
            "i_salary_employee",
            new String[] {"employee_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1997",
            "i_sls_97_cust_id",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1997",
            "i_sls_97_prod_id",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1997",
            "i_sls_97_promo_id",
            new String[] {"promotion_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1997",
            "i_sls_97_store_id",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1997",
            "i_sls_97_time_id",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_dec_1998",
            "i_sls_dec98_cust",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_dec_1998",
            "i_sls_dec98_prod",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_dec_1998",
            "i_sls_dec98_promo",
            new String[] {"promotion_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_dec_1998",
            "i_sls_dec98_store",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_dec_1998",
            "i_sls_dec98_time",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1998",
            "i_sls_98_cust_id",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1998",
            "i_sls_1998_prod_id",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1998",
            "i_sls_1998_promo",
            new String[] {"promotion_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1998",
            "i_sls_1998_store",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "sales_fact_1998",
            "i_sls_1998_time_id",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "store",
            "i_store_id",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "store",
            "i_store_region_id",
            new String[] {"region_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "store_ragged",
            "i_store_raggd_id",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "store_ragged",
            "i_store_rggd_reg",
            new String[] {"region_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "time_by_day",
            "i_time_id",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            true,
            "time_by_day",
            "i_time_day",
            new String[] {"the_date"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "time_by_day",
            "i_time_year",
            new String[] {"the_year"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "time_by_day",
            "i_time_quarter",
            new String[] {"quarter"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "time_by_day",
            "i_time_month",
            new String[] {"month_of_year"},
            baseTables,
            summaryTables,
            tableFilter);

        createIndex(
            false,
            "agg_pl_01_sales_fact_1997",
            "i_sls97pl01cust",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_pl_01_sales_fact_1997",
            "i_sls97pl01prod",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_pl_01_sales_fact_1997",
            "i_sls97pl01time",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);

        createIndex(
            false,
            "agg_ll_01_sales_fact_1997",
            "i_sls97ll01cust",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_ll_01_sales_fact_1997",
            "i_sls97ll01prod",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_ll_01_sales_fact_1997",
            "i_sls97ll01time",
            new String[] {"time_id"},
            baseTables,
            summaryTables,
            tableFilter);

        createIndex(
            false,
            "agg_l_05_sales_fact_1997",
            "i_sls97l05cust",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_l_05_sales_fact_1997",
            "i_sls97l05prod",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_l_05_sales_fact_1997",
            "i_sls97l05promo",
            new String[] {"promotion_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_l_05_sales_fact_1997",
            "i_sls97l05store",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);

        createIndex(
            false,
            "agg_c_14_sales_fact_1997",
            "i_sls97c14cust",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_c_14_sales_fact_1997",
            "i_sls97c14prod",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_c_14_sales_fact_1997",
            "i_sls97c14promo",
            new String[] {"promotion_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_c_14_sales_fact_1997",
            "i_sls97c14store",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);

        createIndex(
            false,
            "agg_lc_100_sales_fact_1997",
            "i_sls97lc100cust",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_lc_100_sales_fact_1997",
            "i_sls97lc100prod",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);

        createIndex(
            false,
            "agg_c_special_sales_fact_1997",
            "i_sls97speccust",
            new String[] {"customer_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_c_special_sales_fact_1997",
            "i_sls97specprod",
            new String[] {"product_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_c_special_sales_fact_1997",
            "i_sls97specpromo",
            new String[] {"promotion_id"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_c_special_sales_fact_1997",
            "i_sls97specstore",
            new String[] {"store_id"},
            baseTables,
            summaryTables,
            tableFilter);

        createIndex(
            false,
            "agg_g_ms_pcat_sales_fact_1997",
            "i_sls97gmp_gender",
            new String[] {"gender"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_g_ms_pcat_sales_fact_1997",
            "i_sls97gmp_ms",
            new String[] {"marital_status"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_g_ms_pcat_sales_fact_1997",
            "i_sls97gmp_pfam",
            new String[] {"product_family"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_g_ms_pcat_sales_fact_1997",
            "i_sls97gmp_pdept",
            new String[] {"product_department"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_g_ms_pcat_sales_fact_1997",
            "i_sls97gmp_pcat",
            new String[] {"product_category"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_g_ms_pcat_sales_fact_1997",
            "i_sls97gmp_tmonth",
            new String[] {"month_of_year"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_g_ms_pcat_sales_fact_1997",
            "i_sls97gmp_tquarter",
            new String[] {"quarter"},
            baseTables,
            summaryTables,
            tableFilter);
        createIndex(
            false,
            "agg_g_ms_pcat_sales_fact_1997",
            "i_sls97gmp_tyear",
            new String[] {"the_year"},
            baseTables,
            summaryTables,
            tableFilter);

        if (outputDirectory != null) {
            fileOutput.close();
        }
    }

    /**
     * Creates an index.
     *
     * <p>If we are outputting to JDBC, executes the CREATE INDEX statement;
     * otherwise, outputs the statement to a file.
     */
    private void createIndex(
        boolean isUnique,
        String tableName,
        String indexName,
        String[] columnNames,
        boolean baseTables,
        boolean aggregateTables,
        Condition<String> tableFilter)
    {
        if (!tableFilter.test(tableName)) {
            return;
        }
        if (!baseTables && !aggregateTables) {
            // This is just a dry run to record the unique indexes
            // so that we can implement them as standard
            // UNIQUE constraints if desired.
            if (!isUnique || !generateUniqueConstraints) {
                return;
            }
            List<UniqueConstraint> constraintList =
                tableConstraints.get(tableName);
            if (constraintList == null) {
                constraintList = new ArrayList<UniqueConstraint>();
                tableConstraints.put(tableName, constraintList);
            }
            constraintList.add(
                new UniqueConstraint(
                    indexName,
                    columnNames));
            return;
        } else {
            if (isUnique && generateUniqueConstraints) {
                // We'll implement this via a UNIQUE constraint instead
                return;
            }
        }

        try {
            // Is it an aggregate table or a base table?
            boolean isBase =
                    !aggregateTableMetadataToLoad.containsKey(tableName);

            // Only do aggregate tables
            if (populationQueries && !isBase) {
                return;
            }

            if (isBase && !baseTables) {
                // This is a base table, but we're not to index base tables.
                return;
            }

            if (!isBase && !aggregateTables) {
                // This is an aggregate table, but we're not to index agg
                // tables.
                return;
            }

            StringBuilder buf = new StringBuilder();

            // If we're [re]creating tables, no need to drop indexes.
            if (jdbcOutput && !tables) {
                try {
                    buf.append("DROP INDEX ")
                        .append(quoteId(schema, indexName));
                    switch (dialect.getDatabaseProduct()) {
                    case MARIADB:
                    case MYSQL:
                    case INFOBRIGHT:
                    case TERADATA:
                        buf.append(" ON ")
                            .append(quoteId(schema, tableName));
                        break;
                    }
                    final String deleteDDL = buf.toString();
                    executeDDL(deleteDDL);
                } catch (Exception e1) {
                    LOGGER.info(
                        "Index Drop failed for " + tableName + ", " + indexName
                        + " : but continue");
                }
            }

            buf.setLength(0);
            buf.append(isUnique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ")
                .append(quoteId(indexName));
            if (dialect.getDatabaseProduct()
                != Dialect.DatabaseProduct.TERADATA)
            {
                buf.append(" ON ").append(quoteId(schema, tableName));
            }
            buf.append(" (");
            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(quoteId(columnName));
            }
            buf.append(")");
            if (dialect.getDatabaseProduct()
                == Dialect.DatabaseProduct.TERADATA)
            {
                buf.append(" ON ").append(quoteId(schema, tableName));
            }
            final String createDDL = buf.toString();
            executeDDL(createDDL);
        } catch (Exception e) {
            throw MondrianResource.instance().CreateIndexFailed.ex(
                indexName, tableName, e);
        }
    }

    /**
     * Defines all tables for the FoodMart database.<p/>
     *
     * <p>Also initializes {@link #tableMetadataToLoad} and
     * {@link #aggregateTableMetadataToLoad}.
     *
     * @param tableFilter Condition whether to load a particular table
     */
    private void createTables(Condition<String> tableFilter) throws Exception {
        if (outputDirectory != null) {
            file = new File(outputDirectory, "createTables.sql");
            fileOutput = new FileWriter(file);
        }

        createTable(
            "sales_fact_1997", tableFilter,
            new Column("product_id", Type.Integer, false),
            new Column("time_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("promotion_id", Type.Integer, false),
            new Column("store_id", Type.Integer, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false));
        createTable(
            "sales_fact_1998", tableFilter,
            new Column("product_id", Type.Integer, false),
            new Column("time_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("promotion_id", Type.Integer, false),
            new Column("store_id", Type.Integer, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false));
        createTable(
            "sales_fact_dec_1998", tableFilter,
            new Column("product_id", Type.Integer, false),
            new Column("time_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("promotion_id", Type.Integer, false),
            new Column("store_id", Type.Integer, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false));
        createTable(
            "inventory_fact_1997", tableFilter,
            new Column("product_id", Type.Integer, false),
            new Column("time_id", Type.Integer, true),
            new Column("warehouse_id", Type.Integer, true),
            new Column("store_id", Type.Integer, true),
            new Column("units_ordered", Type.Integer, true),
            new Column("units_shipped", Type.Integer, true),
            new Column("warehouse_sales", Type.Currency, true),
            new Column("warehouse_cost", Type.Currency, true),
            new Column("supply_time", Type.Smallint, true),
            new Column("store_invoice", Type.Currency, true));
        createTable(
            "inventory_fact_1998", tableFilter,
            new Column("product_id", Type.Integer, false),
            new Column("time_id", Type.Integer, true),
            new Column("warehouse_id", Type.Integer, true),
            new Column("store_id", Type.Integer, true),
            new Column("units_ordered", Type.Integer, true),
            new Column("units_shipped", Type.Integer, true),
            new Column("warehouse_sales", Type.Currency, true),
            new Column("warehouse_cost", Type.Currency, true),
            new Column("supply_time", Type.Smallint, true),
            new Column("store_invoice", Type.Currency, true));

        //  Aggregate tables

        createTable(
            "agg_pl_01_sales_fact_1997", tableFilter, false,
            true,
            new Column("product_id", Type.Integer, false),
            new Column("time_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("store_sales_sum", Type.Currency, false),
            new Column("store_cost_sum", Type.Currency, false),
            new Column("unit_sales_sum", Type.Currency, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_ll_01_sales_fact_1997", tableFilter, false,
            true,
            new Column("product_id", Type.Integer, false),
            new Column("time_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_l_03_sales_fact_1997", tableFilter, false,
            true,
            new Column("time_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_l_04_sales_fact_1997", tableFilter, false,
            true,
            new Column("time_id", Type.Integer, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("customer_count", Type.Integer, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_l_05_sales_fact_1997", tableFilter, false,
            true,
            new Column("product_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("promotion_id", Type.Integer, false),
            new Column("store_id", Type.Integer, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_c_10_sales_fact_1997", tableFilter, false,
            true,
            new Column("month_of_year", Type.Smallint, false),
            new Column("quarter", Type.Varchar30, false),
            new Column("the_year", Type.Smallint, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("customer_count", Type.Integer, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_c_14_sales_fact_1997", tableFilter, false,
            true,
            new Column("product_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("store_id", Type.Integer, false),
            new Column("promotion_id", Type.Integer, false),
            new Column("month_of_year", Type.Smallint, false),
            new Column("quarter", Type.Varchar30, false),
            new Column("the_year", Type.Smallint, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_lc_100_sales_fact_1997", tableFilter, false,
            true,
            new Column("product_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("quarter", Type.Varchar30, false),
            new Column("the_year", Type.Smallint, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_c_special_sales_fact_1997", tableFilter, false,
            true,
            new Column("product_id", Type.Integer, false),
            new Column("promotion_id", Type.Integer, false),
            new Column("customer_id", Type.Integer, false),
            new Column("store_id", Type.Integer, false),
            new Column("time_month", Type.Smallint, false),
            new Column("time_quarter", Type.Varchar30, false),
            new Column("time_year", Type.Smallint, false),
            new Column("store_sales_sum", Type.Currency, false),
            new Column("store_cost_sum", Type.Currency, false),
            new Column("unit_sales_sum", Type.Currency, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_g_ms_pcat_sales_fact_1997", tableFilter, false,
            true,
            new Column("gender", Type.Varchar30, false),
            new Column("marital_status", Type.Varchar30, false),
            new Column("product_family", Type.Varchar30, true),
            new Column("product_department", Type.Varchar30, true),
            new Column("product_category", Type.Varchar30, true),
            new Column("month_of_year", Type.Smallint, false),
            new Column("quarter", Type.Varchar30, false),
            new Column("the_year", Type.Smallint, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("customer_count", Type.Integer, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "agg_lc_06_sales_fact_1997", tableFilter, false,
            true,
            new Column("time_id", Type.Integer, false),
            new Column("city", Type.Varchar30, false),
            new Column("state_province", Type.Varchar30, false),
            new Column("country", Type.Varchar30, false),
            new Column("store_sales", Type.Currency, false),
            new Column("store_cost", Type.Currency, false),
            new Column("unit_sales", Type.Currency, false),
            new Column("fact_count", Type.Integer, false));
        createTable(
            "currency", tableFilter,
            new Column("currency_id", Type.Integer, false),
            new Column("date", Type.Date, false),
            new Column("currency", Type.Varchar30, false),
            new Column("conversion_ratio", Type.Currency, false));
        createTable(
            "account", tableFilter,
            new Column("account_id", Type.Integer, false),
            new Column("account_parent", Type.Integer, true),
            new Column("account_description", Type.Varchar30, true),
            new Column("account_type", Type.Varchar30, false),
            new Column("account_rollup", Type.Varchar30, false),
            new Column("Custom_Members", Type.Varchar255, true));
        createTable(
            "category", tableFilter,
            new Column("category_id", Type.Varchar30, false),
            new Column("category_parent", Type.Varchar30, true),
            new Column("category_description", Type.Varchar30, false),
            new Column("category_rollup", Type.Varchar30, true));
        createTable(
            "customer", tableFilter,
            new Column("customer_id", Type.Integer, false),
            new Column("account_num", Type.Bigint, false),
            new Column("lname", Type.Varchar30, false),
            new Column("fname", Type.Varchar30, false),
            new Column("mi", Type.Varchar30, true),
            new Column("address1", Type.Varchar30, true),
            new Column("address2", Type.Varchar30, true),
            new Column("address3", Type.Varchar30, true),
            new Column("address4", Type.Varchar30, true),
            new Column("city", Type.Varchar30, true),
            new Column("state_province", Type.Varchar30, true),
            new Column("postal_code", Type.Varchar30, false),
            new Column("country", Type.Varchar30, false),
            new Column("customer_region_id", Type.Integer, false),
            new Column("phone1", Type.Varchar30, false),
            new Column("phone2", Type.Varchar30, false),
            new Column("birthdate", Type.Date, false),
            new Column("marital_status", Type.Varchar30, false),
            new Column("yearly_income", Type.Varchar30, false),
            new Column("gender", Type.Varchar30, false),
            new Column("total_children", Type.Smallint, false),
            new Column("num_children_at_home", Type.Smallint, false),
            new Column("education", Type.Varchar30, false),
            new Column("date_accnt_opened", Type.Date, false),
            new Column("member_card", Type.Varchar30, true),
            new Column("occupation", Type.Varchar30, true),
            new Column("houseowner", Type.Varchar30, true),
            new Column("num_cars_owned", Type.Integer, true),
            new Column("fullname", Type.Varchar60, false));
        createTable(
            "days", tableFilter,
            new Column("day", Type.Integer, false),
            new Column("week_day", Type.Varchar30, false));
        createTable(
            "department", tableFilter,
            new Column("department_id", Type.Integer, false),
            new Column("department_description", Type.Varchar30, false));
        createTable(
            "employee", tableFilter,
            new Column("employee_id", Type.Integer, false),
            new Column("full_name", Type.Varchar30, false),
            new Column("first_name", Type.Varchar30, false),
            new Column("last_name", Type.Varchar30, false),
            new Column("position_id", Type.Integer, true),
            new Column("position_title", Type.Varchar30, true),
            new Column("store_id", Type.Integer, false),
            new Column("department_id", Type.Integer, false),
            new Column("birth_date", Type.Date, false),
            new Column("hire_date", Type.Timestamp, true),
            new Column("end_date", Type.Timestamp, true),
            new Column("salary", Type.Currency, false),
            new Column("supervisor_id", Type.Integer, true),
            new Column("education_level", Type.Varchar30, false),
            new Column("marital_status", Type.Varchar30, false),
            new Column("gender", Type.Varchar30, false),
            new Column("management_role", Type.Varchar30, true));
        createTable(
            "employee_closure", tableFilter,
            new Column("employee_id", Type.Integer, false),
            new Column("supervisor_id", Type.Integer, false),
            new Column("distance", Type.Integer, true));
        createTable(
            "expense_fact", tableFilter,
            new Column("store_id", Type.Integer, false),
            new Column("account_id", Type.Integer, false),
            new Column("exp_date", Type.Timestamp, false),
            new Column("time_id", Type.Integer, false),
            new Column("category_id", Type.Varchar30, false),
            new Column("currency_id", Type.Integer, false),
            new Column("amount", Type.Currency, false));
        createTable(
            "position", tableFilter,
            new Column("position_id", Type.Integer, false),
            new Column("position_title", Type.Varchar30, false),
            new Column("pay_type", Type.Varchar30, false),
            new Column("min_scale", Type.Currency, false),
            new Column("max_scale", Type.Currency, false),
            new Column("management_role", Type.Varchar30, false));
        createTable(
            "product", tableFilter,
            new Column("product_class_id", Type.Integer, false),
            new Column("product_id", Type.Integer, false),
            new Column("brand_name", Type.Varchar60, true),
            new Column("product_name", Type.Varchar60, false),
            new Column("SKU", Type.Bigint, false),
            new Column("SRP", Type.Currency, true),
            new Column("gross_weight", Type.Real, true),
            new Column("net_weight", Type.Real, true),
            new Column("recyclable_package", Type.Boolean, true),
            new Column("low_fat", Type.Boolean, true),
            new Column("units_per_case", Type.Smallint, true),
            new Column("cases_per_pallet", Type.Smallint, true),
            new Column("shelf_width", Type.Real, true),
            new Column("shelf_height", Type.Real, true),
            new Column("shelf_depth", Type.Real, true));
        createTable(
            "product_class", tableFilter,
            new Column("product_class_id", Type.Integer, false),
            new Column("product_subcategory", Type.Varchar30, true),
            new Column("product_category", Type.Varchar30, true),
            new Column("product_department", Type.Varchar30, true),
            new Column("product_family", Type.Varchar30, true));
        createTable(
            "promotion", tableFilter,
            new Column("promotion_id", Type.Integer, false),
            new Column("promotion_district_id", Type.Integer, true),
            new Column("promotion_name", Type.Varchar30, true),
            new Column("media_type", Type.Varchar30, true),
            new Column("cost", Type.Currency, true),
            new Column("start_date", Type.Timestamp, true),
            new Column("end_date", Type.Timestamp, true));
        createTable(
            "region", tableFilter,
            new Column("region_id", Type.Integer, false),
            new Column("sales_city", Type.Varchar30, true),
            new Column("sales_state_province", Type.Varchar30, true),
            new Column("sales_district", Type.Varchar30, true),
            new Column("sales_region", Type.Varchar30, true),
            new Column("sales_country", Type.Varchar30, true),
            new Column("sales_district_id", Type.Integer, true));
        createTable(
            "reserve_employee", tableFilter,
            new Column("employee_id", Type.Integer, false),
            new Column("full_name", Type.Varchar30, false),
            new Column("first_name", Type.Varchar30, false),
            new Column("last_name", Type.Varchar30, false),
            new Column("position_id", Type.Integer, true),
            new Column("position_title", Type.Varchar30, true),
            new Column("store_id", Type.Integer, false),
            new Column("department_id", Type.Integer, false),
            new Column("birth_date", Type.Timestamp, false),
            new Column("hire_date", Type.Timestamp, true),
            new Column("end_date", Type.Timestamp, true),
            new Column("salary", Type.Currency, false),
            new Column("supervisor_id", Type.Integer, true),
            new Column("education_level", Type.Varchar30, false),
            new Column("marital_status", Type.Varchar30, false),
            new Column("gender", Type.Varchar30, false));
        createTable(
            "salary", tableFilter,
            new Column("pay_date", Type.Timestamp, false),
            new Column("employee_id", Type.Integer, false),
            new Column("department_id", Type.Integer, false),
            new Column("currency_id", Type.Integer, false),
            new Column("salary_paid", Type.Currency, false),
            new Column("overtime_paid", Type.Currency, false),
            new Column("vacation_accrued", Type.Real, false),
            new Column("vacation_used", Type.Real, false));
        createTable(
            "store", tableFilter,
            new Column("store_id", Type.Integer, false),
            new Column("store_type", Type.Varchar30, true),
            new Column("region_id", Type.Integer, true),
            new Column("store_name", Type.Varchar30, true),
            new Column("store_number", Type.Integer, true),
            new Column("store_street_address", Type.Varchar30, true),
            new Column("store_city", Type.Varchar30, true),
            new Column("store_state", Type.Varchar30, true),
            new Column("store_postal_code", Type.Varchar30, true),
            new Column("store_country", Type.Varchar30, true),
            new Column("store_manager", Type.Varchar30, true),
            new Column("store_phone", Type.Varchar30, true),
            new Column("store_fax", Type.Varchar30, true),
            new Column("first_opened_date", Type.Timestamp, true),
            new Column("last_remodel_date", Type.Timestamp, true),
            new Column("store_sqft", Type.Integer, true),
            new Column("grocery_sqft", Type.Integer, true),
            new Column("frozen_sqft", Type.Integer, true),
            new Column("meat_sqft", Type.Integer, true),
            new Column("coffee_bar", Type.Boolean, true),
            new Column("video_store", Type.Boolean, true),
            new Column("salad_bar", Type.Boolean, true),
            new Column("prepared_food", Type.Boolean, true),
            new Column("florist", Type.Boolean, true));
        createTable(
            "store_ragged", tableFilter,
            new Column("store_id", Type.Integer, false),
            new Column("store_type", Type.Varchar30, true),
            new Column("region_id", Type.Integer, true),
            new Column("store_name", Type.Varchar30, true),
            new Column("store_number", Type.Integer, true),
            new Column("store_street_address", Type.Varchar30, true),
            new Column("store_city", Type.Varchar30, true),
            new Column("store_state", Type.Varchar30, true),
            new Column("store_postal_code", Type.Varchar30, true),
            new Column("store_country", Type.Varchar30, true),
            new Column("store_manager", Type.Varchar30, true),
            new Column("store_phone", Type.Varchar30, true),
            new Column("store_fax", Type.Varchar30, true),
            new Column("first_opened_date", Type.Timestamp, true),
            new Column("last_remodel_date", Type.Timestamp, true),
            new Column("store_sqft", Type.Integer, true),
            new Column("grocery_sqft", Type.Integer, true),
            new Column("frozen_sqft", Type.Integer, true),
            new Column("meat_sqft", Type.Integer, true),
            new Column("coffee_bar", Type.Boolean, true),
            new Column("video_store", Type.Boolean, true),
            new Column("salad_bar", Type.Boolean, true),
            new Column("prepared_food", Type.Boolean, true),
            new Column("florist", Type.Boolean, true));
        createTable(
            "time_by_day", tableFilter,
            new Column("time_id", Type.Integer, false),
            new Column("the_date", Type.Timestamp, true),
            new Column("the_day", Type.Varchar30, true),
            new Column("the_month", Type.Varchar30, true),
            new Column("the_year", Type.Smallint, true),
            new Column("day_of_month", Type.Smallint, true),
            new Column("week_of_year", Type.Integer, true),
            new Column("month_of_year", Type.Smallint, true),
            new Column("quarter", Type.Varchar30, true),
            new Column("fiscal_period", Type.Varchar30, true));
        createTable(
            "warehouse", tableFilter,
            new Column("warehouse_id", Type.Integer, false),
            new Column("warehouse_class_id", Type.Integer, true),
            new Column("stores_id", Type.Integer, true),
            new Column("warehouse_name", Type.Varchar60, true),
            new Column("wa_address1", Type.Varchar30, true),
            new Column("wa_address2", Type.Varchar30, true),
            new Column("wa_address3", Type.Varchar30, true),
            new Column("wa_address4", Type.Varchar30, true),
            new Column("warehouse_city", Type.Varchar30, true),
            new Column("warehouse_state_province", Type.Varchar30, true),
            new Column("warehouse_postal_code", Type.Varchar30, true),
            new Column("warehouse_country", Type.Varchar30, true),
            new Column("warehouse_owner_name", Type.Varchar30, true),
            new Column("warehouse_phone", Type.Varchar30, true),
            new Column("warehouse_fax", Type.Varchar30, true));
        createTable(
            "warehouse_class", tableFilter,
            new Column("warehouse_class_id", Type.Integer, false),
            new Column("description", Type.Varchar30, true));
        if (outputDirectory != null) {
            fileOutput.close();
        }
    }

    /**
     * If we are outputting to JDBC, and not creating tables, delete all rows.
     *
     * <p>Otherwise:
     *
     * <p>Generate the SQL CREATE TABLE statement.
     *
     * <p>If we are outputting to JDBC,
     *      Execute a DROP TABLE statement
     *      Execute the CREATE TABLE statement
     *
     * <p>Otherwise,
     *      output the statement to a file.
     */
    private void createTable(
        String name,
        Condition<String> tableFilter,
        Column... columns)
    {
        createTable(name, tableFilter, true, false, columns);
    }

    /**
     * Creates a table definition.
     *
     * @param name    Table name
     * @param tableFilter Table filter
     * @param loadData Whether to load data
     * @param aggregate Whether it is an aggregate table
     * @param columns Column definitions
     */
    private void createTable(
        String name,
        Condition<String> tableFilter,
        boolean loadData,
        boolean aggregate,
        Column... columns)
    {
        try {
            // Initialize columns
            for (Column column1 : columns) {
                column1.init(dialect);
            }

            if (!tableFilter.test(name)) {
                return;
            }

            // Store this metadata if we are going to load the table
            // from JDBC or a file

            if (loadData) {
                tableMetadataToLoad.put(name, columns);
            }

            if (aggregate && aggregates) {
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
                        executeDDL("DELETE FROM " + quoteId(schema, name));
                    } catch (SQLException e) {
                        throw MondrianResource.instance().CreateTableFailed.ex(
                            name, e);
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
                executeDDL("DROP TABLE " + quoteId(schema, name));
            } catch (Exception e) {
                LOGGER.debug("Drop of " + name + " failed. Ignored");
            }

            // Define the table.
            StringBuilder buf = new StringBuilder();
            buf.append("CREATE TABLE ")
                .append(quoteId(schema, name))
                .append("(");

            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                if (i > 0) {
                    buf.append(",");
                }
                buf.append(nl);
                buf.append("    ").append(quoteId(column.name)).append(" ")
                    .append(column.typeName);
                if (!column.constraint.equals("")) {
                    buf.append(" ").append(column.constraint);
                }
            }

            List<UniqueConstraint> uniqueConstraints =
                tableConstraints.get(name);

            if (uniqueConstraints != null) {
                for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
                    buf.append(",");
                    buf.append(nl);
                    buf.append("    ");
                    buf.append("CONSTRAINT ");
                    buf.append(quoteId(uniqueConstraint.name));
                    buf.append(" UNIQUE(");
                    String [] columnNames = uniqueConstraint.columnNames;
                    for (int i = 0; i < columnNames.length; i++) {
                        if (i > 0) {
                            buf.append(",");
                        }
                        buf.append(quoteId(columnNames[i]));
                    }
                    buf.append(")");
                }
            }

            buf.append(")");
            switch (dialect.getDatabaseProduct()) {
            case NEOVIEW:
                // no unique keys defined
                buf.append(" NO PARTITION");
            }

            final String ddl = buf.toString();
            executeDDL(ddl);
        } catch (Exception e) {
            throw MondrianResource.instance().CreateTableFailed.ex(name, e);
        }
    }

    private void analyzeTables() throws SQLException {
        switch (dialect.getDatabaseProduct()) {
        case LUCIDDB:
            Statement statement = null;
            try {
                LOGGER.info("Analyzing schema...");
                statement = connection.createStatement();
                statement.execute(
                    "call "
                    + "applib.estimate_statistics_for_schema(current_schema)");
                LOGGER.info("Analyze complete.");
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
            break;
        default:
            LOGGER.warn("Analyze is not supported for current database.");
            break;
        }
    }

    /**
     * Executes a DDL statement.
     *
     * @param ddl DDL statement
     * @throws Exception on error
     */
    private void executeDDL(String ddl) throws Exception {
        LOGGER.info(ddl);

        if (jdbcOutput) {
            Statement statement = null;
            try {
                statement = connection.createStatement();
                statement.execute(ddl);
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        } else {
            fileOutput.write(ddl);
            fileOutput.write(";" + nl);
        }
    }

    /**
     * Quote the given SQL identifier suitable for the output DBMS.
     */
    private String quoteId(String name) {
        return quoteId(dialect, name);
    }

    /**
     * Quote the given SQL identifier suitable for the given DBMS type.
     */
    private String quoteId(Dialect dialect, String name) {
        return dialect.quoteIdentifier(name);
    }

    /**
     * Quote the given SQL identifier suitable for the output DBMS,
     * with schema.
     */
    private String quoteId(String schemaName, String name) {
        return quoteId(dialect, schemaName, name);
    }

    /**
     * Quote the given SQL identifier suitable for the given DBMS type,
     * with schema.
     */
    private String quoteId(Dialect dialect, String schemaName, String name) {
        return dialect.quoteIdentifier(schemaName, name);
    }

    /**
     * String representation of the column in the result set, suitable for
     * inclusion in a SQL insert statement.<p/>
     *
     * The column in the result set is transformed according to the type in
     * the column parameter.<p/>
     *
     * Different DBMSs (and drivers) return different Java types for a given
     * column; {@link ClassCastException}s may occur.
     *
     * @param rs        ResultSet row to process
     * @param column    Column to process
     * @return          String representation of column value
     */
    private String columnValue(ResultSet rs, Column column) throws Exception {
        Object obj = rs.getObject(column.name);
        String columnType = column.typeName;

        if (obj == null) {
            return "NULL";
        }

        /*
         * Output for an INTEGER column, handling Doubles and Integers
         * in the result set
         */
        if (columnType.startsWith(Type.Integer.name)) {
            if (obj.getClass() == Double.class) {
                try {
                    Double result = (Double) obj;
                    return integerFormatter.format(result.doubleValue());
                } catch (ClassCastException cce) {
                    LOGGER.error(
                        "CCE: "  + column.name + " to Long from: "
                        + obj.getClass().getName() + " - " + obj.toString());
                    throw cce;
                }
            } else {
                try {
                    int result = ((Number) obj).intValue();
                    return Integer.toString(result);
                } catch (ClassCastException cce) {
                    LOGGER.error(
                        "CCE: "  + column.name + " to Integer from: "
                        + obj.getClass().getName() + " - " + obj.toString());
                    throw cce;
                }
            }

        /*
         * Output for an SMALLINT column, handling Integers
         * in the result set
         */
        } else if (columnType.startsWith(Type.Smallint.name)) {
            if (obj instanceof Boolean) {
                return (Boolean) obj ? "1" : "0";
            } else {
                try {
                    Integer result = (Integer) obj;
                    return result.toString();
                } catch (ClassCastException cce) {
                    LOGGER.error(
                        "CCE: "  + column.name + " to Integer from: "
                        + obj.getClass().getName() + " - " + obj.toString());
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
                    LOGGER.error(
                        "CCE: "  + column.name + " to Double from: "
                        + obj.getClass().getName() + " - " + obj.toString());
                    throw cce;
                }
            } else {
                try {
                    Long result = (Long) obj;
                    return result.toString();
                } catch (ClassCastException cce) {
                    LOGGER.error(
                        "CCE: "  + column.name + " to Long from: "
                        + obj.getClass().getName() + " - " + obj.toString());
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
        } else {
            if (columnType.startsWith("TIMESTAMP")) {
                Timestamp ts = (Timestamp) obj;

                // REVIEW jvs 26-Nov-2006:  Is it safe to replace
                // these with dialect.quoteTimestampLiteral, etc?

                switch (dialect.getDatabaseProduct()) {
                case ORACLE:
                case LUCIDDB:
                case NEOVIEW:
                    return "TIMESTAMP '" + ts + "'";
                default:
                    return "'" + ts + "'";
                }
                //return "'" + ts + "'" ;

            /*
             * Output for a DATE
             */
            } else if (columnType.startsWith("DATE")) {
                Date dt = (Date) obj;
                switch (dialect.getDatabaseProduct()) {
                case ORACLE:
                case LUCIDDB:
                case NEOVIEW:
                    return "DATE '" + dateFormatter.format(dt) + "'";
                default:
                    return "'" + dateFormatter.format(dt) + "'";
                }

            /*
             * Output for a FLOAT
             */
            } else if (columnType.startsWith(Type.Real.name)) {
                Float result = (Float) obj;
                return result.toString();

            /*
             * Output for a DECIMAL(length, places)
             */
            } else if (columnType.startsWith("DECIMAL")) {
                final Matcher matcher =
                    decimalDataTypeRegex.matcher(columnType);
                if (!matcher.matches()) {
                    throw new Exception(
                        "Bad DECIMAL column type for " + columnType);
                }
                DecimalFormat formatter =
                    new DecimalFormat(
                        decimalFormat(matcher.group(1), matcher.group(2)));
                if (obj.getClass() == Double.class) {
                    try {
                        Double result = (Double) obj;
                        return formatter.format(result.doubleValue());
                    } catch (ClassCastException cce) {
                        LOGGER.error(
                            "CCE: "  + column.name + " to Double from: "
                            + obj.getClass().getName() + " - "
                            + obj.toString());
                        throw cce;
                    }
                } else {
                    // should be (obj.getClass() == BigDecimal.class)
                    try {
                        BigDecimal result = (BigDecimal) obj;
                        return formatter.format(result);
                    } catch (ClassCastException cce) {
                        LOGGER.error(
                            "CCE: "  + column.name + " to BigDecimal from: "
                            + obj.getClass().getName() + " - "
                            + obj.toString());
                        throw cce;
                    }
                }

            /*
             * Output for a BOOLEAN (Postgres) or BIT (other DBMSs)
             */
            } else if (columnType.startsWith("BOOLEAN")
                       || columnType.startsWith("BIT"))
            {
                Boolean result = (Boolean) obj;
                return result.toString();
            /*
             * Output for a BOOLEAN - TINYINT(1) (MySQL)
             */
            } else if (columnType.startsWith("TINYINT(1)")) {
                return (Boolean) obj ? "1" : "0";
            }
        }
        throw new Exception(
            "Unknown column type: " + columnType
            + " for column: " + column.name);
    }

    private String columnValue(
        String columnValue,
        Column column)
        throws Exception
    {
        String columnType = column.typeName;

        if (columnValue == null) {
            switch (dialect.getDatabaseProduct()) {
            // Sybase only accepts nulls as 0x00.
            case SYBASE:
                return "0x00";
            default:
                return "NULL";
            }
        }

        /*
         * Output for a TIMESTAMP
         */
        final Dialect.DatabaseProduct product = dialect.getDatabaseProduct();
        if (columnType.startsWith("TIMESTAMP")) {
            switch (product) {
            case ORACLE:
            case LUCIDDB:
            case NEOVIEW:
                return "TIMESTAMP " + columnValue;
            }

        /*
         * Output for a DATE
         */
        } else if (columnType.startsWith("DATE")) {
            switch (product) {
            case ORACLE:
            case LUCIDDB:
            case NEOVIEW:
                return "DATE " + columnValue;
            }

        /*
         * Output for a BOOLEAN (Postgres) or BIT (other DBMSs)
         */
        } else if (column.type == Type.Boolean) {
            String trimmedValue = columnValue.trim();
            switch (product) {
            case MYSQL:
            case INFOBRIGHT:
            case ORACLE:
            case DB2:
            case DB2_AS400:
            case DB2_OLD_AS400:
            case FIREBIRD:
            case MSSQL:
            case SYBASE:
            case DERBY:
            case TERADATA:
            case INGRES:
            case NEOVIEW:
            case VECTORWISE:
            case VERTICA:
            case INFORMIX:
                if (trimmedValue.equals("true")) {
                    return "1";
                } else if (trimmedValue.equals("false")) {
                    return "0";
                }
                break;
            default:
                if (trimmedValue.equals("1")) {
                    return "true";
                } else if (trimmedValue.equals("0")) {
                    return "false";
                }
                break;
            }
        }
        return columnValue;
    }

    /**
     * Generate an appropriate string to use in an SQL insert statement for
     * a VARCHAR colummn, taking into account NULL strings and strings with
     * embedded quotes.
     *
     * @param original  String to transform
     * @return NULL if null string, otherwise massaged string with doubled
     *         quotes for SQL
     */
    private String embedQuotes(String original) {
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
     * Generates an appropriate number format string for doubles etc
     * to be used to include a number in an SQL insert statement.
     *
     * <p>For example, {@code decimalFormat("6", "2")} returns "###0.00".
     *
     * @param lengthStr  String representing integer: number of digits to format
     * @param placesStr  String representing integer: number of decimal places
     * @return numeric format string
     */
    private static String decimalFormat(String lengthStr, String placesStr) {
        int length = Integer.parseInt(lengthStr);
        int places = Integer.parseInt(placesStr);
        return decimalFormat(length, places);
    }

    /**
     * Generates an appropriate number format string for doubles etc.
     * to be used to include a number in an SQL insert statement.
     *
     * <p>For example, {@code decimalFormat(6, 2)} returns "###0.00".
     *
     * @param length  int: number of digits to format
     * @param places  int: number of decimal places
     * @return numeric format string
     */
    private static String decimalFormat(int length, int places) {
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

    private static class Column {
        private final String name;
        private final Type type;
        private String typeName;
        private final String constraint;

        public Column(String name, Type type, boolean nullsAllowed) {
            this.name = name;
            this.type = type;
            this.constraint = nullsAllowed ? "" : "NOT NULL";
        }

        public void init(Dialect dialect) {
            this.typeName = type.toPhysical(dialect);
        }
    }

    private static class UniqueConstraint {
        final String name;
        final String [] columnNames;

        public UniqueConstraint(String name, String [] columnNames)
        {
            this.name = name;
            this.columnNames = columnNames;
        }
    }

    /**
     * Represents a logical type, such as "BOOLEAN".<p/>
     *
     * Specific databases will represent this with their own particular physical
     * type, for example "TINYINT(1)", "BOOLEAN" or "BIT";
     * see {@link #toPhysical(mondrian.spi.Dialect)}.
     */
    private static class Type {
        /**
         * The name of this type. Immutable, and independent of the RDBMS.
         */
        private final String name;

        private static final Type Integer = new Type("INTEGER");
        private static final Type Currency = new Type("DECIMAL(10,4)");
        private static final Type Smallint = new Type("SMALLINT");
        private static final Type Varchar30 = new Type("VARCHAR(30)");
        private static final Type Varchar255 = new Type("VARCHAR(255)");
        private static final Type Varchar60 = new Type("VARCHAR(60)");
        private static final Type Real = new Type("REAL");
        private static final Type Boolean = new Type("BOOLEAN");
        private static final Type Bigint = new Type("BIGINT");
        private static final Type Date = new Type("DATE");
        private static final Type Timestamp = new Type("TIMESTAMP");

        private Type(String name) {
            this.name = name;
        }

        /**
         * Returns the physical type which a given RDBMS (dialect) uses to
         * represent this logical type.
         */
        String toPhysical(Dialect dialect) {
            if (this == Integer
                || this == Currency
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
                case GREENPLUM:
                case LUCIDDB:
                case NETEZZA:
                case HSQLDB:
                    return name;
                case MARIADB:
                case MYSQL:
                case INFOBRIGHT:
                    return "TINYINT(1)";
                case MSSQL:
                case SYBASE:
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
                case INGRES:
                    return "INGRESDATE";
                default:
                    return name;
                }
            }
            if (this == Timestamp) {
                switch (dialect.getDatabaseProduct()) {
                case MSSQL:
                case MARIADB:
                case MYSQL:
                case INFOBRIGHT:
                case SYBASE:
                    return "DATETIME";
                case INGRES:
                    return "INGRESDATE";
                case INFORMIX:
                    return "DATETIME YEAR TO FRACTION(1)";
                default:
                    return name;
                }
            }
            throw new AssertionError("unexpected type: " + name);
        }
    }

    /**
     * Functor that evaluates a condition.
     *
     * @param <T> Argument type
     */
    private interface Condition<T> {
        /**
         * Evaluates the condition.
         * @param t Argument
         * @return Whether condition holds
         */
        boolean test(T t);
    }
}

// End MondrianFoodMartLoader.java
