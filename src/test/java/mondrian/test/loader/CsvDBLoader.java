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

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Implementation of {@link DBLoader} which gets its Tables by reading CSV files
 * using the {@link CsvLoader} class and is the loader use for CSV junit tests.
 *
 * <p>
 * <code>CsvDBLoader</code> requires that the CSV files have a specific format
 * as defined:
 *
 * <blockquote><pre>
 * list_of_csv_files : (csv_file)+
 * csv_file: table_definitions
 * table_definitions: (table_definition)+
 * table_definition: actions table_name column_names column_types
 *          (file_name or nos_of_rows or rows)
 * actions: (action)*
 * action: '##' (ActionBefore: | ActionAfter:) action_type
 * action_type: DropIndex index_name | CreateIndex index_name column_name
 * table_name: '##' TableName: table_name
 * column_names: '##' ColumnNames: column_name (',' column_name)*
 * column_types: '##' ColumnTypes: column_types ('.' column_types)*
 * file_name:'##' FileName: relative_filename ?
 * nos_of_rows:'##' NosOfRows: number
 * column_types: type (':' null)
 * type: "INTEGER" "DECIMAL(*,*)" "SMALLINT"
 *       "VARCHAR(*)" "REAL" "BOOLEAN"
 *       "BIGINT" "DATE" "TIMESTAMP"
 *  rows: (row)*
 *  row: value (',' value)*
 *
 *  if FileName is given, then
 *      there is no NosOfRows
 *      the file can only contains rows
 *  else if NosOfRows is given, then
 *      there is no FileName
 *      the number of rows in current file are rows for table
 *  else
 *      the all remaining rows in current file are rows for table
 *  fi
 *
 *  comment lines start with '#'
 *
 * </pre></blockquote>
 * <p>
 * See the src/test/resources/mondrian/rolap/aggmatcher/BUG_1541077.csv file
 * for an example.
 *
 *
 * @author Richard M. Emberson
 */
public class CsvDBLoader extends DBLoader {


    public static final String ACTION_BEFORE_TAG = "ActionBefore:";
    public static final String ACTION_AFTER_TAG = "ActionAfter:";
    public static final String DROP_INDEX_TAG = "DropIndex";
    public static final String CREATE_INDEX_TAG = "CreateIndex";

    public static final String TABLE_NAME_TAG = "TableName:";
    public static final String COLUMN_NAMES_TAG = "ColumnNames:";
    public static final String COLUMN_TYPES_TAG = "ColumnTypes:";
    public static final String FILE_NAME_TAG = "FileName:";
    public static final String NOS_OF_ROWS_TAG = "NosOfRows:";

    private File inputDirectory;
    private String inputDirectoryRegex;
    private File[] inputFiles;
    private File inputFile;

    public CsvDBLoader() {
        super();
    }

    public void setInputDirectory(File inputDirectory) {
        this.inputDirectory = inputDirectory;
    }

    public File getInputDirectory() {
        return this.inputDirectory;
    }

    public void setInputDirectoryRegex(String inputDirectoryRegex) {
        this.inputDirectoryRegex = inputDirectoryRegex;
    }

    public String getInputDirectoryRegex() {
        return this.inputDirectoryRegex;
    }

    public void setInputFiles(File[] inputFiles) {
        this.inputFiles = inputFiles;
    }

    public File[] getInputFiles() {
        return this.inputFiles;
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public File getInputFile() {
        return this.inputFile;
    }

    public Table[] getTables() throws Exception {
        initialize();

        if (this.inputDirectory != null) {
            return getTablesFromDirectory();
        } else if (this.inputFiles != null) {
            return getTablesFromFiles();
        } else if (this.inputFile != null) {
            return getTablesFromFile();
        } else {
            return new Table[0];
        }
    }

    public Table[] getTablesFromDirectory() throws Exception {
        File[] files;
        if (this.inputDirectoryRegex == null) {
            files = this.inputDirectory.listFiles();
        } else {
            final Pattern pat = Pattern.compile(this.inputDirectoryRegex);
            files = this.inputDirectory.listFiles(
                new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return pat.matcher(name).matches();
                    }
                }
            );
            if (files == null) {
                files = new File[0];
            }
        }
        return getTables(files);
    }

    public Table[] getTablesFromFiles() throws Exception {
        return getTables(this.inputFiles);
    }

    public Table[] getTablesFromFile() throws Exception {
        List<Table> list = new ArrayList<Table>();
        loadTables(this.inputFile, list);
        return list.toArray(new Table[list.size()]);
    }

    public Table[] getTables(File[] files) throws Exception {
        List<Table> list = new ArrayList<Table>();
        for (File file : files) {
            loadTables(file, list);
        }
        return list.toArray(new Table[list.size()]);
    }

    public Table[] getTables(Reader reader) throws Exception {
        List<Table> list = new ArrayList<Table>();
        loadTables(reader, list);
        return list.toArray(new Table[list.size()]);
    }

    public static class ListRowStream implements RowStream {
        private List<Row> list;
        ListRowStream() {
            this(new ArrayList<Row>());
        }
        ListRowStream(List<Row> list) {
            this.list = list;
        }
        void add(Row row) {
            this.list.add(row);
        }
        public Iterator<Row> iterator() {
            return this.list.iterator();
        }
    }

    public static class CsvLoaderRowStream implements RowStream {
        private final CsvLoader csvloader;
        CsvLoaderRowStream(CsvLoader csvloader) {
            this.csvloader = csvloader;
        }
        public Iterator<Row> iterator() {
            return new Iterator<Row>() {
                String[] line;
                public boolean hasNext() {
                    try {
                        boolean hasNext =
                            CsvLoaderRowStream.this.csvloader.hasNextLine();
                        if (! hasNext) {
                            CsvLoaderRowStream.this.csvloader.close();
                        } else {
                            line = CsvLoaderRowStream.this.csvloader.nextLine();
                            // remove comment lines
                            if (line.length > 0
                                && line[0].length() > 0
                                && line[0].startsWith("#"))
                            {
                                return hasNext();
                            }
                        }
                        return hasNext;
                    } catch (IOException ex) {
                        //
                    }
                    return false;
                }
                public Row next() {
                    return new RowDefault(line);
/*
                    return new RowDefault(
                        CsvLoaderRowStream.this.csvloader.nextLine());
*/
                }
                public void remove() {
                }
            };
        }
    }

    /**
     * Looks for a file relative to the current directory and all of its
     * parent directories. This gives a little flexibility if you invoke a
     * test in a sub-directory.
     *
     * @param file File
     * @return File within current directory or a parent directory.
     */
    private static File resolveFile(File file) {
        if (file.isAbsolute()) {
            return file;
        } else {
            File base = new File(System.getProperty("user.dir"));
            while (base != null) {
                File file2 = new File(base, file.getPath());
                if (file2.exists()) {
                    return file2;
                }
                base = base.getParentFile();
            }
        }
        return file;
    }

    public void loadTables(File file, List<Table> tableList) throws Exception {
        Reader reader = new FileReader(resolveFile(file));
        loadTables(reader, tableList);
    }

    public void loadTables(Reader reader, List<Table> tableList)
        throws Exception
    {
        CsvLoader csvloader = null;
        try {
            Table table = null;
            List<String> beforeActionList = new ArrayList<String>();
            List<String> afterActionList = new ArrayList<String>();
            String tableName = null;
            String[] columnNames = null;
            String[] columnTypes = null;
            String fileName = null;
            int nosOfRowsStr = -1;
            boolean ok = false;
            Column[] columns;
            csvloader = new CsvLoader(reader);
            int lineNos = 0;
            while (csvloader.hasNextLine()) {
                String[] values = csvloader.nextLine();
                lineNos++;
//System.out.println("CsvLoader.loadTables: lineNos=" +lineNos);
                if (values.length == 0) {
                    continue;
                }
                String value0 = values[0];
//System.out.println("CsvLoader.loadTables: value0=" +value0);
                if (value0.startsWith("##") && (fileName == null)) {
                    if (table != null) {
                        table = null;
                        beforeActionList.clear();
                        afterActionList.clear();
                        tableName = null;
                        columnNames = null;
                        columnTypes = null;
                        fileName = null;
                        nosOfRowsStr = -1;
                    }
                    // meta info
                    int index = value0.indexOf(ACTION_BEFORE_TAG);
                    if (index != -1) {
                        String s = value0.substring(
                            index + ACTION_BEFORE_TAG.length());
                        if (s.length() == 0) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no action before sql"
                                + ", linenos " + lineNos);
                        }
                        s = s.trim();
                        if (! s.startsWith(DROP_INDEX_TAG)) {
                            // only support dropping indexes currently
                            throw new IOException(
                                "CSV File parse Error: "
                                + " unknown before action" + s
                                + ", linenos " + lineNos);
                        }
                        // get index name
                        index = s.indexOf(' ');
                        if (index < 0) {
                            // only support dropping indexes currently
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no index name in before action" + s
                                + ", linenos " + lineNos);
                        }
                        s = s.substring(index + 1);
                        s = s.trim();
                        beforeActionList.add(s);
                        continue;
                    }

                    index = value0.indexOf(ACTION_AFTER_TAG);
                    if (index != -1) {
                        String s = value0.substring(
                            index + ACTION_AFTER_TAG.length());
                        if (s.length() == 0) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no action after sql"
                                + ", linenos " + lineNos);
                        }
                        s = s.trim();
                        if (! s.startsWith(CREATE_INDEX_TAG)) {
                            // only support creating indexes currently
                            throw new IOException(
                                "CSV File parse Error: "
                                + " unknown before action" + s
                                + ", linenos " + lineNos);
                        }
                        // get index name
                        index = s.indexOf(' ');
                        if (index < 0) {
                            // only support creating indexes currently
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no index_name/column_name in after action"
                                + s + ", linenos " + lineNos);
                        }
                        // CreateIndex index_name column_name
                        s = s.substring(index + 1);
                        s = s.trim();
                        index = s.indexOf(' ');
                        // just check that there is a space and
                        if (index < 0) {
                            // only support creating indexes currently
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no column_name after index_name "
                                + "in after action" + s
                                + ", linenos " + lineNos);
                        }
                        afterActionList.add(s);
                        continue;
                    }

                    index = value0.indexOf(TABLE_NAME_TAG);
                    if (index != -1) {
                        String s = value0.substring(
                            index + TABLE_NAME_TAG.length());
                        if (s.length() == 0) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no table name"
                                + ", linenos " + lineNos);
                        }
                        s = s.trim();
                        if (tableName != null) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " new table name \"" + s
                                + "\" while processing table name \""
                                + tableName
                                + "\", linenos " + lineNos);
                        }
                        tableName = s;
                        continue;
                    }
                    index = value0.indexOf(COLUMN_NAMES_TAG);
                    if (index != -1) {
                        String s = value0.substring(
                            index + COLUMN_NAMES_TAG.length());
                        if (s.length() == 0) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no column names"
                                + ", linenos " + lineNos);
                        }
                        if (tableName == null) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no table name for columns "
                                + ", linenos " + lineNos);
                        }
                        values[0] = s.trim();
                        columnNames = values;
                        continue;
                    }
                    index = value0.indexOf(COLUMN_TYPES_TAG);
                    if (index != -1) {
                        String s = value0.substring(
                            index + COLUMN_TYPES_TAG.length());
                        if (s.length() == 0) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no column types"
                                + ", linenos " + lineNos);
                        }
                        if (columnNames == null) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no column names for columns types"
                                + ", linenos " + lineNos);
                        }
                        values[0] = s.trim();
                        columnTypes = values;

                        if (columnNames.length != columnTypes.length) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " number of column names \""
                                + columnNames.length
                                + "\" does not equal "
                                + " number of column types \""
                                + columnTypes.length
                                + "\", linenos " + lineNos);
                        }
                        continue;
                    }

                    ok = true;

                    index = value0.indexOf(FILE_NAME_TAG);
                    if (index != -1) {
                        String s = value0.substring(
                            index + FILE_NAME_TAG.length());
                        if (s.length() == 0) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no file name "
                                + ", linenos " + lineNos);
                        }
                        if (columnTypes == null) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no column types for file name"
                                + ", linenos " + lineNos);
                        }
                        fileName = s.trim();
                        continue;
                    }
                    index = value0.indexOf(NOS_OF_ROWS_TAG);
                    if (index != -1) {
                        String s = value0.substring(
                            index + NOS_OF_ROWS_TAG.length());
                        if (s.length() == 0) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no number of rows"
                                + ", linenos " + lineNos);
                        }
                        if (columnTypes == null) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no column types for file name"
                                + ", linenos " + lineNos);
                        }
                        nosOfRowsStr = Integer.parseInt(s.trim());
                        continue;
                    }
                } else if (value0.startsWith("# ")) {
                    // comment, do nothing
                } else {
//System.out.println("CsvLoader.loadTables: ELSE");
                    // rows
                    if (! ok) {
                        if (tableName == null) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no table name before rows"
                                + ", linenos " + lineNos);
                        }
                        if (columnNames == null) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no column names before rows"
                                + ", linenos " + lineNos);
                        }
                        if (columnTypes == null) {
                            throw new IOException(
                                "CSV File parse Error: "
                                + " no column types before rows"
                                + ", linenos " + lineNos);
                        }
                    }
                    columns = loadColumns(columnNames, columnTypes, lineNos);
                    table = new Table(tableName, columns);
                    table.setBeforeActions(beforeActionList);
                    table.setAfterActions(afterActionList);
                    tableList.add(table);
                    Table.Controller controller = table.getController();

                    if (fileName != null) {
//System.out.println("CsvLoader.loadTables: fileName="+fileName);
                        RowStream rowStream = new CsvLoaderRowStream(
                            new CsvLoader(fileName));
                        controller.setRowStream(rowStream);
                        csvloader.nextSet();
                        csvloader.putBack(values);
//System.out.println("CsvLoader.loadTables: fileName putback0=" +values[0]);
                        lineNos--;
                        fileName = null;
//System.out.println("CsvLoader.loadTables: fileName OK");

                    } else if (nosOfRowsStr != -1) {
//System.out.println("CsvLoader.loadTables: nosOfRowsStr="+nosOfRowsStr);
                        List<Row> list = new ArrayList<Row>();

                        list.add(new RowDefault(values));
                        nosOfRowsStr--;

                        while (nosOfRowsStr-- > 0) {
                            if (! csvloader.hasNextLine()) {
                                throw new Exception(
                                    "CSV File parse Error: "
                                    + " not enough lines in file "
                                    + lineNos);
                            }
                            values = csvloader.nextLine();
value0 = values[0];
if (value0.startsWith("# ")) {
    nosOfRowsStr++;
    continue;
}
//System.out.println("CsvLoader.loadTables: v0="+values[0]);
                            list.add(new RowDefault(values));
                            lineNos++;
                        }

                        RowStream rowStream = new ListRowStream(list);
                        controller.setRowStream(rowStream);
                        csvloader.nextSet();
//System.out.println("CsvLoader.loadTables: nosOfRowsStr OK");

                    } else {
//System.out.println("CsvLoader.loadTables: else");
                        csvloader.putBack(values);
                        RowStream rowStream =
                            new CsvLoaderRowStream(csvloader);
                        controller.setRowStream(rowStream);
                        csvloader = null;
                        break;
                    }
                }
            }
        } finally {
            if (csvloader != null) {
                csvloader.close();
            }
        }
//System.out.println("CsvLoader.loadTables: BOTTOM:");
    }

    protected Column[] loadColumns(
        String[] columnNames, String[] columnTypes, int lineNos)
        throws Exception
    {
        List<Column> list = new ArrayList<Column>();
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
//System.out.println("columnName="+columnName);
            String columnType = columnTypes[i];
//System.out.println("columnType="+columnType);
            int index = columnType.indexOf(':');
//System.out.println("index="+index);
            Type type;
            boolean nullsAllowed = false;
            if (index != -1) {
                String nullString = columnType.substring(index + 1).trim();
                if (nullString.equalsIgnoreCase("NULL")) {
                    nullsAllowed = true;
                } else {
                    throw new IOException(
                        "CSV File parse Error: "
                        + " for type name \""
                        + columnType
                        + "\" expecting \"null\" not \""
                        + nullString
                        + "\", linenos " + lineNos);
                }
                columnType = columnType.substring(0, index).trim();
//System.out.println("columnType="+columnType);
                type = Type.getType(columnType);
//System.out.println("nullsAllowed="+nullsAllowed);
            } else {
                type = Type.getType(columnType);
            }
            if (type == null) {
                type = Type.makeType(columnType);
            }
            if (type == null) {
                throw new IOException(
                    "CSV File parse Error: "
                    + " no type found for type name \""
                    + columnType
                    + "\", linenos " + lineNos);
            }
            Column column = new Column(columnName, type, nullsAllowed);
            list.add(column);
        }
        return list.toArray(new Column[list.size()]);
    }

    protected void check() throws Exception {
        super.check();

        if (this.inputDirectory != null) {
            if (this.inputFiles != null) {
                throw new Exception(
                    "Both input Directory and input files can not be set");
            }
            if (this.inputFile != null) {
                throw new Exception(
                    "Both input Directory and input file can not be set");
            }
        }
        if (this.inputFiles != null) {
            if (this.inputFile != null) {
                throw new Exception(
                    "Both input input files and input file can not be set");
            }
        }
    }

    protected static File checkDirectory(String dirName) throws Exception {
        File dir = new File(dirName);
        if (! dir.exists()) {
            throw new Exception(
                "The directory \"" + dirName + "\" does not exist");
        }
        if (! dir.isDirectory()) {
            throw new Exception(
                "The file \"" + dirName + "\" is not a directory");
        }
        return dir;
    }

    protected static void usage(String msg) {
        StringBuilder buf = new StringBuilder(500);
        if (msg != null) {
            buf.append(msg);
            buf.append(nl);
        }
        buf.append("Usage: CsvDBLoader ");
        buf.append(" [-h or -help]");
        buf.append(" (-p <propertyFile>");
        buf.append(" or -p=<propertyFile>)");
        buf.append(" (-jdbcDrivers <jdbcDriver>");
        buf.append(" or -jdbcDrivers=<jdbcDriver>)");
        buf.append(" (-jdbcURL <jdbcURL>");
        buf.append(" or -jdbcURL=<jdbcURL>)");
        buf.append(" [-user username");
        buf.append(" or -user=username]");
        buf.append(" [-password password");
        buf.append(" or -password=password]");
        buf.append(" [-BatchSize <batch size>");
        buf.append(" or -BatchSize=<batch size>]");
        buf.append(" [-outputDirectory <directory name>");
        buf.append(" or -outputDirectory <directory name>]");
        buf.append(" [-f or -force]");
        buf.append(" [-inputDirectory <directory name>");
        buf.append(" or -inputDirectory <directory name>]");
        buf.append(" [-regex <regular expression>");
        buf.append(" or -regex <regular expression>]");
        buf.append(" (inputFiles)+");
        buf.append(nl);

        buf.append("Options:");
        buf.append(nl);
        buf.append(
            "  <propertyFile>        A property file which can be used to");
        buf.append(nl);
        buf.append("                        to set some of the options.");
        buf.append(nl);
        buf.append(
            "  <jdbcDrivers>         Comma-separated list of JDBC drivers;");
        buf.append(nl);
        buf.append("  <jdbcURL>             JDBC connect string for DB.");
        buf.append(nl);
        buf.append("  [username]            JDBC user name for DB.");
        buf.append(nl);
        buf.append("  [password]            JDBC password for user for DB.");
        buf.append(nl);
        buf.append(
            "  <batch size>          "
            + "Size of JDBC batch updates - default to 50 inserts.");
        buf.append(nl);
        buf.append(
            "  [outputDirectory]     "
            + "Directory where per-table sql should be put");
        buf.append(nl);
        buf.append("                        rather than loading the database.");
        buf.append(nl);
        buf.append(
            "  [force]               "
            + "If output files already exist, delete them");
        buf.append(nl);
        buf.append("  [inputDirectory]     Directory containing input files");
        buf.append(nl);
        buf.append(
            "  [regular expression] A regular expression used to determine");
        buf.append(nl);
        buf.append(
            "                       "
            + "which files in the input directory to use.");
        buf.append(nl);
        buf.append("The values in the property file are overridden by");
        buf.append(nl);
        buf.append("  the explicit command line options");


        System.out.println(buf.toString());
        System.exit((msg == null) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        String propFile = null;
        String jdbcDrivers = null;
        String jdbcURL = null;
        String user = null;
        String password = null;
        String batchSizeStr = null;
        String outputDirectory = null;
        boolean force = false;
        List<File> files = new ArrayList<File>();
        String inputDirectory = null;
        String regex = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-h") || arg.equals("-help")) {
                usage(null);

            } else if (arg.equals("-f") || arg.equals("-force")) {
                force = true;

            } else if (arg.startsWith("-p=")) {
                propFile = args[i].substring("-p=".length());

            } else if (arg.equals("-p")) {
                if (++i == args.length) {
                    usage("Missing argument for -p");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -p: " + arg);
                }
                propFile = arg;

            } else if (arg.startsWith("-jdbcDrivers=")) {
                jdbcDrivers = args[i].substring("-jdbcDrivers=".length());
            } else if (arg.equals("-jdbcDrivers")) {
                if (++i == args.length) {
                    usage("Missing argument for -jdbcDrivers");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -jdbcDrivers: " + arg);
                }
                jdbcDrivers = arg;

            } else if (arg.startsWith("-jdbcURL=")) {
                jdbcURL = args[i].substring("-jdbcURL=".length());
            } else if (arg.equals("-jdbcURL")) {
                if (++i == args.length) {
                    usage("Missing argument for -jdbcURL");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -jdbcURL: " + arg);
                }
                jdbcURL = arg;

            } else if (arg.startsWith("-user=")) {
                user = args[i].substring("-user=".length());
            } else if (arg.equals("-user")) {
                if (++i == args.length) {
                    usage("Missing argument for -user");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -user: " + arg);
                }
                user = arg;

            } else if (arg.startsWith("-password=")) {
                password = args[i].substring("-password=".length());
            } else if (arg.equals("-password")) {
                if (++i == args.length) {
                    usage("Missing argument for -password");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -password: " + arg);
                }
                password = arg;

            } else if (arg.startsWith("-batchSize=")) {
                batchSizeStr = args[i].substring("-batchSize=".length());
            } else if (arg.equals("-batchSize")) {
                if (++i == args.length) {
                    usage("Missing argument for -batchSize");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -batchSize: " + arg);
                }
                batchSizeStr = arg;

            } else if (arg.startsWith("-outputDirectory=")) {
                outputDirectory =
                    args[i].substring("-outputDirectory=".length());
            } else if (arg.equals("-outputDirectory")) {
                if (++i == args.length) {
                    usage("Missing argument for -outputDirectory");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -outputDirectory: " + arg);
                }
                outputDirectory = arg;

            } else if (arg.startsWith("-inputDirectory=")) {
                inputDirectory = args[i].substring("-inputDirectory=".length());
            } else if (arg.equals("-inputDirectory")) {
                if (++i == args.length) {
                    usage("Missing argument for -inputDirectory");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -inputDirectory: " + arg);
                }
                inputDirectory = arg;

            } else if (arg.startsWith("-regex=")) {
                regex = args[i].substring("-regex=".length());
            } else if (arg.equals("-regex")) {
                if (++i == args.length) {
                    usage("Missing argument for -regex");
                }
                arg = args[i];
                if (arg.startsWith("-")) {
                    usage("Bad argument for -regex: " + arg);
                }
                regex = arg;

            } else if (arg.startsWith("-")) {
                usage("Bad option : " + arg);
            } else {
                File file = new File(arg);
                if (! file.exists()) {
                    String msg = "The file \"" + arg + "\" does not exist";
                    throw new Exception(msg);
                }
                files.add(file);
            }
        }

        CsvDBLoader loader = new CsvDBLoader();
        if (propFile != null) {
            Properties props = new Properties();
            props.load(new FileInputStream(propFile));

            String v = props.getProperty(BATCH_SIZE_PROP);
            if (v != null) {
                loader.setBatchSize(Integer.parseInt(v));
            }
            v = props.getProperty(JDBC_DRIVER_PROP);
            if (v != null) {
                loader.setJdbcDriver(v);
            }
            v = props.getProperty(JDBC_URL_PROP);
            if (v != null) {
                loader.setJdbcURL(v);
            }
            v = props.getProperty(JDBC_USER_PROP);
            if (v != null) {
                loader.setUserName(v);
            }
            v = props.getProperty(JDBC_PASSWORD_PROP);
            if (v != null) {
                loader.setPassword(v);
            }
            v = props.getProperty(OUTPUT_DIRECTORY_PROP);
            if (v != null) {
                File dir = checkDirectory(v);
                loader.setOutputDirectory(dir);
            }
            v = props.getProperty(FORCE_PROP);
            if (v != null) {
                force = Boolean.valueOf(v);
            }
        }

        if (batchSizeStr != null) {
            loader.setBatchSize(Integer.parseInt(batchSizeStr));
        }
        if (jdbcDrivers != null) {
            loader.setJdbcDriver(jdbcDrivers);
        }
        if (jdbcURL != null) {
            loader.setJdbcURL(jdbcURL);
        }
        if (user != null) {
            loader.setUserName(user);
        }
        if (password != null) {
            loader.setPassword(password);
        }
        if (outputDirectory != null) {
            File dir = checkDirectory(outputDirectory);
            loader.setOutputDirectory(dir);
        }
        loader.setForce(force);

        if (files.size() == 1) {
            loader.setInputFile(files.get(0));
        } else if (files.size() > 1) {
            loader.setInputFiles(files.toArray(new File[files.size()]));
        }

        if (inputDirectory != null) {
            File dir = checkDirectory(inputDirectory);
            loader.setInputDirectory(dir);
        }
        if (regex != null) {
            loader.setInputDirectoryRegex(regex);
        }

        loader.initialize();
        Table[] tables = loader.getTables();
        loader.generateStatements(tables);
        loader.executeStatements(tables);
    }
}

// End CsvDBLoader.java
