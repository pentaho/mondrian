/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.tui;

import mondrian.olap.*;
import mondrian.olap.fun.FunInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Command line utility which reads and executes MDX commands.
 *
 * <p>TODO: describe how to use this class.</p>
 *
 * @author Richard Emberson
 * @version $Id$
 */
public class CmdRunner {

    private static boolean RELOAD_CONNECTION = true;

    /** Name of the property "mondrian.catalogURL". */
    public static final String CATALOG_URL = "mondrian.catalogURL";


    /** List of all properties of interest to this utility. */
    private static final String[] propertyNames = {
        MondrianProperties.QueryLimit,
        MondrianProperties.TraceLevel,
        MondrianProperties.DebugOutFile,
        MondrianProperties.JdbcDrivers,
        MondrianProperties.ResultLimit,
        MondrianProperties.TestConnectString,
        MondrianProperties.JdbcURL,
        CmdRunner.CATALOG_URL,
        MondrianProperties.LargeDimensionThreshold,
        MondrianProperties.SparseSegmentCountThreshold,
        MondrianProperties.SparseSegmentDensityThreshold,
    };

    private String filename;
    private String mdxCmd;
    private String mdxResult;
    private String error;
    private String stack;
    private String connectString;
    private Connection connection;

    /**
     * Creates a <code>CmdRunner</code>.
     */
    public CmdRunner() {
        this.filename = null;
        this.mdxCmd = null;
        this.mdxResult = null;
        this.error = null;
    }

    void setError(Throwable t) {
        this.error = formatError(t);
//System.out.println("CmdRunner.setError: error=" +error);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();
        this.stack = sw.toString();
    }

    void clearError() {
        this.error = null;
        this.stack = null;
    }

	private String formatError(Throwable mex) {
		String message = mex.getMessage();
		if (mex.getCause() != null && mex.getCause() != mex)
			message = message + "\n" + formatError(mex.getCause());
		return message;
	}

    public static void listPropertyNames(StringBuffer buf) {
        for (int i = 0; i < CmdRunner.propertyNames.length; i++) {
            String propertyName = CmdRunner.propertyNames[i];
            buf.append(propertyName);
            buf.append('\n');
        }
    }

    public static void listPropertiesAll(StringBuffer buf) {
        for (int i = 0; i < CmdRunner.propertyNames.length; i++) {
            String propertyName = CmdRunner.propertyNames[i];
            String propertyValue = getPropertyValue(propertyName);
            buf.append(propertyName);
            buf.append('=');
            buf.append(propertyValue);
            buf.append('\n');
        }
    }

    private static String getPropertyValue(String propertyName) {
        if (propertyName.equals(CmdRunner.CATALOG_URL)) {
            return System.getProperty(CmdRunner.CATALOG_URL);
        } else {
            return MondrianProperties.instance().getProperty(propertyName);
        }
    }

    public static void listProperty(String propertyName, StringBuffer buf) {
        buf.append(getPropertyValue(propertyName));
    }

    public static boolean isProperty(String propertyName) {
        for (int i = 0; i < CmdRunner.propertyNames.length; i++) {
            if (CmdRunner.propertyNames[i].equals(propertyName)) {
                return true;
            }
        }
        return false;
    }

    public static boolean setProperty(String name, String value) {
        String oldValue = getPropertyValue(name);
        if (! Util.equals(oldValue, value)) {
            if (name.equals(CmdRunner.CATALOG_URL)) {
                System.setProperty(CmdRunner.CATALOG_URL, value);
            } else {
                MondrianProperties.instance().setProperty(name, value);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Executes a query and returns the result as a string.
     *
     * @param queryString MDX query text
     * @return result String
     */
    public String execute(String queryString) {
        Result result = runQuery(queryString);
        String resultString = toString(result);
        return resultString;
    }

    /**
     * Executes a query and returns the result.
     *
     * @param queryString MDX query text
     * @return a {@link Result} object
     */
    public Result runQuery(String queryString) {
        CmdRunner.debug("CmdRunner.runQuery: TOP");
        try {
            Connection connection = getConnection();
            CmdRunner.debug("CmdRunner.runQuery: AFTER getConnection");
            Query query = connection.parseQuery(queryString);
            CmdRunner.debug("CmdRunner.runQuery: AFTER parseQuery");
            return connection.execute(query);
        } finally {
            CmdRunner.debug("CmdRunner.runQuery: BOTTOM");
        }
    }


    /**
     * Converts a {@link Result} object to a string
     *
     * @param result
     * @return String version of mondrian Result object.
     */
    public String toString(Result result) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        result.print(pw);
        pw.flush();
        return sw.toString();
    }


    public void makeConnectString() {
        String connectString = CmdRunner.getConnectStringProperty();
        CmdRunner.debug("CmdRunner.makeConnectString: connectString="+connectString);

        Util.PropertyList connectProperties = null;
        if (connectString == null || connectString.equals("")) {
            // create new and add provider
            connectProperties = new Util.PropertyList();
            connectProperties.put("Provider","mondrian");
        } else {
            // load with existing connect string
            connectProperties = Util.parseConnectString(connectString);
        }

        // override jdbc url
        String jdbcURL = CmdRunner.getJdbcURLProperty();

        CmdRunner.debug("CmdRunner.makeConnectString: jdbcURL="+jdbcURL);

        if (jdbcURL != null) {
            // add jdbc url to connect string
            connectProperties.put("Jdbc", jdbcURL);
        }

        // override jdbc drivers
        String jdbcDrivers = CmdRunner.getJdbcDriversProperty();

        CmdRunner.debug("CmdRunner.makeConnectString: jdbcDrivers="+jdbcDrivers);
        if (jdbcDrivers != null) {
            // add jdbc drivers to connect string
            connectProperties.put("JdbcDrivers", jdbcDrivers);
        }

        // override catalog url
        String catalogURL = CmdRunner.getCatalogURLProperty();

        CmdRunner.debug("CmdRunner.makeConnectString: catalogURL="+catalogURL);

        if (catalogURL != null) {
            // add catalog url to connect string
            connectProperties.put("catalog", catalogURL);
        }

        CmdRunner.debug("CmdRunner.makeConnectString: connectProperties="+connectProperties);

        this.connectString = connectProperties.toString();
    }

    /**
     * Gets a connection to Mondrian.
     *
     * @return Mondrian {@link Connection}
     */
    public Connection getConnection() {
        return getConnection(CmdRunner.RELOAD_CONNECTION);
    }

    /**
     * Gets a Mondrian connection, creating a new one if fresh is true.
     *
     * @param fresh
     * @return mondrian Connection.
     */
    public synchronized Connection getConnection(boolean fresh) {
        if (this.connectString == null) {
            makeConnectString();
        }
        if (fresh) {
            return DriverManager.getConnection(this.connectString, null, fresh);
        } else if (connection == null) {
            connection =
                DriverManager.getConnection(this.connectString, null, fresh);
        }
        return connection;
    }

    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////
    //
    // static methods
    //
    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////
    private static boolean debug = false;

    protected static void debug(String msg) {
        if (CmdRunner.debug) {
            System.out.println(msg);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // properties
    /////////////////////////////////////////////////////////////////////////
    protected static String getConnectStringProperty() {
        return MondrianProperties.instance().getTestConnectString();
    }
    protected static String getJdbcURLProperty() {
        return MondrianProperties.instance().getFoodmartJdbcURL();
    }
    protected static String getCatalogURLProperty() {
        return System.getProperty(CmdRunner.CATALOG_URL);
    }
    protected static String getJdbcDriversProperty() {
        return MondrianProperties.instance().getJdbcDrivers();
    }

    /////////////////////////////////////////////////////////////////////////
    // command loop
    /////////////////////////////////////////////////////////////////////////

    protected void commandLoop(boolean interactive) throws IOException {
        commandLoop(System.in, interactive);
    }

    protected void commandLoop(File file) throws IOException {
        // If we open a stream, then we close it.
        InputStream in = new FileInputStream(file);
        try {
            commandLoop(in, false);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ex) {
                    // ignore
                }
            }
        }
    }

    protected void commandLoop(String mdxCmd, boolean interactive)
        throws IOException {

        StringBufferInputStream is = new StringBufferInputStream(mdxCmd);
        commandLoop(is, interactive);
    }

    private static final String COMMAND_PROMPT_START = "> ";
    private static final String COMMAND_PROMPT_MID = "? ";

    /**
     * The Command Loop where lines are read from the InputStream and
     * interpreted. If interactive then prompts are printed.
     *
     * @param in Input stream
     * @param interactive Whether the session is interactive
     * @throws IOException if stream can not be accessed
     */
    protected void commandLoop(InputStream in, boolean interactive)
        throws IOException {

        StringBuffer buf = new StringBuffer(2048);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        boolean inMDXCmd = false;
    	String resultString = null;

        for(;;) {
            if (resultString != null) {
                printResults(resultString);
                resultString = null;
            }
            if (interactive) {
            	if (inMDXCmd)
                    System.out.print(COMMAND_PROMPT_MID);
            	else
            		System.out.print(COMMAND_PROMPT_START);
            }
            if (!inMDXCmd) {
                buf.setLength(0);
            	//buf = new StringBuffer(2048);
            }
            String line = readLine(br);
            if (line != null) {
                line = line.trim();
            }
            debug("line="+line);

            if (! inMDXCmd) {
                // If not in the middle of reading an mdx query and
                // we reach end of file on the stream, then we are over.
                if (line == null) {
                    return;
                }
            }

            // If not reading an mdx query, then check if the line is a
            // user command.
            if (! inMDXCmd) {
                String cmd = line;
                if (cmd.startsWith("help")) {
                    resultString = executeHelp(cmd);
                } else if (cmd.startsWith("set")) {
                    resultString = executeSet(cmd);
                } else if (cmd.startsWith("log")) {
                    resultString = executeLog(cmd);
                } else if (cmd.startsWith("file")) {
                    resultString = executeFile(cmd);
                } else if (cmd.startsWith("list")) {
                    resultString = executeList(cmd);
                } else if (cmd.startsWith("func")) {
                    resultString = executeFunc(cmd);
                } else if (cmd.startsWith("error")) {
                    resultString = executeError(cmd);
                } else if (cmd.startsWith("exit")) {
                    break;
                }
                if (resultString != null) {
                    inMDXCmd = false;
                    continue;
                }
            }

            // Are we ready to execute an mdx query.
            if ((line == null) ||
                    ((line.length() == 1) &&
                    ((line.charAt(0) == EXECUTE_CHAR) ||
                        (line.charAt(0) == CANCEL_CHAR)) )) {

                // If EXECUTE_CHAR, then execute, otherwise its the
                // CANCEL_CHAR and simply empty buffer.
                if ((line == null) || (line.charAt(0) == EXECUTE_CHAR)) {
                    String mdxCmd = buf.toString().trim();
                    debug("mdxCmd=\""+mdxCmd+"\"");

                    resultString = executeMDXCmd(mdxCmd);
                }

                inMDXCmd = false;

            } else if (line.length() > 0) {
                // OK, just add the line to the mdx query we are building.
                inMDXCmd = true;
                buf.append(line);
                if (line.endsWith(";")) {
                    String mdxCmd = buf.toString().trim();
                    debug("mdxCmd=\""+mdxCmd+"\"");
                    resultString = executeMDXCmd(mdxCmd);
                    inMDXCmd = false;
                } else {
	                // add carriage return so that query keeps formatting
	                buf.append('\n');
	            }
            }
        }
    }

    protected static void printResults(String resultString) {
        if (resultString != null) {
            resultString = resultString.trim();
            if (resultString.length() > 0) {
                System.out.println(resultString);
            }
        }
    }

    /**
     * Gather up a line ending in '\n' or EOF.
     * Returns null if at EOF.
     * Strip out comments. If a comment character appears within a
     * string then its not a comment. Strings are defined with "\"" or
     * "'" characters. Also, a string can span more than one line (a
     * nice little complication). So, if we read a string, then we consume
     * the whole string as part of the "line" returned,
     * including EOL characters.
     * If an escape character is seen '\\', then it and the next character
     * is added to the line regardless of what the next character is.
     *
     * @param reader
     * @return
     * @throws IOException
     */
    protected static String readLine(Reader reader) throws IOException {
        StringBuffer buf = null;

        int i = reader.read();
        for (;;) {
            if (i == -1) {
                // At EOF, return what we've read so far.
                return (buf == null) ? null : buf.toString();
            }
            char c = (char) i;

            if (buf == null) {
                buf = new StringBuffer(128);
            }

            if (c == ESCAPE_CHAR) {
                buf.append(c);

                i = reader.read();
                if (i == -1) {
                    // At EOF, return what we've read so far.
                    return buf.toString();
                }
                buf.append((char)i);

                i = reader.read();
                continue;
            }

            // At EOL, return what we've read so far.
            if ((c == '\n') || (c == '\r')) {
                return buf.toString();
            }
            // comment handling
            if (c == COMMENT_CHAR) {
                // Not in string, read to EOL or EOF
                i = reader.read();
                for (;;) {
                    if (i == -1) {
                        return buf.toString();
                    }
                    c = (char) i;
                    if ((c == '\n') || (c == '\r')) {
                        return buf.toString();
                    }
                    i = reader.read();
                }
                // In string, do nothing special with comment character
            }

            if ((c == STRING_CHAR_1) || (c == STRING_CHAR_2)) {
                // Start of a string, read all of it even if it spans
                // more than one line adding each line's <cr> to the
                // buffer.

                char str_char = c;
                buf.append(c);

                i = reader.read();

                STRING_LOOP:
                for (;;) {
                    if (i == -1) {
                        return buf.toString();
                    }
                    c = (char) i;

                    if (c == ESCAPE_CHAR) {
                        buf.append(c);

                        i = reader.read();
                        if (i == -1) {
                            // At EOF, return what we've read so far.
                            return buf.toString();
                        }
                        buf.append((char)i);

                        i = reader.read();
                        continue STRING_LOOP;
                    }

                    buf.append(c);

                    if (c == str_char) {
                        break STRING_LOOP;
                    }

                    i = reader.read();
                }


            } else {
                buf.append(c);
            }

            i = reader.read();
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // user commands and help messages
    /////////////////////////////////////////////////////////////////////////
    private static final String INDENT = "  ";

    private static final int UNKNOWN_CMD        = 0x000;
    private static final int HELP_CMD           = 0x001;
    private static final int SET_CMD            = 0x002;
    private static final int LOG_CMD            = 0x004;
    private static final int FILE_CMD           = 0x008;
    private static final int LIST_CMD           = 0x010;
    private static final int MDX_CMD            = 0x020;
    private static final int FUNC_CMD           = 0x040;
    private static final int ERROR_CMD          = 0x080;
    private static final int EXIT_CMD           = 0x100;
    private static final int ALL_CMD  = HELP_CMD |
                                        SET_CMD |
                                        LOG_CMD |
                                        FILE_CMD |
                                        LIST_CMD |
                                        MDX_CMD |
                                        FUNC_CMD |
                                        ERROR_CMD |
                                        EXIT_CMD;

    private static final char ESCAPE_CHAR        = '\\';
    private static final char EXECUTE_CHAR        = '=';
    private static final char CANCEL_CHAR         = '~';
    private static final char COMMENT_CHAR        = '#';
    private static final char STRING_CHAR_1       = '"';
    private static final char STRING_CHAR_2       = '\'';

    protected static String executeHelp(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s");

        int cmd = UNKNOWN_CMD;

        if (tokens.length == 1) {
            buf.append("Commands:");
            cmd = ALL_CMD;

        } else if (tokens.length == 2) {
            String cmdName = tokens[1];

            if (cmdName.equals("help")) {
                cmd = HELP_CMD;
            } else if (cmdName.equals("set")) {
                cmd = SET_CMD;
            } else if (cmdName.equals("log")) {
                cmd = LOG_CMD;
            } else if (cmdName.equals("file")) {
                cmd = FILE_CMD;
            } else if (cmdName.equals("list")) {
                cmd = LIST_CMD;
            } else if (cmdName.equals("error")) {
                cmd = ERROR_CMD;
            } else if (cmdName.equals("exit")) {
                cmd = EXIT_CMD;
            } else {
                cmd = UNKNOWN_CMD;
            }
        }

        if (cmd == UNKNOWN_CMD) {
            buf.append("Unknown help command: ");
            buf.append(mdxCmd);
            buf.append('\n');
            buf.append("Type \"help\" for list of commands");
        }

        if ((cmd & HELP_CMD) != 0) {
            // help
            buf.append('\n');
            appendIndent(buf, 1);
            buf.append("help");
            buf.append('\n');
            appendIndent(buf, 2);
            buf.append("Prints this text");
        }

        if ((cmd & SET_CMD) != 0) {
            // set
            buf.append('\n');
            appendSet(buf);
        }

        if ((cmd & LOG_CMD) != 0) {
            // set
            buf.append('\n');
            appendLog(buf);
        }

        if ((cmd & FILE_CMD) != 0) {
            // file
            buf.append('\n');
            appendFile(buf);

        }
        if ((cmd & LIST_CMD) != 0) {
            // list
            buf.append('\n');
            appendList(buf);
        }

        if ((cmd & MDX_CMD) != 0) {
            buf.append('\n');
            appendIndent(buf, 1);
            buf.append("<mdx query> <cr> ( '");
            buf.append(EXECUTE_CHAR);
            buf.append("' | '");
            buf.append(CANCEL_CHAR);
            buf.append("' ) <cr>");
            buf.append('\n');
            appendIndent(buf, 2);
            buf.append("Execute or cancel mdx query.");
            buf.append('\n');
            appendIndent(buf, 2);
            buf.append("An mdx query may span one or more lines.");
            buf.append('\n');
            appendIndent(buf, 2);
            buf.append("After the last line of the query has been entered,");
            buf.append('\n');
            appendIndent(buf, 3);
            buf.append("on the next line a single execute character, '");
            buf.append(EXECUTE_CHAR);
            buf.append("', may be entered");
            buf.append('\n');
            appendIndent(buf, 3);
            buf.append("followed by a carriage return.");
            buf.append('\n');
            appendIndent(buf, 3);
            buf.append("The lone '");
            buf.append(EXECUTE_CHAR);
            buf.append("' informs the interpreter that the query has");
            buf.append('\n');
            appendIndent(buf, 3);
            buf.append("has been entered and is ready to execute.");
            buf.append('\n');
            appendIndent(buf, 2);
            buf.append("At anytime during the entry of a query the cancel");
            buf.append('\n');
            appendIndent(buf, 3);
            buf.append("character, '");
            buf.append(CANCEL_CHAR);
            buf.append("', may be entered alone on a line.");
            buf.append('\n');
            appendIndent(buf, 3);
            buf.append("This removes all of the query text from the");
            buf.append('\n');
            appendIndent(buf, 3);
            buf.append("the command interpreter.");
            buf.append('\n');
            appendIndent(buf, 2);
            buf.append("Queries can also be ended by using a semicolon (;)");
            buf.append('\n');
            appendIndent(buf, 3);
            buf.append("at the end of a line.");
        }
        if ((cmd & FUNC_CMD) != 0) {
            buf.append('\n');
            appendFunc(buf);
        }

        if ((cmd & ERROR_CMD) != 0) {
            // list
            buf.append('\n');
            appendError(buf);
        }

        if ((cmd & EXIT_CMD) != 0) {
            // exit
            buf.append('\n');
            appendExit(buf);
        }


        return buf.toString();
    }

    protected static void appendIndent(StringBuffer buf, int i) {
        while (i-- > 0) {
            buf.append(CmdRunner.INDENT);
        }
    }

    protected static void appendSet(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("set [ property[=value ] ] <cr>");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With no args, prints all mondrian properties and values.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"property\" prints property's value.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"all\" prints all properties' values.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"property=value\" set property to that value.");
    }

    protected String executeSet(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s");

        if (tokens.length == 1) {
            // list all properties
            listPropertyNames(buf);

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            if (arg.equals("all")) {
                listPropertiesAll(buf);
            } else {
                int index = arg.indexOf('=');
                if (index == -1) {
                    listProperty(arg, buf);
                } else {
                    String[] nv = arg.split("=");
                    String name = nv[0];
                    String value = nv[1];
                    if (isProperty(name)) {
                        if (setProperty(name, value)) {
                            this.connectString = null;
                        }
                    } else {
                        buf.append("Bad property name:");
                        buf.append(name);
                        buf.append('\n');
                    }
                }
            }

        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append("\"\n");
            appendSet(buf);
        }

        return buf.toString();
    }

    protected static void appendLog(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("log [ value ] <cr>");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With no args, prints the current log level.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"value\" sets the Log to new value.");
        buf.append('\n');
    }

    protected String executeLog(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s");

        if (tokens.length == 1) {
            String levelStr = Log.lookupLevelName(Log.getLevel());
            buf.append(levelStr);
            buf.append('\n');

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            int level = Log.lookupLevel(arg);
            if (level == Log.BAD_LEVEL) {
                buf.append("Bad Log level name:");
                buf.append(arg);
                buf.append('\n');
            } else {
                Log.setLevel(level);
            }

        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append("\"\n");
            appendSet(buf);
        }

        return buf.toString();
    }

    protected static void appendFile(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("file [ filename | '=' ] <cr>");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With no args, prints the last filename executed.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"filename\", read and execute filename .");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"=\" character, re-read and re-execute previous filename .");
    }

    protected String executeFile(String mdxCmd) {
        StringBuffer buf = new StringBuffer(512);
        String[] tokens = mdxCmd.split("\\s");

        if (tokens.length == 1) {
            if (this.filename != null) {
                buf.append(this.filename);
            }

        } else if (tokens.length == 2) {
            String token = tokens[1];
            String nameOfFile = null;
            if ((token.length() == 1) && (token.charAt(0) == EXECUTE_CHAR)) {
                // file '='
                if (this.filename == null) {
                    buf.append("Bad command usage: \"");
                    buf.append(mdxCmd);
                    buf.append("\", no file to re-execute");
                    buf.append('\n');
                    appendFile(buf);
                } else {
                    nameOfFile = this.filename;
                }
            } else {
                // file filename
                nameOfFile = token;
            }

            if (nameOfFile != null) {
                this.filename = nameOfFile;

                try {
                    commandLoop(new File(this.filename));
                } catch (IOException ex) {
                    setError(ex);
                    buf.append("Error: " +ex);
                }
            }

        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append("\"\n");
            appendFile(buf);
        }
        return buf.toString();
    }

    protected static void appendList(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("list [ cmd | result ] <cr>");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With no arguments, list previous cmd and result");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"cmd\" argument, list the last mdx query cmd.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"result\" argument, list the last mdx query result.");
    }

    protected String executeList(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s");

        if (tokens.length == 1) {
            if (this.mdxCmd != null) {
                buf.append(this.mdxCmd);
                if (mdxResult != null) {
                    buf.append('\n');
                    buf.append(mdxResult);
                }
            } else if (mdxResult != null) {
                buf.append(mdxResult);
            }

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            if (arg.equals("cmd")) {
                if (this.mdxCmd != null) {
                    buf.append(this.mdxCmd);
                }
            } else if (arg.equals("result")) {
                if (mdxResult != null) {
                    buf.append(mdxResult);
                }
            } else {
                buf.append("Bad sub command usage:");
                buf.append(mdxCmd);
                buf.append('\n');
                appendList(buf);
            }
        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append("\"\n");
            appendList(buf);
        }

        return buf.toString();
    }
    protected static void appendFunc(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("func [ name ] <cr>");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With no arguments, list all defined function names");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"name\" argument, diplay the functions:");
        buf.append('\n');
        appendIndent(buf, 3);
        buf.append("name, description, and syntax");
    }
    protected String executeFunc(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s");

        if (tokens.length == 1) {
            // prints names only once
            List funInfoList = FunTable.instance().getFunInfoList();
            Iterator it = funInfoList.iterator();
            String prevName = null;
            while (it.hasNext()) {
                FunInfo fi = (FunInfo) it.next();
                String name = fi.getName();
                if (prevName == null || ! prevName.equals(name)) {
                    buf.append(name);
                    buf.append('\n');
                    prevName = name;
                }
            }

        } else if (tokens.length == 2) {
            String funcname = tokens[1];
            List funInfoList = FunTable.instance().getFunInfoList();
            Category cat = Category.instance();
            List matches = new ArrayList();

            Iterator it = funInfoList.iterator();
            while (it.hasNext()) {
                FunInfo fi = (FunInfo) it.next();
                if (fi.getName().equalsIgnoreCase(funcname)) {
                    matches.add(fi);
                }
            }

            if (matches.size() == 0) {
                buf.append("Bad function name \"");
                buf.append(funcname);
                buf.append("\", usage:");
                buf.append('\n');
                appendList(buf);
            } else {
                it = matches.iterator();
                boolean doname = true;
                while (it.hasNext()) {
                    FunInfo fi = (FunInfo) it.next();
                    if (doname) {
                        buf.append(fi.getName());
                        buf.append('\n');
                        doname = false;
                    }

                    appendIndent(buf, 1);
                    buf.append(fi.getDescription());
                    buf.append('\n');

                    String[] sigs = fi.getSignatures();
                    if (sigs == null) {
                        appendIndent(buf, 2);
                        buf.append("Signature: ");
                        buf.append("NONE");
                        buf.append('\n');
                    } else {
                        for (int i = 0; i < sigs.length; i++) {
                            appendIndent(buf, 2);
                            buf.append(sigs[i]);
                            buf.append('\n');
                        }
                    }
/*
                    appendIndent(buf, 1);
                    buf.append("Return Type: ");
                    int returnType = fi.getReturnTypes();
                    if (returnType >= 0) {
                        buf.append(cat.getName(returnType));
                    } else {
                        buf.append("NONE");
                    }
                    buf.append('\n');
                    int[][] paramsArray = fi.getParameterTypes();
                    if (paramsArray == null) {
                        appendIndent(buf, 1);
                        buf.append("Paramter Types: ");
                        buf.append("NONE");
                        buf.append('\n');

                    } else {

                        for (int j = 0; j < paramsArray.length; j++) {
                            int[] params = paramsArray[j];
                            appendIndent(buf, 1);
                            buf.append("Paramter Types: ");
                            for (int k = 0; k < params.length; k++) {
                                int param = params[k];
                                buf.append(cat.getName(param));
                                buf.append(' ');
                            }
                            buf.append('\n');
                        }
                    }
*/
                }
            }
        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append("\"\n");
            appendList(buf);
        }

        return buf.toString();
    }

    protected static void appendError(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("error [ msg | stack ] <cr>");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With no arguemnts, both message and stack are printed.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"msg\" argument, the Error message is printed.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"stack\" argument, the Error stack trace is printed.");
    }

    protected String executeError(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s");

        if (tokens.length == 1) {
            if (error != null) {
                buf.append(error);
                if (stack != null) {
                    buf.append('\n');
                    buf.append(stack);
                }
            } else if (stack != null) {
                buf.append(stack);
            }

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            if (arg.equals("msg")) {
                if (error != null) {
                    buf.append(error);
                }
            } else if (arg.equals("stack")) {
                if (stack != null) {
                    buf.append(stack);
                }
            } else {
                buf.append("Bad sub command usage:");
                buf.append(mdxCmd);
                buf.append('\n');
                appendList(buf);
            }
        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append("\"\n");
            appendList(buf);
        }

        return buf.toString();
    }
    protected static void appendExit(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("exit <cr>");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("Exit mdx command interpreter.");
    }


    protected String executeMDXCmd(String mdxCmd) {

        this.mdxCmd = mdxCmd;
        try {

            String resultString = execute(mdxCmd);
            mdxResult = resultString;
            clearError();
            return resultString;

        } catch (Exception ex) {
//System.out.println("GOT ERROR=" +ex);
            setError(ex);
            return error;
        }
    }

	/////////////////////////////////////////////////////////////////////////
    // context
    /////////////////////////////////////////////////////////////////////////
/** Currently there is only one global Context
    private static class Context {
        CmdRunner cmdRunner;
    }

    private static final Context context = new Context();
*/

    /////////////////////////////////////////////////////////////////////////
    // helpers
    /////////////////////////////////////////////////////////////////////////
    protected static void loadPropertiesFromFile(String propFile)
                throws IOException {

        Properties props = System.getProperties();
        props.load(new FileInputStream(propFile));
    }

    /////////////////////////////////////////////////////////////////////////
    // main
    /////////////////////////////////////////////////////////////////////////
    protected static void usage(String msg) {
        StringBuffer buf = new StringBuffer(256);
        if (msg != null) {
            buf.append(msg);
            buf.append('\n');
        }
        buf.append("Usage: mondrian.CmdRunner args");
        buf.append('\n');
        buf.append("  args:");
        buf.append('\n');
        buf.append("  -h               : print this usage text");
        buf.append('\n');
        buf.append("  -d               : enable local debugging");
        buf.append('\n');
        buf.append("  -rc              : do NOT reload connections each query");
        buf.append('\n');
        buf.append("                     (default is to reload connections)");
        buf.append("  -p propertyfile  : load mondrian properties");
        buf.append('\n');
        buf.append("  -f filename      : execute mdx in filename");
        buf.append('\n');
        buf.append("  mdx_cmd          : execute mdx_cmd");
        buf.append('\n');

        System.out.println(buf.toString());
        System.exit(0);
    }
    public static void main(String[] args) throws IOException {
        List filenames = null;
        String mdxCmd = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals("-h")) {
                usage(null);

            } else if (arg.equals("-d")) {
                CmdRunner.debug = true;

            } else if (arg.equals("-rc")) {
                CmdRunner.RELOAD_CONNECTION = false;

            } else if (arg.equals("-f")) {
                i++;
                if (i == args.length) {
                    usage("no filename given");
                }
                if (filenames == null) {
                    filenames = new ArrayList();
                }
                filenames.add(args[i]);

            } else if (arg.equals("-p")) {
                i++;
                if (i == args.length) {
                    usage("no mondrian properties file given");
                }
                String propFile = args[i];
                loadPropertiesFromFile(propFile);

            } else {
                mdxCmd = arg;
            }
        }

        CmdRunner cmdRunner = new CmdRunner();
        if (filenames != null) {
            Iterator it = filenames.iterator();
            while (it.hasNext()) {
                String filename = (String) it.next();
                cmdRunner.filename = filename;
                cmdRunner.commandLoop(new File(filename));
                if (cmdRunner.error != null) {
                    System.err.println(cmdRunner.error);
                    if (cmdRunner.stack != null) {
                        System.err.println(cmdRunner.stack);
                    }
                }
            }
        } else if (mdxCmd != null) {
            cmdRunner.commandLoop(mdxCmd, false);
        } else {
            cmdRunner.commandLoop(true);
        }
    }
}

// End CmdRunner.java
