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

import java.io.*;
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
    private String filename;
    private String mdxCmd;
    private String mdxResult;
    private String error;
    private String stack;
    private String connectString;
    private Connection connection;

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
        CATALOG_URL,
        MondrianProperties.LargeDimensionThreshold,
        MondrianProperties.SparseSegmentCountThreshold,
        MondrianProperties.SparseSegmentDensityThreshold,
    };

    /**
     * Creates a <code>CmdRunner</code>.
     */
    public CmdRunner() {
        this.connectString = makeConnectString();
        this.filename = null;
        this.mdxCmd = null;
        this.mdxResult = null;
        this.error = null;
    }

    void setError(Throwable t) {
        this.error = t.toString();
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

    public static void listPropertyNames(StringBuffer buf) {
        for (int i = 0; i < propertyNames.length; i++) {
            String propertyName = propertyNames[i];
            buf.append(propertyName).append('\n');
        }
    }

    public static void listPropertiesAll(StringBuffer buf) {
        for (int i = 0; i < propertyNames.length; i++) {
            String propertyName = propertyNames[i];
            String propertyValue = getPropertyValue(propertyName);
            buf.append(propertyName)
                    .append('=')
                    .append(propertyValue)
                    .append('\n');
        }
    }

    private static String getPropertyValue(String propertyName) {
        if (propertyName.equals(CATALOG_URL)) {
            // XXX TODO is this ok in System
            return System.getProperty(CATALOG_URL);
        } else {
            return MondrianProperties.instance().getProperty(propertyName);
        }
    }

    public static void listProperty(String propertyName, StringBuffer buf) {
        buf.append(getPropertyValue(propertyName));
    }

    public static boolean isProperty(String propertyName) {
        for (int i = 0; i < propertyNames.length; i++) {
            if (propertyNames[i].equals(propertyName)) {
                return true;
            }
        }
        return false;
    }

    public static void setProperty(String name, String value) {
        MondrianProperties.instance().setProperty(name, value);
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


    public String makeConnectString() {
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

        return connectProperties.toString();

    }

    /**
     * Gets a connection to Mondrian.
     *
     * @return Mondrian {@link Connection}
     */
    public Connection getConnection() {
        return getConnection(false);
    }

    /**
     * Gets a Mondrian connection, creating a new one if fresh is true.
     *
     * @param fresh
     * @return mondrian Connection.
     */
    public synchronized Connection getConnection(boolean fresh) {
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
        commandLoop(new FileInputStream(file), false);
    }

    protected void commandLoop(
            String mdxCmd, boolean interactive) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(mdxCmd.getBytes());
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
    protected void commandLoop(
        InputStream in,
        boolean interactive) throws IOException {

        StringBuffer buf = new StringBuffer(2048);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        boolean inMDXCmd = false;
        if (interactive) {
            System.out.print(COMMAND_PROMPT_START);
        }
        for(;;) {
            String line = br.readLine();
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
            line = stripComment(line);

            // If not reading an mdx query, then check if the line is a
            // user command.
            if (! inMDXCmd) {
                String cmd = line;

                String resultString = null;
                if (cmd.startsWith("help")) {
                    resultString = executeHelp(cmd);
                } else if (cmd.startsWith("set")) {
                    resultString = executeSet(cmd);
                } else if (cmd.startsWith("file")) {
                    resultString = executeFile(cmd);
                } else if (cmd.startsWith("list")) {
                    resultString = executeList(cmd);
                } else if (cmd.startsWith("error")) {
                    resultString = executeError(cmd);
                } else if (cmd.startsWith("exit")) {
                    resultString = executeExit(cmd);
                }
                if (resultString != null) {
                    printResults(resultString);
                    inMDXCmd = false;
                    buf.setLength(0);
                    if (interactive) {
                        System.out.print(COMMAND_PROMPT_START);
                    }
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

                    String resultString = executeMDXCmd(mdxCmd);
                    if (resultString != null) {
                        printResults(resultString);
                    }
                }

                inMDXCmd = false;
                buf.setLength(0);
                if (interactive) {
                    System.out.print(COMMAND_PROMPT_START);
                }

            } else if (line.length() > 0) {
                // OK, just add the line to the mdx query we are building.
                inMDXCmd = true;
                buf.append(line);
                // add carriage return so that query keeps formatting
                buf.append('\n');
                if (interactive) {
                    System.out.print(COMMAND_PROMPT_MID);
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
     * Strips a line from the {@link #COMMENT_CHAR comment character)
     * (if present) to the end of line.
     *
     * <p>If the comment character is inside a string, this method
     * incorrectly truncates the rest of the line.
     *
     * @param line Line
     * @return Line truncated at comment character.
     */
    protected static String stripComment(String line) {
        if (line != null) {
            int index = line.indexOf(COMMENT_CHAR);
            if (index != -1) {
                line = line.substring(0,index);
            }
        }
        return line;
    }

    /////////////////////////////////////////////////////////////////////////
    // user commands and help messages
    /////////////////////////////////////////////////////////////////////////
    private static final String INDENT = "  ";

    private static final int UNKNOWN_CMD        = 0x00;
    private static final int HELP_CMD           = 0x01;
    private static final int SET_CMD            = 0x02;
    private static final int FILE_CMD           = 0x04;
    private static final int LIST_CMD           = 0x08;
    private static final int MDX_CMD            = 0x10;
    private static final int ERROR_CMD          = 0x20;
    private static final int EXIT_CMD           = 0x40;
    private static final int ALL_CMD  =
        HELP_CMD | SET_CMD | FILE_CMD | LIST_CMD | MDX_CMD | ERROR_CMD | EXIT_CMD;

    private static final char EXECUTE_CHAR        = '=';
    private static final char CANCEL_CHAR         = '~';
    private static final char COMMENT_CHAR        = '#';

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
            buf.append(INDENT);
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
                        setProperty(name, value);
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
            if (filename != null) {
                buf.append(filename);
            }

        } else if (tokens.length == 2) {
            String token = tokens[1];
            String filename = null;
            if ((token.length() == 1) && (token.charAt(0) == EXECUTE_CHAR)) {
                // file '='
                if (filename == null) {
                    buf.append("Bad command usage: \"");
                    buf.append(mdxCmd);
                    buf.append("\", no file to re-execute");
                    buf.append('\n');
                    appendFile(buf);
                } else {
                    filename = this.filename;
                }
            } else {
                // file filename
                filename = token;
            }

            if (filename != null) {
                this.filename = filename;

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

    protected String executeList(String _mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = _mdxCmd.split("\\s");

        if (tokens.length == 1) {
            if (mdxCmd != null) {
                buf.append(mdxCmd);
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
                if (mdxCmd != null) {
                    buf.append(mdxCmd);
                }
            } else if (arg.equals("result")) {
                if (mdxResult != null) {
                    buf.append(mdxResult);
                }
            } else {
                buf.append("Bad sub command usage:");
                buf.append(_mdxCmd);
                buf.append('\n');
                appendList(buf);
            }
        } else {
            buf.append("Bad command usage: \"");
            buf.append(_mdxCmd);
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

    protected String executeExit(String mdxCmd) {
        // TODO: Don't call System.exit(). It makes the CmdRunner unusable
        // inside a container such as an app server or junit.
        System.exit(0);
        return null;
    }

    protected String executeMDXCmd(String mdxCmd) {

        this.mdxCmd = mdxCmd;
        try {

            String resultString = context.cmdRunner.execute(mdxCmd);
            mdxResult = resultString;
            clearError();
            return resultString;

        } catch (MondrianException mex) {
            setError(mex);
            return mex.getMessage();
        }
    }


    /////////////////////////////////////////////////////////////////////////
    // context
    /////////////////////////////////////////////////////////////////////////
    private static class Context {
        CmdRunner cmdRunner;
    }

    /** Currently there is only one global Context */
    private static final Context context = new Context();

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
        String filename = null;
        String mdxCmd = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals("-h")) {
                    usage(null);
            } else if (arg.equals("-d")) {
                CmdRunner.debug = true;
            } else if (arg.equals("-f")) {
                i++;
                if (i == args.length) {
                    usage("no filename given");
                }
                filename = args[i];
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
        if (filename != null) {
            cmdRunner.filename = filename;
            cmdRunner.commandLoop(new File(filename));
        } else if (mdxCmd != null) {
            cmdRunner.commandLoop(mdxCmd, false);
        } else {
            cmdRunner.commandLoop(true);
        }
    }
}

// End CmdRunner.java
