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

    private static final String[] propertyNames = {
        MondrianProperties.QueryLimit,
        MondrianProperties.TraceLevel,
        MondrianProperties.DebugOutFile,
        MondrianProperties.JdbcDrivers,
        MondrianProperties.ResultLimit,
        MondrianProperties.TestConnectString,
        MondrianProperties.JdbcURL,
        "mondrian.catalogURL",
        MondrianProperties.LargeDimensionThreshold,
        MondrianProperties.SparseSegmentCountThreshold,
        MondrianProperties.SparseSegmentDensityThreshold,
    };

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
        if (propertyName.equals("mondrian.catalogURL")) {
            // XXX TODO is this ok in System
            return System.getProperty("mondrian.catalogURL");
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

    private String connectString;
    private Connection connection;

    public CmdRunner() {
        this.connectString = makeConnectString();
//System.out.println("CmdRunner<init>: connectString="+connectString);
    }

    public String execute(String queryString) {
        Result result = runQuery(queryString);
        String resultString = toString(result);
        return resultString;
    }

    public Result runQuery(String queryString) {
        Connection connection = getConnection();
        Query query = connection.parseQuery(queryString);
        return connection.execute(query);
    }


    public String toString(Result result) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        result.print(pw);
        pw.flush();
        return sw.toString();
    }


    public String makeConnectString() {
        String connectString = CmdRunner.getConnectStringProperty();
        // TODO replace all of these 'println' calls with Logger
System.out.println("CmdRunner.makeConnectString: connectString="+connectString);

        Util.PropertyList connectProperties = null;
        if (connectString == null || connectString.equals("")) {
            connectProperties = new Util.PropertyList();
            connectProperties.put("Provider","mondrian");
        } else {
            connectProperties = Util.parseConnectString(connectString);
        }

        // override jdbc url
        String jdbcURL = CmdRunner.getJdbcURLProperty();
System.out.println("CmdRunner.makeConnectString: jdbcURL="+jdbcURL);
        if (jdbcURL != null) {
            connectProperties.put("Jdbc", jdbcURL);
        }
        // override jdbc drivers
        String jdbcDrivers = CmdRunner.getJdbcDriversProperty();
System.out.println("CmdRunner.makeConnectString: jdbcDrivers="+jdbcDrivers);
        if (jdbcURL != null) {
            connectProperties.put("JdbcDrivers", jdbcDrivers);
        }

        // override catalog url
        String catalogURL = CmdRunner.getCatalogURLProperty();
System.out.println("CmdRunner.makeConnectString: catalogURL="+catalogURL);
        if (catalogURL != null) {
            connectProperties.put("catalog", catalogURL);
        }

System.out.println("CmdRunner.makeConnectString: connectProperties="+connectProperties);
        return connectProperties.toString();

    }

    public Connection getConnection() {
        return getConnection(false);
    }

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
        //return System.getProperty(CmdRunner.CONNECT_STRING);
        return MondrianProperties.instance().getTestConnectString();
    }
    protected static String getJdbcURLProperty() {
        //return System.getProperty(CmdRunner.JDBC_URL);
        return MondrianProperties.instance().getFoodmartJdbcURL();
    }
    protected static String getCatalogURLProperty() {
        return System.getProperty("mondrian.catalogURL");
    }
    protected static String getJdbcDriversProperty() {
        //return System.getProperty(CmdRunner.JDBC_DRIVERS);
        return MondrianProperties.instance().getJdbcDrivers();
    }

    protected static String readFully(String filename) throws IOException {
        return readFully(new File(filename));
    }

    /////////////////////////////////////////////////////////////////////////
    // reading file
    /////////////////////////////////////////////////////////////////////////

    protected static String readFully(File file) throws IOException {
        return readFully(new FileReader(file));
    }

    protected static String readFully(Reader rdr)
                 throws IOException {


        char[] buffer = new char[2048];
        StringBuffer buf = new StringBuffer(buffer.length);

        int len = rdr.read(buffer);
        while (len != -1) {
            buf.append(buffer, 0, len);
            len = rdr.read(buffer);
        }
        String s = buf.toString();

        return (s.length() == 0) ? null : s;
    }

    /////////////////////////////////////////////////////////////////////////
    // command loop
    /////////////////////////////////////////////////////////////////////////

    protected void commandLoop(boolean interactive)
        throws IOException {

        commandLoop(System.in, interactive);
    }

    protected void commandLoop(String mdxCmd,
                                      boolean interactive)
       throws IOException {

        ByteArrayInputStream is = new ByteArrayInputStream(mdxCmd.getBytes());
        commandLoop(is, interactive);
    }

    protected void commandLoop(InputStream in,
                                      boolean interactive)
       throws IOException {

        StringBuffer buf = new StringBuffer(2048);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        boolean inMDXCmd = false;
        if (interactive) {
            System.out.print("> ");
        }

        for(;;) {
            String line = br.readLine();
            debug("line="+line);

            if (! inMDXCmd) {
                if (line == null) {
                    return;
                }
            }

            if (! inMDXCmd) {
                String cmd = line.trim();

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
                        System.out.print("> ");
                    }
                    continue;
                }
            }

            if (line == null ||
                    (line.length() == 1 && line.charAt(0) == '.')) {

                String mdxCmd = buf.toString();
                debug("mdxCmd="+mdxCmd);

                String resultString = CmdRunner.executeMDXCmd(mdxCmd);
                if (resultString != null) {
                    printResults(resultString);
                }

                inMDXCmd = false;
                buf.setLength(0);
                if (interactive) {
                    System.out.print("> ");
                }

            } else if (line.length() > 0) {
                inMDXCmd = true;
                buf.append(line);
                buf.append('\n');
                if (interactive) {
                    System.out.print("? ");
                }
            }
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // commands
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

    protected String executeHelp(String mdxCmd) {
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
            buf.append("<mdx query>");
            buf.append('\n');
            appendIndent(buf, 2);
            buf.append("Execute mdx query.");
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
        buf.append("set [property[=value]]");
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
            buf.append("Bad command usage:");
            buf.append(mdxCmd);
            buf.append('\n');
            appendSet(buf);
        }

        return buf.toString();
    }

    protected static void appendFile(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("file [filename]");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With no args, prints the last filename executed.");
        buf.append('\n');
        appendIndent(buf, 2);
        buf.append("With \"filename\", read and execute filename .");
    }

    protected String executeFile(String mdxCmd) {

        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s");

        if (tokens.length == 1) {
            buf.append(context.filename);

        } else if (tokens.length == 2) {
            String filename = tokens[1];
            context.filename = filename;

            try {
                String mcmd = readFully(filename);
                String resultString = CmdRunner.executeMDXCmd(mcmd);
                buf.append(resultString);
            } catch (IOException ex) {
                context.setError(ex);
                buf.append("Error: " +ex);
            }


        } else {
            buf.append("Bad command usage:");
            buf.append(mdxCmd);
            buf.append('\n');
            appendFile(buf);
        }
        return buf.toString();
    }

    protected static void appendList(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("list [cmd | result]");
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
            buf.append(context.mdxCmd);
            buf.append('\n');
            buf.append(context.mdxResult);

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            if (arg.equals("cmd")) {
                buf.append(context.mdxCmd);
            } else if (arg.equals("result")) {
                buf.append(context.mdxResult);
            } else {
                buf.append("Bad sub command usage:");
                buf.append(mdxCmd);
                buf.append('\n');
                appendList(buf);
            }
        } else {
            buf.append("Bad command usage:");
            buf.append(mdxCmd);
            buf.append('\n');
            appendList(buf);
        }

        return buf.toString();
    }
    protected static void appendError(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("error [msg | stack]");
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
            buf.append(context.error);
            buf.append('\n');
            buf.append(context.stack);

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            if (arg.equals("msg")) {
                buf.append(context.error);
            } else if (arg.equals("stack")) {
                buf.append(context.stack);
            } else {
                buf.append("Bad sub command usage:");
                buf.append(mdxCmd);
                buf.append('\n');
                appendList(buf);
            }
        } else {
            buf.append("Bad command usage:");
            buf.append(mdxCmd);
            buf.append('\n');
            appendList(buf);
        }

        return buf.toString();
    }
    protected static void appendExit(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("exit");
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

    protected static String executeMDXCmd(String mdxCmd) {

        context.mdxCmd = mdxCmd;
        try {

            String resultString = context.cmdRunner.execute(mdxCmd);
            context.mdxResult = resultString;
            context.clearError();
            return resultString;

        } catch (MondrianException mex) {
            context.setError(mex);
            return mex.getMessage();
        }
    }


    protected void printResults(String resultString) {
        System.out.println(resultString);
    }

    /////////////////////////////////////////////////////////////////////////
    // context
    /////////////////////////////////////////////////////////////////////////
    private static class Context {
        CmdRunner cmdRunner;
        String filename;
        String mdxCmd;
        String mdxResult;
        String error;
        String stack;
        Context() {
            this.cmdRunner = new CmdRunner();
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
    }

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
        buf.append("  mdx_cmd              : execute mdx_cmd");
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

        if (filename != null) {
            context.filename = filename;

            mdxCmd = readFully(filename);
        }
        CmdRunner cmdRunner = new CmdRunner();
        if (mdxCmd != null) {
            cmdRunner.commandLoop(mdxCmd, false);
        } else {
            cmdRunner.commandLoop(true);
        }
    }
}

// End CmdRunner.java
