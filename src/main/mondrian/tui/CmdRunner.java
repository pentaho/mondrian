/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.tui;

import mondrian.olap.*;
import mondrian.olap.type.TypeUtil;
import mondrian.olap.fun.FunInfo;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.rolap.RolapCube;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.eigenbase.util.property.Property;

import java.io.*;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Command line utility which reads and executes MDX commands.
 *
 * <p>TODO: describe how to use this class.</p>
 *
 * @author Richard Emberson
 * @version $Id$
 */
public class CmdRunner {

    private static final String nl = Util.nl;

    private static boolean RELOAD_CONNECTION = true;
    private static String CATALOG_NAME = "FoodMart";

    private static final Map paraNameValues = new HashMap();

    private static String[][] commentDelim;
    private static boolean allowNestedComments;

    private final Options options;
    private long queryTime;
    private long totalQueryTime;
    private String filename;
    private String mdxCmd;
    private String mdxResult;
    private String error;
    private String stack;
    private String connectString;
    private Connection connection;
    private final PrintWriter out;

    static {
        setDefaultCommentState();
    }

    /**
     * Creates a <code>CmdRunner</code>.
     *
     * @param options Option set, or null to use default options
     * @param out Output writer, or null to use {@link System#out}.
     */
    public CmdRunner(Options options, PrintWriter out) {
        if (options == null) {
            options = new Options();
        }
        this.options = options;
        this.filename = null;
        this.mdxResult = null;
        this.error = null;
        this.queryTime = -1;
        if (out == null) {
            out = new PrintWriter(System.out);
        }
        this.out = out;
    }

    public void setTimeQueries(boolean timeQueries) {
        this.options.timeQueries = timeQueries;
    }

    public boolean getTimeQueries() {
        return options.timeQueries;
    }

    public long getQueryTime() {
        return queryTime;
    }

    public long getTotalQueryTime() {
        return totalQueryTime;
    }

    public void noCubeCaching() {
        Cube[] cubes = getCubes();
        for (int i = 0; i < cubes.length; i++) {
            Cube cube = cubes[i];
            RolapCube rcube = (RolapCube) cube;
            rcube.setCacheAggregations(false);
        }
    }

    void setError(String s) {
        this.error = s;
    }

    void setError(Throwable t) {
        this.error = formatError(t);
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
            if (message == null) {
                message = mex.toString();
            }
            if (mex.getCause() != null && mex.getCause() != mex) {
                    message = message + nl + formatError(mex.getCause());
            }
            return message;
    }

    public static void listPropertyNames(StringBuffer buf) {
        PropertyInfo propertyInfo =
                new PropertyInfo(MondrianProperties.instance());
        for (int i = 0; i < propertyInfo.size(); i++) {
            buf.append(propertyInfo.getProperty(i).getPath());
            buf.append(nl);
        }
    }

    public static void listPropertiesAll(StringBuffer buf) {
        PropertyInfo propertyInfo =
                new PropertyInfo(MondrianProperties.instance());
        for (int i = 0; i < propertyInfo.size(); i++) {
            String propertyName = propertyInfo.getPropertyName(i);
            String propertyValue = propertyInfo.getProperty(i).getString();
            buf.append(propertyName);
            buf.append('=');
            buf.append(propertyValue);
            buf.append(nl);
        }
    }

    /**
     * Returns the value of a property, or null if it is not set.
     */
    private static String getPropertyValue(String propertyName) {
        final Property property = PropertyInfo.lookupProperty(
                MondrianProperties.instance(),
                propertyName);
        return property.isSet() ?
                property.getString() :
                null;
    }

    public static void listProperty(String propertyName, StringBuffer buf) {
        buf.append(getPropertyValue(propertyName));
    }

    public static boolean isProperty(String propertyName) {
        final Property property = PropertyInfo.lookupProperty(
                MondrianProperties.instance(),
                propertyName);
        return property != null;
    }

    public static boolean setProperty(String name, String value) {
        final Property property = PropertyInfo.lookupProperty(
                MondrianProperties.instance(),
                name);
        String oldValue = property.getString();
        if (! Util.equals(oldValue, value)) {
            property.setString(value);
            return true;
        } else {
            return false;
        }
    }

    public void loadParameters(Query query) {
        Parameter[] params = query.getParameters();
        for (int i = 0; i < params.length; i++) {
            Parameter param = params[i];
            loadParameter(query, param);
        }
    }

    /**
     * Looks up the definition of a property with a given name.
     */
    private static class PropertyInfo {
        private final List propertyList = new ArrayList();
        private final List propertyNameList = new ArrayList();

        PropertyInfo(MondrianProperties properties) {
            final Class clazz = properties.getClass();
            final Field[] fields = clazz.getFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                if (!Modifier.isPublic(field.getModifiers()) ||
                        Modifier.isStatic(field.getModifiers()) ||
                        !Property.class.isAssignableFrom(
                                field.getType())) {
                    continue;
                }
                final Property property;
                try {
                    property = (Property) field.get(properties);
                } catch (IllegalAccessException e) {
                    continue;
                }
                propertyList.add(property);
                propertyNameList.add(field.getName());
            }
        }

        public int size() {
            return propertyList.size();
        }

        public Property getProperty(int i) {
            return (Property) propertyList.get(i);
        }

        public String getPropertyName(int i) {
            return (String) propertyNameList.get(i);
        }

        /**
         * Looks up the definition of a property with a given name.
         */
        public static Property lookupProperty(
                MondrianProperties properties,
                String propertyName)
        {
            final Class clazz = properties.getClass();
            final Field field;
            try {
                field = clazz.getField(propertyName);
            } catch (NoSuchFieldException e) {
                return null;
            }
            if (!Modifier.isPublic(field.getModifiers()) ||
                    Modifier.isStatic(field.getModifiers()) ||
                    !Property.class.isAssignableFrom(field.getType())) {
                return null;
            }
            try {
                return (Property) field.get(properties);
            } catch (IllegalAccessException e) {
                return null;
            }
        }
    }

    private static class Expr {
        static final int STRING_TYPE    = 1;
        static final int NUMERIC_TYPE   = 2;
        static final int MEMBER_TYPE    = 3;
        static String typeName(int type) {
            switch (type) {
            case STRING_TYPE:
                return "STRING_TYPE";
            case NUMERIC_TYPE:
                return "NUMERIC_TYPE";
            case MEMBER_TYPE:
                return "MEMBER_TYPE";
            default:
                return "UNKNOWN_TYPE";
            }
        }
        Object value;
        int type;
        Expr(Object value, int type) {
            this.value = value;
            this.type = type;
        }
    }

    public void loadParameter(Query query, Parameter param) {
        int category = TypeUtil.typeToCategory(param.getType());
        String name = param.getName();
        String value = (String) CmdRunner.paraNameValues.get(name);
        debug("loadParameter: name=" +name+ ", value=" + value);
        if (value == null) {
            return;
        }
        Expr expr = parseParameter(value);
        if  (expr == null) {
            return;
        }
        int type = expr.type;
        // found the parameter with the given name in the query
        switch (category) {
        case Category.Numeric:
            if (type != Expr.NUMERIC_TYPE) {
                String msg = "For parameter named \""
                        + name
                        + "\" of Catetory.Numeric, "
                        + "the value was type \""
                        + Expr.typeName(type)
                        + "\"";
                throw new IllegalArgumentException(msg);
            }
            break;
        case Category.String:
            if (type != Expr.STRING_TYPE) {
                String msg = "For parameter named \""
                        + name
                        + "\" of Catetory.String, "
                        + "the value was type \""
                        + Expr.typeName(type)
                        + "\"";
                throw new IllegalArgumentException(msg);
            }
            break;

        case Category.Member:
            if (type != Expr.MEMBER_TYPE) {
                String msg = "For parameter named \""
                        + name
                        + "\" of Catetory.Member, "
                        + "the value was type \""
                        + Expr.typeName(type)
                        + "\"";
                throw new IllegalArgumentException(msg);
            }
            break;

        default:
            throw Util.newInternal("unexpected category " + category);
        }
        query.setParameter(param.getName(), String.valueOf(expr.value));
    }

    static NumberFormat nf = NumberFormat.getInstance();

    // this is taken from JPivot
    public Expr parseParameter(String value) {
        // is it a String (enclose in double or single quotes ?
        String trimmed = value.trim();
        int len = trimmed.length();
        if (trimmed.charAt(0) == '"' && trimmed.charAt(len - 1) == '"') {
            debug("parseParameter. STRING_TYPE: " +trimmed);
            return new Expr(trimmed.substring(1, trimmed.length() - 1),
                            Expr.STRING_TYPE);
        }
        if (trimmed.charAt(0) == '\'' && trimmed.charAt(len - 1) == '\'') {
            debug("parseParameter. STRING_TYPE: " +trimmed);
            return new Expr(trimmed.substring(1, trimmed.length() - 1),
                            Expr.STRING_TYPE);
        }

        // is it a Number ?
        Number number = null;
        try {
            number = nf.parse(trimmed);
        } catch (ParseException pex) {
            // nothing to do, should be member
        }
        if (number != null) {
            debug("parseParameter. NUMERIC_TYPE: " +number);
            return new Expr(number, Expr.NUMERIC_TYPE);
        }

        debug("parseParameter. MEMBER_TYPE: " +trimmed);
        Query query = this.connection.parseQuery(this.mdxCmd);
        // dont have to execute
        //this.connection.execute(query);

        // assume member, dimension, hierarchy, level
        OlapElement element = Util.lookup(query, Util.explode(trimmed));

        debug("parseParameter. exp="
            +((element == null) ? "null" : element.getClass().getName()));

        if (element instanceof Member) {
            Member member = (Member) element;
            return new Expr(member, Expr.MEMBER_TYPE);
        } else if (element instanceof mondrian.olap.Level) {
            mondrian.olap.Level level = (mondrian.olap.Level) element;
            return new Expr(level, Expr.MEMBER_TYPE);
        } else if (element instanceof Hierarchy) {
            Hierarchy hier = (Hierarchy) element;
            return new Expr(hier, Expr.MEMBER_TYPE);
        } else if (element instanceof Dimension) {
            Dimension dim = (Dimension) element;
            return new Expr(dim, Expr.MEMBER_TYPE);
        }
        return null;
    }

    public static void listParameterNameValues(StringBuffer buf) {
        Iterator it = CmdRunner.paraNameValues.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry e = (Map.Entry) it.next();
            buf.append(e.getKey());
            buf.append('=');
            buf.append(e.getValue());
            buf.append(nl);
        }
    }

    public static void listParam(String name, StringBuffer buf) {
        String v = (String) CmdRunner.paraNameValues.get(name);
        buf.append(v);
    }

    public static boolean isParam(String name) {
        String v = (String) CmdRunner.paraNameValues.get(name);
        return (v != null);
    }

    public static void setParameter(String name, String value) {
        if (name == null) {
            CmdRunner.paraNameValues.clear();
        } else {
            if (value == null) {
                CmdRunner.paraNameValues.remove(name);
            } else {
                CmdRunner.paraNameValues.put(name, value);
            }
        }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // cubes
    //
    public Cube[] getCubes() {
        Connection conn = getConnection();
        Cube[] cubes = conn.getSchemaReader().getCubes();
        return cubes;
    }

    public Cube getCube(String name) {
        Cube[] cubes = getCubes();
        for (int i = 0; i < cubes.length; i++) {
            Cube cube = cubes[i];
            if (cube.getName().equals(name)) {
                return cube;
            }
        }
        return null;
    }

    public void listCubeName(StringBuffer buf) {
        Cube[] cubes = getCubes();
        for (int i = 0; i < cubes.length; i++) {
            Cube cube = cubes[i];
            buf.append(cube.getName());
            buf.append(nl);
        }
    }

    public void listCubeAttribues(String name, StringBuffer buf) {
        Cube cube = getCube(name);
        if (cube == null) {
            buf.append("No cube found with name \"");
            buf.append(name);
            buf.append("\"");
        } else {
            RolapCube rcube = (RolapCube) cube;
            buf.append("facttable=");
            buf.append(rcube.getStar().getFactTable().getAlias());
            buf.append(nl);
            buf.append("caching=");
            buf.append(rcube.isCacheAggregations());
            buf.append(nl);
        }
    }

    public void executeCubeCommand(
            String cubename,
            String command,
            StringBuffer buf) {
        Cube cube = getCube(cubename);
        if (cube == null) {
            buf.append("No cube found with name \"");
            buf.append(cubename);
            buf.append("\"");
        } else {
            if (command.equals("clearCache")) {
                RolapCube rcube = (RolapCube) cube;
                rcube.clearCachedAggregations();
            } else {
                buf.append("For cube \"");
                buf.append(cubename);
                buf.append("\" there is no command \"");
                buf.append(command);
                buf.append("\"");
            }
        }
    }

    public void setCubeAttribute(
            String cubename,
            String name,
            String value,
            StringBuffer buf) {
        Cube cube = getCube(cubename);
        if (cube == null) {
            buf.append("No cube found with name \"");
            buf.append(cubename);
            buf.append("\"");
        } else {
            if (name.equals("caching")) {
                RolapCube rcube = (RolapCube) cube;
                boolean isCache = Boolean.valueOf(value).booleanValue();
                rcube.setCacheAggregations(isCache);
            } else {
                buf.append("For cube \"");
                buf.append(cubename);
                buf.append("\" there is no attribute \"");
                buf.append(name);
                buf.append("\"");
            }
        }
    }
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * Executes a query and returns the result as a string.
     *
     * @param queryString MDX query text
     * @return result String
     */
    public String execute(String queryString) {
        Result result = runQuery(queryString, true);
        String resultString = toString(result);
        return resultString;
    }

    /**
     * Executes a query and returns the result.
     *
     * @param queryString MDX query text
     * @return a {@link Result} object
     */
    public Result runQuery(String queryString, boolean loadParams) {
        debug("CmdRunner.runQuery: TOP");
        Result result = null;
        long start = 0;
        try {
            this.connection = getConnection();
            debug("CmdRunner.runQuery: AFTER getConnection");
            Query query = this.connection.parseQuery(queryString);
            debug("CmdRunner.runQuery: AFTER parseQuery");
            if (loadParams) {
                loadParameters(query);
            }
            start = System.currentTimeMillis();
            result = this.connection.execute(query);
/*
// RME START
int len = query.axes.length;
out.println("CmdRunner.runQuery: len=" +len);
for (int i = 0; i < len; i++) {
    Hierarchy[] hs = query.getMdxHierarchiesOnAxis(i);
    if (hs == null) {
        out.println("CmdRunner.runQuery: got null i=" +i);
    } else {
        for (int j = 0; j < hs.length; j++) {
            Hierarchy h = hs[j];
            out.print("CmdRunner.runQuery: j=" +j);
            if (h == null) {
                out.println(": got null");
            } else {
                out.println(": h=" +h.getName());
            }
        }
    }
}
// RME END
*/
        } finally {
            debug("CmdRunner.runQuery: BOTTOM");
        }
        queryTime = (System.currentTimeMillis() - start);
        totalQueryTime += queryTime;
        return result;
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
        debug("CmdRunner.makeConnectString: connectString="+connectString);

        Util.PropertyList connectProperties = null;
        if (connectString == null || connectString.equals("")) {
            // create new and add provider
            connectProperties = new Util.PropertyList();
            connectProperties.put(RolapConnectionProperties.Provider,"mondrian");
        } else {
            // load with existing connect string
            connectProperties = Util.parseConnectString(connectString);
        }

        // override jdbc url
        String jdbcURL = CmdRunner.getJdbcURLProperty();

        debug("CmdRunner.makeConnectString: jdbcURL="+jdbcURL);

        if (jdbcURL != null) {
            // add jdbc url to connect string
            connectProperties.put(RolapConnectionProperties.Jdbc, jdbcURL);
        }

        // override jdbc drivers
        String jdbcDrivers = CmdRunner.getJdbcDriversProperty();

        debug("CmdRunner.makeConnectString: jdbcDrivers="+jdbcDrivers);
        if (jdbcDrivers != null) {
            // add jdbc drivers to connect string
            connectProperties.put(RolapConnectionProperties.JdbcDrivers, jdbcDrivers);
        }

        // override catalog url
        String catalogURL = CmdRunner.getCatalogURLProperty();

        debug("CmdRunner.makeConnectString: catalogURL="+catalogURL);

        if (catalogURL != null) {
            // add catalog url to connect string
            connectProperties.put(RolapConnectionProperties.Catalog, catalogURL);
        }

        // override JDBC user
        String jdbcUser = CmdRunner.getJdbcUserProperty();

        debug("CmdRunner.makeConnectString: jdbcUser="+jdbcUser);

        if (jdbcUser != null) {
            // add user to connect string
            connectProperties.put(RolapConnectionProperties.JdbcUser, jdbcUser);
        }

        // override JDBC password
        String jdbcPassword = CmdRunner.getJdbcPasswordProperty();

        debug("CmdRunner.makeConnectString: jdbcPassword="+jdbcPassword);

        if (jdbcPassword != null) {
            // add password to connect string
            connectProperties.put(RolapConnectionProperties.JdbcPassword, jdbcPassword);
        }

        debug("CmdRunner.makeConnectString: connectProperties="+connectProperties);

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
        } else if (this.connection == null) {
            this.connection =
                DriverManager.getConnection(this.connectString, null, fresh);
        }
        return this.connection;
    }
    public String getConnectString() {
        return getConnectString(CmdRunner.RELOAD_CONNECTION);
    }
    public synchronized String getConnectString(boolean fresh) {
        if (this.connectString == null) {
            makeConnectString();
        }
        return this.connectString;
    }

    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////
    //
    // static methods
    //
    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////

    protected void debug(String msg) {
        if (options.debug) {
            out.println(msg);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // properties
    /////////////////////////////////////////////////////////////////////////
    protected static String getConnectStringProperty() {
        return MondrianProperties.instance().TestConnectString.get();
    }
    protected static String getJdbcURLProperty() {
        return MondrianProperties.instance().TestJdbcURL.get();
    }

    protected static String getJdbcUserProperty() {
        return MondrianProperties.instance().TestJdbcUser.get();
    }

    protected static String getJdbcPasswordProperty() {
        return MondrianProperties.instance().TestJdbcPassword.get();
    }
    protected static String getCatalogURLProperty() {
        return MondrianProperties.instance().CatalogURL.get();
    }
    protected static String getJdbcDriversProperty() {
        return MondrianProperties.instance().JdbcDrivers.get();
    }

    /////////////////////////////////////////////////////////////////////////
    // command loop
    /////////////////////////////////////////////////////////////////////////

    protected void commandLoop(boolean interactive) throws IOException {
        commandLoop(
                new BufferedReader(
                        new InputStreamReader(System.in)),
                interactive);
    }

    protected void commandLoop(File file) throws IOException {
        // If we open a stream, then we close it.
        FileReader in = new FileReader(file);
        try {
            commandLoop(new BufferedReader(in), false);
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

        StringReader is = new StringReader(mdxCmd);
        commandLoop(is, interactive);
    }

    private static final String COMMAND_PROMPT_START = "> ";
    private static final String COMMAND_PROMPT_MID = "? ";

    /**
     * The Command Loop where lines are read from the InputStream and
     * interpreted. If interactive then prompts are printed.
     *
     * @param in Input reader (preferably buffered)
     * @param interactive Whether the session is interactive
     */
    protected void commandLoop(Reader in, boolean interactive) {

        StringBuffer buf = new StringBuffer(2048);
        boolean inMdxCmd = false;
        String resultString = null;

        for(;;) {
            if (resultString != null) {
                printResults(resultString);
                resultString = null;
            } else if (interactive && (error != null)) {
                printResults(error);
            }
            if (interactive) {
                if (inMdxCmd) {
                    out.print(COMMAND_PROMPT_MID);
                } else {
                    out.print(COMMAND_PROMPT_START);
                }
                out.flush();
            }
            if (!inMdxCmd) {
                buf.setLength(0);
            }
            String line;
            try {
                line = readLine(in, inMdxCmd);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Exception while reading command line", e);
            }
            if (line != null) {
                line = line.trim();
            }
            debug("line="+line);

            if (! inMdxCmd) {
                // If not in the middle of reading an mdx query and
                // we reach end of file on the stream, then we are over.
                if (line == null) {
                    return;
                }
            }

            // If not reading an mdx query, then check if the line is a
            // user command.
            if (! inMdxCmd) {
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
                } else if (cmd.startsWith("param")) {
                    resultString = executeParam(cmd);
                } else if (cmd.startsWith("cube")) {
                    resultString = executeCube(cmd);
                } else if (cmd.startsWith("error")) {
                    resultString = executeError(cmd);
                } else if (cmd.startsWith("echo")) {
                    resultString = executeEcho(cmd);
                } else if (cmd.startsWith("expr")) {
                    resultString = executeExpr(cmd);
                } else if (cmd.equals("=")) {
                    resultString = reExecuteMdxCmd();
                } else if (cmd.startsWith("exit")) {
                    break;
                }
                if (resultString != null) {
                    inMdxCmd = false;
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
                    resultString = executeMdxCmd(mdxCmd);
                }

                inMdxCmd = false;

            } else if (line.length() > 0) {
                // OK, just add the line to the mdx query we are building.
                inMdxCmd = true;
                if (line.endsWith(SEMI_COLON_STRING)) {
                    // Remove the ';' character.
                    buf.append(line.substring(0, line.length() - 1));
                    String mdxCmd = buf.toString().trim();
                    debug("mdxCmd=\"" + mdxCmd + "\"");
                    resultString = executeMdxCmd(mdxCmd);
                    inMdxCmd = false;
                } else {
                    buf.append(line);
                    // add carriage return so that query keeps formatting
                    buf.append(nl);
                }
            }
        }
    }

    protected void printResults(String resultString) {
        if (resultString != null) {
            resultString = resultString.trim();
            if (resultString.length() > 0) {
                out.println(resultString);
                out.flush();
            }
            if (options.timeQueries && (queryTime != -1)) {
                out.println("time[" +queryTime+ "ms]");
                out.flush();
                queryTime = -1;
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
     */
    protected static String readLine(Reader reader, boolean inMdxCmd)
        throws IOException {

        StringBuffer buf = new StringBuffer(128);
        StringBuffer line = new StringBuffer(128);
        int offset;
       int i=getLine(reader, line);

        for (offset = 0; offset < line.length(); offset++) {
            char c = line.charAt(offset);

            if (c == ESCAPE_CHAR) {
                buf.append(ESCAPE_CHAR);
                buf.append(line.charAt(++offset));
            }
            else if ((c == STRING_CHAR_1) || (c == STRING_CHAR_2)) {
                i = readString(reader, line, offset, buf, i);
                offset = 0;
            }
            else {
                int commentType=-1;

                // check if we have the start of a comment block
                for (int x = 0; x < commentDelim.length; x++) {
                    if (line.substring(offset).startsWith(commentDelim[x][0])) {
                        commentType = x;
                        break;
                    }
                }

                // -1 means no comment
                if (commentType == -1) {
                    buf.append(c);
                }
                else {
                    // check for comment to end of line comment
                    if (commentDelim[commentType][1] == null) {
                        if (inMdxCmd) {
                            buf.append(line.substring(offset));
                        }
                        break;
                    }
                    else {
                        // handle delimited comment block
                        i = readBlock(reader, line, offset,
                                commentDelim[commentType][0],
                                commentDelim[commentType][1],
                                false, inMdxCmd, buf, i);
                        offset = 0;
                    }
                }
            }
        }

        if (i == -1 && buf.length() == 0) {
            return null;
        } else {
            return buf.toString();
        }
    }

   /**
     * Read the next line of input.  Return the terminating character,
     * -1 for end of file, or \n or \r.  Add \n and \r to the end of the
     * buffer to be included in strings and comment blocks.
     */
    protected static int getLine(Reader reader, StringBuffer line)
        throws IOException {

        line.setLength(0);
        for (;;) {
            int i = reader.read();

            if (i == -1) {
                return i;
            }

            line.append((char)i);

            if (i == '\n' || i == '\r') {
                return i;
            }
        }
    }

    /**
     * Start of a string, read all of it even if it spans
     * more than one line adding each line's <cr> to the
     * buffer.
     */
    protected static int readString(
            Reader reader,
            StringBuffer line,
            int offset,
            StringBuffer buf,
            int i)
            throws IOException {

        String delim=line.substring(offset, offset + 1);
        return readBlock(reader, line, offset, delim, delim, true, true, buf, i);
    }

    /**
     * Start of a delimted block, read all of it even if it spans
     * more than one line adding each line's <cr> to the
     * buffer.
     *
     * A delimited block is a delimited comment (/\* ... *\/), or a string.
     */
    protected static int readBlock(
            Reader reader,
            StringBuffer line,
            int offset,
            final String startDelim,
            final String endDelim,
            final boolean allowEscape,
            final boolean addToBuf,
            StringBuffer buf,
            int i)
            throws IOException {

        int depth=1;
        if (addToBuf) {
            buf.append(startDelim);
        }
        offset += startDelim.length();

        for (;;) {
            // see if we are at the end of the block
            if (line.substring(offset).startsWith(endDelim)) {
                if (addToBuf) {
                    buf.append(endDelim);
                }
                offset += endDelim.length();
                if (--depth == 0) {
                     break;
                }
            }
            // check for nested block
            else if (allowNestedComments &&
                     line.substring(offset).startsWith(startDelim)) {
                if (addToBuf) {
                    buf.append(startDelim);
                }
                offset += startDelim.length();
                depth++;
            }
            else if (offset < line.length()) {
                // not at the end of line, so eat the next char
                char c = line.charAt(offset++);
                if (allowEscape && c == ESCAPE_CHAR) {
                    if (addToBuf) {
                        buf.append(ESCAPE_CHAR);
                    }

                    if (offset < line.length()) {
                        if (addToBuf) {
                            buf.append(line.charAt(offset));
                        }
                        offset++;
                    }
                }
                else if (addToBuf) {
                    buf.append(c);
                }
            }
            else {
                // finished a line; read in the next one and continue
                if (i == -1) {
                    break;
                }
                i = getLine(reader, line);

                // line will always contain EOL marker at least, unless at EOF
                offset = 0;
               if (line.length() == 0) {
                    break;
                }
            }
        }

        // remove to the end of the string, so caller starts at offset 0
        if (offset > 0) {
            line.delete(0, offset - 1);
        }

        return i;
    }

    /////////////////////////////////////////////////////////////////////////
    // xmla file
    /////////////////////////////////////////////////////////////////////////

    /**
     * This is called to process a file containing XMLA as the contents
     * of SOAP xml.
     *
     * @param file
     * @param validateXmlaResponse
     * @throws Exception
     */
    protected void processSoapXmla(File file, int validateXmlaResponse)
            throws Exception {


        String catalogURL = CmdRunner.getCatalogURLProperty();
        String[][] catalogNameUrls = new String[][] {
                            { CATALOG_NAME, catalogURL }
                        };

        long start = System.currentTimeMillis();

        byte[] bytes = XmlaSupport.processSoapXmla(file,
                        getConnectString(),
                        catalogNameUrls,
                        null);

        queryTime = (System.currentTimeMillis() - start);
        totalQueryTime += queryTime;

        String response = new String(bytes);
        out.println(response);

        switch (validateXmlaResponse) {
        case VALIDATE_NONE:
            break;
        case VALIDATE_TRANSFORM:
            XmlaSupport.validateSchemaSoapXmla(bytes);
            out.println("XML Data is Valid");
            break;
        case VALIDATE_XPATH:
            XmlaSupport.validateSoapXmlaUsingXpath(bytes);
            out.println("XML Data is Valid");
            break;
        }
    }

    /**
     * This is called to process a file containing XMLA xml.
     *
     * @param file
     * @param validateXmlaResponce
     * @throws Exception
     */
    protected void processXmla(File file, int validateXmlaResponce)
            throws Exception {


        String catalogURL = CmdRunner.getCatalogURLProperty();
        String[][] catalogNameUrls = new String[][] {
                            { CATALOG_NAME, catalogURL }
                        };

        long start = System.currentTimeMillis();

        byte[] bytes = XmlaSupport.processXmla(file,
                        getConnectString(),
                        catalogNameUrls);

        queryTime = (System.currentTimeMillis() - start);
        totalQueryTime += queryTime;

        String response = new String(bytes);
        out.println(response);

        switch (validateXmlaResponce) {
        case VALIDATE_NONE:
            break;
        case VALIDATE_TRANSFORM:
            XmlaSupport.validateSchemaXmla(bytes);
            out.println("XML Data is Valid");
            break;
        case VALIDATE_XPATH:
            XmlaSupport.validateXmlaUsingXpath(bytes);
            out.println("XML Data is Valid");
            break;
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // user commands and help messages
    /////////////////////////////////////////////////////////////////////////
    private static final String INDENT = "  ";

    private static final int UNKNOWN_CMD        = 0x0000;
    private static final int HELP_CMD           = 0x0001;
    private static final int SET_CMD            = 0x0002;
    private static final int LOG_CMD            = 0x0004;
    private static final int FILE_CMD           = 0x0008;
    private static final int LIST_CMD           = 0x0010;
    private static final int MDX_CMD            = 0x0020;
    private static final int FUNC_CMD           = 0x0040;
    private static final int PARAM_CMD          = 0x0080;
    private static final int CUBE_CMD           = 0x0100;
    private static final int ERROR_CMD          = 0x0200;
    private static final int ECHO_CMD           = 0x0400;
    private static final int EXPR_CMD           = 0x0800;
    private static final int EXIT_CMD           = 0x1000;

    private static final int ALL_CMD  = HELP_CMD  |
                                        SET_CMD   |
                                        LOG_CMD   |
                                        FILE_CMD  |
                                        LIST_CMD  |
                                        MDX_CMD   |
                                        FUNC_CMD  |
                                        PARAM_CMD |
                                        CUBE_CMD  |
                                        ERROR_CMD |
                                        ECHO_CMD  |
                                        EXPR_CMD  |
                                        EXIT_CMD;

    private static final char ESCAPE_CHAR         = '\\';
    private static final char EXECUTE_CHAR        = '=';
    private static final char CANCEL_CHAR         = '~';
    private static final char STRING_CHAR_1       = '"';
    private static final char STRING_CHAR_2       = '\'';

    private static final String SEMI_COLON_STRING = ";";

    //////////////////////////////////////////////////////////////////////////
    // help
    //////////////////////////////////////////////////////////////////////////
    protected static String executeHelp(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s+");

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
            } else if (cmdName.equals("func")) {
                cmd = FUNC_CMD;
            } else if (cmdName.equals("param")) {
                cmd = PARAM_CMD;
            } else if (cmdName.equals("cube")) {
                cmd = CUBE_CMD;
            } else if (cmdName.equals("error")) {
                cmd = ERROR_CMD;
            } else if (cmdName.equals("echo")) {
                cmd = ECHO_CMD;
            } else if (cmdName.equals("exit")) {
                cmd = EXIT_CMD;
            } else {
                cmd = UNKNOWN_CMD;
            }
        }

        if (cmd == UNKNOWN_CMD) {
            buf.append("Unknown help command: ");
            buf.append(mdxCmd);
            buf.append(nl);
            buf.append("Type \"help\" for list of commands");
        }

        if ((cmd & HELP_CMD) != 0) {
            // help
            buf.append(nl);
            appendIndent(buf, 1);
            buf.append("help");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("Prints this text");
        }

        if ((cmd & SET_CMD) != 0) {
            // set
            buf.append(nl);
            appendSet(buf);
        }

        if ((cmd & LOG_CMD) != 0) {
            // set
            buf.append(nl);
            appendLog(buf);
        }

        if ((cmd & FILE_CMD) != 0) {
            // file
            buf.append(nl);
            appendFile(buf);

        }
        if ((cmd & LIST_CMD) != 0) {
            // list
            buf.append(nl);
            appendList(buf);
        }

        if ((cmd & MDX_CMD) != 0) {
            buf.append(nl);
            appendIndent(buf, 1);
            buf.append("<mdx query> <cr> ( '");
            buf.append(EXECUTE_CHAR);
            buf.append("' | '");
            buf.append(CANCEL_CHAR);
            buf.append("' ) <cr>");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("Execute or cancel mdx query.");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("An mdx query may span one or more lines.");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("After the last line of the query has been entered,");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("on the next line a single execute character, '");
            buf.append(EXECUTE_CHAR);
            buf.append("', may be entered");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("followed by a carriage return.");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("The lone '");
            buf.append(EXECUTE_CHAR);
            buf.append("' informs the interpreter that the query has");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("has been entered and is ready to execute.");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("At anytime during the entry of a query the cancel");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("character, '");
            buf.append(CANCEL_CHAR);
            buf.append("', may be entered alone on a line.");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("This removes all of the query text from the");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("the command interpreter.");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("Queries can also be ended by using a semicolon ';'");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("at the end of a line.");
        }
        if ((cmd & FUNC_CMD) != 0) {
            buf.append(nl);
            appendFunc(buf);
        }

        if ((cmd & PARAM_CMD) != 0) {
            buf.append(nl);
            appendParam(buf);
        }

        if ((cmd & CUBE_CMD) != 0) {
            buf.append(nl);
            appendCube(buf);
        }

        if ((cmd & ERROR_CMD) != 0) {
            buf.append(nl);
            appendError(buf);
        }

        if ((cmd & ECHO_CMD) != 0) {
            buf.append(nl);
            appendEcho(buf);
        }

        if ((cmd & EXPR_CMD) != 0) {
            buf.append(nl);
            appendExpr(buf);
        }

        if (cmd == ALL_CMD) {
            // reexecute
            buf.append(nl);
            appendIndent(buf, 1);
            buf.append("= <cr>");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("Re-Execute mdx query.");
        }

        if ((cmd & EXIT_CMD) != 0) {
            // exit
            buf.append(nl);
            appendExit(buf);
        }


        return buf.toString();
    }

    protected static void appendIndent(StringBuffer buf, int i) {
        while (i-- > 0) {
            buf.append(CmdRunner.INDENT);
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // set
    //////////////////////////////////////////////////////////////////////////
    protected static void appendSet(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("set [ property[=value ] ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no args, prints all mondrian properties and values.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"property\" prints property's value.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"property=value\" set property to that value.");
    }

    protected String executeSet(String mdxCmd) {
        StringBuffer buf = new StringBuffer(400);

        String[] tokens = mdxCmd.split("\\s+");

        if (tokens.length == 1) {
            // list all properties
            listPropertiesAll(buf);

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            int index = arg.indexOf('=');
            if (index == -1) {
                listProperty(arg, buf);
            } else {
                String[] nv = arg.split("=");
                String name = nv[0];
                String value = nv[1];
                if (isProperty(name)) {
                    try {
                        if (setProperty(name, value)) {
                            this.connectString = null;
                        }
                    } catch (Exception ex) {
                        setError(ex);
                    }
                } else {
                    buf.append("Bad property name:");
                    buf.append(name);
                    buf.append(nl);
                }
            }

        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendSet(buf);
        }

        return buf.toString();
    }

    //////////////////////////////////////////////////////////////////////////
    // log
    //////////////////////////////////////////////////////////////////////////
    protected static void appendLog(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("log [ classname[=level ] ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no args, prints the current log level of all classes.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"classname\" prints the current log level of the class.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"classname=level\" set log level to new value.");
    }

    protected String executeLog(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s+");

        if (tokens.length == 1) {
            Enumeration e = LogManager.getCurrentLoggers();
            while (e.hasMoreElements()) {
                Logger logger = (Logger) e.nextElement();
                buf.append(logger.getName());
                buf.append(':');
                buf.append(logger.getLevel());
                buf.append(nl);
            }

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            int index = arg.indexOf('=');
            if (index == -1) {
                Logger logger = LogManager.exists(arg);
                if (logger == null) {
                    buf.append("Bad log name: ");
                    buf.append(arg);
                    buf.append(nl);
                } else {
                    buf.append(logger.getName());
                    buf.append(':');
                    buf.append(logger.getLevel());
                    buf.append(nl);
                }
            } else {
                String[] nv = arg.split("=");
                String classname = nv[0];
                String levelStr = nv[1];

                Logger logger = LogManager.getLogger(classname);

                if (logger == null) {
                    buf.append("Bad log name: ");
                    buf.append(classname);
                    buf.append(nl);
                } else {
                    Level level = Level.toLevel(levelStr, null);
                    if (level == null) {
                        buf.append("Bad log level: ");
                        buf.append(levelStr);
                        buf.append(nl);
                    } else {
                        logger.setLevel(level);
                    }
                }
            }

        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendSet(buf);
        }

        return buf.toString();
    }

    //////////////////////////////////////////////////////////////////////////
    // file
    //////////////////////////////////////////////////////////////////////////
    protected static void appendFile(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("file [ filename | '=' ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no args, prints the last filename executed.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"filename\", read and execute filename .");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"=\" character, re-read and re-execute previous filename .");
    }

    protected String executeFile(String mdxCmd) {
        StringBuffer buf = new StringBuffer(512);
        String[] tokens = mdxCmd.split("\\s+");

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
                    buf.append(nl);
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
            buf.append('"');
            buf.append(nl);
            appendFile(buf);
        }
        return buf.toString();
    }

    //////////////////////////////////////////////////////////////////////////
    // list
    //////////////////////////////////////////////////////////////////////////
    protected static void appendList(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("list [ cmd | result ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no arguments, list previous cmd and result");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"cmd\" argument, list the last mdx query cmd.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"result\" argument, list the last mdx query result.");
    }

    protected String executeList(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s+");

        if (tokens.length == 1) {
            if (this.mdxCmd != null) {
                buf.append(this.mdxCmd);
                if (mdxResult != null) {
                    buf.append(nl);
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
                buf.append(nl);
                appendList(buf);
            }
        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendList(buf);
        }

        return buf.toString();
    }

    //////////////////////////////////////////////////////////////////////////
    // func
    //////////////////////////////////////////////////////////////////////////
    protected static void appendFunc(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("func [ name ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no arguments, list all defined function names");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"name\" argument, display the functions:");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("name, description, and syntax");
    }
    protected String executeFunc(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s+");

        final FunTable funTable = getConnection().getSchema().getFunTable();
        if (tokens.length == 1) {
            // prints names only once
            List funInfoList = funTable.getFunInfoList();
            Iterator it = funInfoList.iterator();
            String prevName = null;
            while (it.hasNext()) {
                FunInfo fi = (FunInfo) it.next();
                String name = fi.getName();
                if (prevName == null || ! prevName.equals(name)) {
                    buf.append(name);
                    buf.append(nl);
                    prevName = name;
                }
            }

        } else if (tokens.length == 2) {
            String funcname = tokens[1];
            List funInfoList = funTable.getFunInfoList();
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
                buf.append(nl);
                appendList(buf);
            } else {
                it = matches.iterator();
                boolean doname = true;
                while (it.hasNext()) {
                    FunInfo fi = (FunInfo) it.next();
                    if (doname) {
                        buf.append(fi.getName());
                        buf.append(nl);
                        doname = false;
                    }

                    appendIndent(buf, 1);
                    buf.append(fi.getDescription());
                    buf.append(nl);

                    String[] sigs = fi.getSignatures();
                    if (sigs == null) {
                        appendIndent(buf, 2);
                        buf.append("Signature: ");
                        buf.append("NONE");
                        buf.append(nl);
                    } else {
                        for (int i = 0; i < sigs.length; i++) {
                            appendIndent(buf, 2);
                            buf.append(sigs[i]);
                            buf.append(nl);
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
                    buf.append(nl);
                    int[][] paramsArray = fi.getParameterTypes();
                    if (paramsArray == null) {
                        appendIndent(buf, 1);
                        buf.append("Paramter Types: ");
                        buf.append("NONE");
                        buf.append(nl);

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
                            buf.append(nl);
                        }
                    }
*/
                }
            }
        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendList(buf);
        }

        return buf.toString();
    }
    //////////////////////////////////////////////////////////////////////////
    // param
    //////////////////////////////////////////////////////////////////////////
    protected static void appendParam(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("param [ name[=value ] ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no argumnts, all param name/value pairs are printed.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"name\" argument, the value of the param is printed.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"name=value\" sets the parameter with name to value.");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append(" If name is null, then unsets all parameters");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append(" If value is null, then unsets the parameter associated with value");
    }
    protected String executeParam(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s+");

        if (tokens.length == 1) {
            // list all properties
            listParameterNameValues(buf);

        } else if (tokens.length == 2) {
            String arg = tokens[1];
            int index = arg.indexOf('=');
            if (index == -1) {
                String name = arg;
                if (isParam(name)) {
                    listParam(name, buf);
                } else {
                    buf.append("Bad parameter name:");
                    buf.append(name);
                    buf.append(nl);
                }
            } else {
                String[] nv = arg.split("=");
                String name = (nv.length == 0) ? null : nv[0];
                String value = (nv.length == 2) ? nv[1] : null;
                setParameter(name, value);
            }

        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendSet(buf);
        }

        return buf.toString();
    }
    //////////////////////////////////////////////////////////////////////////
    // cube
    //////////////////////////////////////////////////////////////////////////
    protected static void appendCube(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("cube [ cubename [ name [=value | command] ] ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no argumnts, all cubes are listed by name.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"cubename\" argument, cube attribute name/values for:");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("fact table (readonly)");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("aggregate caching (readwrite)");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("are printed");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"cubename name=value\" sets the readwrite attribute with name to value.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"cubename command\" executes the commands:");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("clearCache");
    }

    protected String executeCube(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s+");

        if (tokens.length == 1) {
            // list all properties
            listCubeName(buf);
        } else if (tokens.length == 2) {
            String cubename = tokens[1];
            listCubeAttribues(cubename, buf);

        } else if (tokens.length == 3) {
            String cubename = tokens[1];
            String arg = tokens[2];
            int index = arg.indexOf('=');
            if (index == -1) {
                // its a commnd
                String command = arg;
                executeCubeCommand(cubename, command, buf);
            } else {
                String[] nv = arg.split("=");
                String name = (nv.length == 0) ? null : nv[0];
                String value = (nv.length == 2) ? nv[1] : null;
                setCubeAttribute(cubename, name, value, buf);
            }

        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendSet(buf);
        }

        return buf.toString();
    }
    //////////////////////////////////////////////////////////////////////////
    // error
    //////////////////////////////////////////////////////////////////////////
    protected static void appendError(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("error [ msg | stack ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no argumnts, both message and stack are printed.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"msg\" argument, the Error message is printed.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"stack\" argument, the Error stack trace is printed.");
    }

    protected String executeError(String mdxCmd) {
        StringBuffer buf = new StringBuffer(200);

        String[] tokens = mdxCmd.split("\\s+");

        if (tokens.length == 1) {
            if (error != null) {
                buf.append(error);
                if (stack != null) {
                    buf.append(nl);
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
                buf.append(nl);
                appendList(buf);
            }
        } else {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendList(buf);
        }

        return buf.toString();
    }
    //////////////////////////////////////////////////////////////////////////
    // echo
    //////////////////////////////////////////////////////////////////////////
    protected static void appendEcho(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("echo text <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("echo text to standard out.");
    }
    protected String executeEcho(String mdxCmd) {

        try {
            String resultString = (mdxCmd.length() == 4)
                ? "" : mdxCmd.substring(4);
            return resultString;

        } catch (Exception ex) {
            setError(ex);
            //return error;
            return null;
        }
    }
    //////////////////////////////////////////////////////////////////////////
    // expr
    //////////////////////////////////////////////////////////////////////////
    protected static void appendExpr(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("expr cubename expression<cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("evaluate an expression against a cube.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("where: ");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("cubename is single word or string using [], '' or \"\"");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("expression is string using \"\"");
    }
    protected String executeExpr(String mdxCmd) {
        StringBuffer buf = new StringBuffer(256);

        mdxCmd = (mdxCmd.length() == 5)
                ? "" : mdxCmd.substring(5);

        String regex = "(\"[^\"]+\"|'[^\']+'|\\[[^\\]]+\\]|[^\\s]+)\\s+.*";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(mdxCmd);
        boolean b = m.matches();

        if (! b) {
            buf.append("Could not parse into \"cubename expression\" command:");
            buf.append(nl);
            buf.append(mdxCmd);
            String msg = buf.toString();
            setError(msg);
            return msg;
        } else {

            String cubeName = m.group(1);
            String expression = mdxCmd.substring(cubeName.length()+1);

            if (cubeName.charAt(0) == '"') {
                cubeName = cubeName.substring(1, cubeName.length()-1);
            } else if (cubeName.charAt(0) == '\'') {
                cubeName = cubeName.substring(1, cubeName.length()-1);
            } else if (cubeName.charAt(0) == '[') {
                cubeName = cubeName.substring(1, cubeName.length()-1);
            }

            int len = expression.length();
            if (expression.charAt(0) == '"') {
                if (expression.charAt(len-1) != '"') {
                    buf.append("Missing end '\"' in expression:");
                    buf.append(nl);
                    buf.append(expression);
                    String msg = buf.toString();
                    setError(msg);
                    return msg;
                }
                expression = expression.substring(1, len-1);

            } else if (expression.charAt(0) == '\'') {
                if (expression.charAt(len-1) != '\'') {
                    buf.append("Missing end \"'\" in expression:");
                    buf.append(nl);
                    buf.append(expression);
                    String msg = buf.toString();
                    setError(msg);
                    return msg;
                }
                expression = expression.substring(1, len-1);
            }

            Cube cube = getCube(cubeName);
            if (cube == null) {
                buf.append("No cube found with name \"");
                buf.append(cubeName);
                buf.append("\"");
                String msg = buf.toString();
                setError(msg);
                return msg;

            } else {
                try {
                    if (cubeName.indexOf(' ') >= 0) {
                        if (cubeName.charAt(0) != '[') {
                            cubeName = Util.quoteMdxIdentifier(cubeName);
                        }
                    }
                    final char c = '\'';
                    if (expression.indexOf('\'') != -1) {
                        // make sure all "'" are escaped
                        int start = 0;
                        int index = expression.indexOf('\'', start);
                        if (index == 0) {
                            // error: starts with "'"
                            buf.append("Double \"''\" starting expression:");
                            buf.append(nl);
                            buf.append(expression);
                            String msg = buf.toString();
                            setError(msg);
                            return msg;
                        }
                        while (index != -1) {
                            if (expression.charAt(index-1) != '\\') {
                                // error
                                buf.append("Non-escaped \"'\" in expression:");
                                buf.append(nl);
                                buf.append(expression);
                                String msg = buf.toString();
                                setError(msg);
                                return msg;
                            }
                            start = index+1;
                            index = expression.indexOf('\'', start);
                        }
                    }

                    // taken from FoodMartTest code
                    StringBuffer queryStringBuf = new StringBuffer(64);
                    queryStringBuf.append("with member [Measures].[Foo] as ");
                    queryStringBuf.append(c);
                    queryStringBuf.append(expression);
                    queryStringBuf.append(c);
                    queryStringBuf.append(" select {[Measures].[Foo]} on columns from ");
                    queryStringBuf.append(cubeName);

                    String queryString = queryStringBuf.toString();

                    Result result = runQuery(queryString, true);
                    String resultString =
                        result.getCell(new int[]{0}).getFormattedValue();
                    mdxResult = resultString;
                    clearError();

                    buf.append(resultString);

                } catch (Exception ex) {
                    setError(ex);
                    buf.append("Error: " +ex);
                }
            }
        }
        return buf.toString();
    }
    //////////////////////////////////////////////////////////////////////////
    // exit
    //////////////////////////////////////////////////////////////////////////
    protected static void appendExit(StringBuffer buf) {
        appendIndent(buf, 1);
        buf.append("exit <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("Exit mdx command interpreter.");
    }


    protected String reExecuteMdxCmd() {
        if (this.mdxCmd == null) {
            return "No command to execute";
        } else {
            return executeMdxCmd(this.mdxCmd);
        }
    }

    protected String executeMdxCmd(String mdxCmd) {

        this.mdxCmd = mdxCmd;
        try {

            String resultString = execute(mdxCmd);
            mdxResult = resultString;
            clearError();
            return resultString;

        } catch (Exception ex) {
            setError(ex);
            //return error;
            return null;
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // helpers
    /////////////////////////////////////////////////////////////////////////
    protected static void loadPropertiesFromFile(String propFile)
                throws IOException {

        MondrianProperties.instance().load(new FileInputStream(propFile));
    }

    /////////////////////////////////////////////////////////////////////////
    // main
    /////////////////////////////////////////////////////////////////////////

    /**
     * Prints a usage message.
     *
     * @param msg Prefix to the message
     * @param out Output stream
     */
    protected static void usage(String msg, PrintStream out) {
        StringBuffer buf = new StringBuffer(256);
        if (msg != null) {
            buf.append(msg);
            buf.append(nl);
        }
        buf.append("Usage: mondrian.tui.CmdRunner args");
        buf.append(nl);
        buf.append("  args:");
        buf.append(nl);
        buf.append("  -h               : print this usage text");
        buf.append(nl);
        buf.append("  -d               : enable local debugging");
        buf.append(nl);
        buf.append("  -t               : time each mdx query");
        buf.append(nl);
        buf.append("  -nocache         : turn off in-memory aggregate caching");
        buf.append(nl);
        buf.append("                     for all cubes regardless of setting");
        buf.append(nl);
        buf.append("                     in schema");
        buf.append(nl);
        buf.append("  -rc              : do NOT reload connections each query");
        buf.append(nl);
        buf.append("                     (default is to reload connections)");
        buf.append(nl);
        buf.append("  -p propertyfile  : load mondrian properties");
        buf.append(nl);
        buf.append("  -f mdx_filename+ : execute mdx in one or more files");
        buf.append(nl);
        buf.append("  -x xmla_filename+: execute XMLA in one or more files");
        buf.append("                     the XMLA request has no SOAP wrapper");
        buf.append(nl);
        buf.append("  -xs soap_xmla_filename+ ");
        buf.append("                   : execute Soap XMLA in one or more files");
        buf.append("                     the XMLA request has a SOAP wrapper");
        buf.append(nl);
        buf.append("  -vt              : validate xmla response using transforms");
        buf.append("                     only used with -x or -xs flags");
        buf.append(nl);
        buf.append("  -vx              : validate xmla response using xpaths");
        buf.append("                     only used with -x or -xs flags");
        buf.append(nl);
        buf.append("  mdx_cmd          : execute mdx_cmd");
        buf.append(nl);

        out.println(buf.toString());
    }

    /**
     * Set the default comment delimiters for CmdRunner.  These defaults are
     * # to end of line
     * plus all the comment delimiters in Scanner.
     */
    private static void setDefaultCommentState() {
        allowNestedComments = mondrian.olap.Scanner.getNestedCommentsState();
        String[][] scannerCommentsDelimiters = mondrian.olap.Scanner.getCommentDelimiters();
        commentDelim = new String[scannerCommentsDelimiters.length+1][2];

        // CmdRunner has extra delimiter; # to end of line
        commentDelim[0][0] = "#";
        commentDelim[0][1] = null;

        // copy all the rest of the delimiters
        for (int x = 0; x < scannerCommentsDelimiters.length; x++) {
            commentDelim[x + 1][0] = scannerCommentsDelimiters[x][0];
            commentDelim[x + 1][1] = scannerCommentsDelimiters[x][1];
        }
    }

    private static final int DO_MDX             = 1;
    private static final int DO_XMLA            = 2;
    private static final int DO_SOAP_XMLA       = 3;

    private static final int VALIDATE_NONE      = 1;
    private static final int VALIDATE_TRANSFORM = 2;
    private static final int VALIDATE_XPATH     = 3;

    protected static class Options {
        private boolean debug = false;
        private boolean timeQueries;
        private boolean noCache = false;
        private int validateXmlaResponse = VALIDATE_NONE;
        private final List filenames = new ArrayList();
        private int doingWhat = DO_MDX;
        private String singleMdxCmd;
    }

    public static void main(String[] args) throws Exception {

        Options options;
        try {
            options = parseOptions(args);
        } catch (BadOption badOption) {
            usage(badOption.getMessage(), System.out);
            return;
        }

        CmdRunner cmdRunner =
                new CmdRunner(options, new PrintWriter(System.out));
        if (options.noCache) {
            cmdRunner.noCubeCaching();
        }

        if (!options.filenames.isEmpty()) {
            for (int i = 0; i < options.filenames.size(); i++) {
                String filename = (String) options.filenames.get(i);
                cmdRunner.filename = filename;
                switch (options.doingWhat) {
                case DO_MDX:
                    // its a file containing mdx
                    cmdRunner.commandLoop(new File(filename));
                    break;
                case DO_XMLA:
                    // its a file containing XMLA
                    cmdRunner.processXmla(new File(filename),
                            options.validateXmlaResponse);
                    break;
                default:
                    // its a file containing SOAP XMLA
                    cmdRunner.processSoapXmla(new File(filename),
                            options.validateXmlaResponse);
                    break;
                }
                if (cmdRunner.error != null) {
                    System.err.println(filename);
                    System.err.println(cmdRunner.error);
                    if (cmdRunner.stack != null) {
                        System.err.println(cmdRunner.stack);
                    }
                    cmdRunner.clearError();
                }
            }
        } else if (options.singleMdxCmd != null) {
            cmdRunner.commandLoop(options.singleMdxCmd, false);
            if (cmdRunner.error != null) {
                System.err.println(cmdRunner.error);
                if (cmdRunner.stack != null) {
                    System.err.println(cmdRunner.stack);
                }
            }
        } else {
            cmdRunner.commandLoop(true);
        }
        cmdRunner.printTotalQueryTime();
    }

    private void printTotalQueryTime() {
        if (options.timeQueries) {
            // only print if different
            if (totalQueryTime != queryTime) {
                out.println("total[" + totalQueryTime + "ms]");
            }
        }
        out.flush();
    }

    private static Options parseOptions(String[] args)
            throws BadOption, IOException {
        final Options options = new Options();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals("-h")) {
                throw new BadOption(null);

            } else if (arg.equals("-d")) {
                options.debug = true;

            } else if (arg.equals("-t")) {
                options.timeQueries = true;

            } else if (arg.equals("-nocache")) {
                options.noCache = true;

            } else if (arg.equals("-rc")) {
                CmdRunner.RELOAD_CONNECTION = false;

            } else if (arg.equals("-vt")) {
                options.validateXmlaResponse = VALIDATE_TRANSFORM;

            } else if (arg.equals("-vx")) {
                options.validateXmlaResponse = VALIDATE_XPATH;

            } else if (arg.equals("-f")) {
                i++;
                if (i == args.length) {
                    throw new BadOption("no mdx filename given");
                }
                options.filenames.add(args[i]);

            } else if (arg.equals("-x")) {
                i++;
                if (i == args.length) {
                    throw new BadOption("no XMLA filename given");
                }
                options.doingWhat = DO_XMLA;
                options.filenames.add(args[i]);

            } else if (arg.equals("-xs")) {
                i++;
                if (i == args.length) {
                    throw new BadOption("no XMLA filename given");
                }
                options.doingWhat = DO_SOAP_XMLA;
                options.filenames.add(args[i]);

            } else if (arg.equals("-p")) {
                i++;
                if (i == args.length) {
                    throw new BadOption("no mondrian properties file given");
                }
                String propFile = args[i];
                loadPropertiesFromFile(propFile);

            } else if (!options.filenames.isEmpty()) {
                options.filenames.add(arg);
            } else {
                options.singleMdxCmd = arg;
            }
        }
        return options;
    }

    private static class BadOption extends Exception {
        BadOption(String msg) {
            super(msg);
        }
    }
}

// End CmdRunner.java

