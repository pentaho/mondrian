/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.resource.MondrianResource;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.StringProperty;
import java.io.*;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.*;

import mondrian.calc.ExpCompiler;

import javax.sql.DataSource;

/**
 * Utility methods for classes in the <code>mondrian.rolap</code> package.
 *
 * @author jhyde
 * @since 22 December, 2001
 * @version $Id$
 */
public class RolapUtil {

    static final Logger LOGGER = Logger.getLogger(RolapUtil.class);
    static final RolapMember[] emptyMemberArray = new RolapMember[0];
    private static PrintWriter traceOut = null;
    private static boolean checkedTracing = false;
    private static boolean traceEnabled;
    private static Semaphore querySemaphore;

    /**
     * Special cell value indicates that the value is not in cache yet.
     */
    public static final Object valueNotReadyException = new Double(0);

    /**
     * Hook to run when a query is executed.
     */
    static final ThreadLocal<ExecuteQueryHook> threadHooks =
        new ThreadLocal<ExecuteQueryHook>();

    /**
     * Special value represents a null key.
     */
    public static final Comparable sqlNullValue = new Comparable() {
        public boolean equals(Object o) {
            return o == this;
        }
        public int hashCode() {
            return super.hashCode();
        }
        public String toString() {
            return "#null";
        }

        public int compareTo(Object o) {
            return o == this ? 0 : -1;
        }
    };

    public static final String mdxNullLiteral = "#null";
    public static final String sqlNullLiteral = "null";

    /**
     * Names of classes of drivers we've loaded (or have tried to load).
     *
     * <p>NOTE: Synchronization policy: Lock the {@link RolapConnection} class
     * before modifying or using this member.
     */
    private static final Set<String> loadedDrivers = new HashSet<String>();

    static RolapMember[] toArray(List<RolapMember> v) {
        return v.isEmpty()
            ? emptyMemberArray
            : v.toArray(new RolapMember[v.size()]);
    }

    static RolapMember lookupMember(
            MemberReader reader,
            String[] uniqueNameParts,
            boolean failIfNotFound) {
        RolapMember member =
            lookupMemberInternal(
                uniqueNameParts, null, reader, failIfNotFound);
        if (member != null) {
            return member;
        }

        // If this hierarchy has an 'all' member, we can omit it.
        // For example, '[Gender].[(All Gender)].[F]' can be abbreviated
        // '[Gender].[F]'.
        final List<RolapMember> rootMembers = reader.getRootMembers();
        if (rootMembers.size() == 1) {
            final RolapMember rootMember = rootMembers.get(0);
            if (rootMember.isAll()) {
                member =
                    lookupMemberInternal(
                        uniqueNameParts, rootMember, reader, failIfNotFound);
            }
        }
        return member;
    }

    private static RolapMember lookupMemberInternal(
        String[] uniqueNameParts,
        RolapMember member,
        MemberReader reader,
        boolean failIfNotFound)
    {
        for (String name : uniqueNameParts) {
            List<RolapMember> children;
            if (member == null) {
                children = reader.getRootMembers();
            } else {
                children = new ArrayList<RolapMember>();
                reader.getMemberChildren(member, children);
                member = null;
            }
            for (RolapMember child : children) {
                if (child.getName().equals(name)) {
                    member = child;
                    break;
                }
            }
            if (member == null) {
                break;
            }
        }
        if (member == null && failIfNotFound) {
            throw MondrianResource.instance().MdxCantFindMember.ex(
                Util.implode(uniqueNameParts));
        }
        return member;
    }

    /**
     * Adds an object to the end of an array.  The resulting array is of the
     * same type (e.g. <code>String[]</code>) as the input array.
     */
    static <T> T[] addElement(T[] a, T o) {
        Class clazz = a.getClass().getComponentType();
        T[] a2 = (T[]) Array.newInstance(clazz, a.length + 1);
        System.arraycopy(a, 0, a2, 0, a.length);
        a2[a.length] = o;
        return a2;
    }

    /**
     * Adds an array to the end of an array.  The resulting array is of the
     * same type (e.g. <code>String[]</code>) as the input array.
     */
    static <T> T[] addElements(T[] a, T[] b) {
        Class clazz = a.getClass().getComponentType();
        T[] c = (T[]) Array.newInstance(clazz, a.length + b.length);
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    /**
     * Checks whether tracing is enabled, and returns a PrintWriter if so.
     *
     * <p>Tracing is enabled if the {@link MondrianProperties#TraceLevel}
     * property is greater than 0 and a debug output file
     * ({@link MondrianProperties#DebugOutFile}) is configured.
     *
     * <p>These properties are only checked the first time this method is
     * called; subsequent invocations return the cached result.
     *
     * @return A PrintWriter if tracing is enabled, otherwise null
     */
    public static synchronized PrintWriter checkTracing() {
        if (!checkedTracing) {
            int trace = MondrianProperties.instance().TraceLevel.get();
            if (trace > 0) {
                String debugOutFile =
                        MondrianProperties.instance().DebugOutFile.get();
                if (debugOutFile != null) {
                    File f;
                    try {
                        f = new File(debugOutFile);
                        setDebugOut(new PrintWriter(new FileOutputStream(f), true));
                    } catch (Exception e) {
                        setDebugOut(new PrintWriter(System.out, true));
                    }
                } else {
                    setDebugOut(new PrintWriter(System.out, true));
                }
                traceEnabled = true;
            } else {
                traceEnabled = false;
            }
            checkedTracing = true;
        }
        return traceEnabled ? traceOut : null;
    }

    /**
     * Redirects debug output to another PrintWriter.
     * @param pw A PrintWriter
     */
    static public void setDebugOut(PrintWriter pw) {
        traceOut = pw;
    }

    /**
     * Executes a query, printing to the trace log if tracing is enabled.
     *
     * <p>If the query fails, it wraps the {@link SQLException} in a runtime
     * exception with <code>message</code> as description, and closes the result
     * set.
     *
     * <p>If it succeeds, the caller must call the {@link SqlStatement#close}
     * method of the returned {@link SqlStatement}.
     *
     * @param dataSource DataSource
     * @param sql SQL string
     * @param component Description of a the component executing the query,
     *   generally a method name, e.g. "SqlTupleReader.readTuples"
     * @param message Description of the purpose of this statement, to be
     *   printed if there is an error
     * @return ResultSet
     */
    public static SqlStatement executeQuery(
        DataSource dataSource,
        String sql,
        String component,
        String message)
    {
        return executeQuery(
            dataSource, sql, -1, component, message, -1, -1);
    }

    /**
     * Executes a query.
     *
     * <p>If the query fails, it wraps the {@link SQLException} in a runtime
     * exception with <code>message</code> as description, and closes the result
     * set.
     *
     * <p>If it succeeds, the caller must call the {@link SqlStatement#close}
     * method of the returned {@link SqlStatement}.
     *
     * @param dataSource DataSource
     * @param sql SQL string
     * @param maxRows Row limit, or -1 if no limit
     * @param component Description of a the component executing the query,
     *   generally a method name, e.g. "SqlTupleReader.readTuples"
     * @param message Description of the purpose of this statement, to be
     *   printed if there is an error
     * @param resultSetType Result set type, or -1 to use default
     * @param resultSetConcurrency Result set concurrency, or -1 to use default
     * @return ResultSet
     */
    public static SqlStatement executeQuery(
        DataSource dataSource,
        String sql,
        int maxRows,
        String component,
        String message,
        int resultSetType,
        int resultSetConcurrency)
    {
        SqlStatement stmt =
            new SqlStatement(
                dataSource, sql, maxRows, component, message,
                resultSetType, resultSetConcurrency);
        try {
            stmt.execute();
            return stmt;
        } catch (SQLException e) {
            throw stmt.handle(e);
        }
    }

    /**
     * Raises an alert that native SQL evaluation could not be used
     * in a case where it might have been beneficial, but some
     * limitation in Mondrian's implementation prevented it.
     * (Do not call this in cases where native evaluation would
     * have been wasted effort.)
     *
     * @param functionName name of function for which native evaluation
     * was skipped
     *
     * @param reason reason why native evaluation was skipped
     */
    public static void alertNonNative(
        String functionName, String reason)
        throws NativeEvaluationUnsupportedException {

        // No i18n for log message, but yes for excn
        String alertMsg =
            "Unable to use native SQL evaluation for '" + functionName
            + "'; reason:  " + reason;

        StringProperty alertProperty =
            MondrianProperties.instance().AlertNativeEvaluationUnsupported;
        String alertValue = alertProperty.get();

        if (alertValue.equalsIgnoreCase(
                org.apache.log4j.Level.WARN.toString())) {
            LOGGER.warn(alertMsg);
        } else if (alertValue.equalsIgnoreCase(
                       org.apache.log4j.Level.ERROR.toString())) {
            LOGGER.error(alertMsg);
            throw MondrianResource.instance().NativeEvaluationUnsupported.ex(
                functionName);
        }
    }

    /**
     * Loads a set of JDBC drivers.
     *
     * @param jdbcDrivers A string consisting of the comma-separated names
     *  of JDBC driver classes. For example
     *  <code>"sun.jdbc.odbc.JdbcOdbcDriver,com.mysql.jdbc.Driver"</code>.
     */
    public static synchronized void loadDrivers(String jdbcDrivers) {
        StringTokenizer tok = new StringTokenizer(jdbcDrivers, ",");
        while (tok.hasMoreTokens()) {
            String jdbcDriver = tok.nextToken();
            if (loadedDrivers.add(jdbcDriver)) {
                try {
                    Class.forName(jdbcDriver);
                    LOGGER.info("Mondrian: JDBC driver "
                        + jdbcDriver + " loaded successfully");
                } catch (ClassNotFoundException e) {
                    LOGGER.warn("Mondrian: Warning: JDBC driver "
                        + jdbcDriver + " not found");
                }
            }
        }
    }

    /**
     * Creates a compiler which will generate programs which will test
     * whether the dependencies declared via
     * {@link mondrian.calc.Calc#dependsOn(mondrian.olap.Dimension)} are
     * accurate.
     */
    public static ExpCompiler createDependencyTestingCompiler(
            ExpCompiler compiler) {
        return new RolapDependencyTestingEvaluator.DteCompiler(compiler);
    }

    /**
     * Locates a member specified by its member name, from an array of
     * members.  If an exact match isn't found, but a matchType of BEFORE
     * or AFTER is specified, then the closest matching member is returned.
     *
     * @param members array of members to search from
     * @param parent parent member corresponding to the member being searched
     * for
     * @param level level of the member
     * @param searchName member name
     * @param matchType match type
     * @param caseInsensitive if true, use case insensitive search (if
     * applicable) when when doing exact searches
     *
     * @return matching member (if it exists) or the closest matching one
     * in the case of a BEFORE or AFTER search
     */
    public static Member findBestMemberMatch(
        List<? extends Member> members,
        RolapMember parent,
        RolapLevel level,
        String searchName,
        MatchType matchType,
        boolean caseInsensitive)
    {
        // create a member corresponding to the member we're trying
        // to locate so we can use it to hierarchically compare against
        // the members array
        RolapMember searchMember = new RolapMember(parent, level, searchName);
        Member bestMatch = null;
        for (Member member : members) {
            int rc;
            if (matchType == MatchType.EXACT) {
                if (caseInsensitive) {
                    rc = Util.compareName(member.getName(), searchName);
                } else {
                    rc = member.getName().compareTo(searchName);
                }
            } else {
                rc =
                    FunUtil.compareSiblingMembers(
                        member,
                        searchMember);
            }
            if (rc == 0) {
                return member;
            }
            if (matchType == MatchType.BEFORE) {
                if (rc < 0 &&
                    (bestMatch == null ||
                        FunUtil.compareSiblingMembers(member, bestMatch) > 0)) {
                    bestMatch = member;
                }
            } else if (matchType == MatchType.AFTER) {
                if (rc > 0 &&
                    (bestMatch == null ||
                        FunUtil.compareSiblingMembers(member, bestMatch) < 0)) {
                    bestMatch = member;
                }
            }
        }
        if (matchType == MatchType.EXACT) {
            return null;
        }
        return bestMatch;
    }

    /**
     * Writes to a string and also to an underlying writer.
     */
    public static class TeeWriter extends FilterWriter {
        StringWriter buf = new StringWriter();
        public TeeWriter(Writer out) {
            super(out);
        }

        /**
         * Returns everything which has been written so far.
         */
        public String toString() {
            return buf.toString();
        }

        /**
         * Returns the underlying writer.
         */
        public Writer getWriter() {
            return out;
        }

        public void write(int c) throws IOException {
            super.write(c);
            buf.write(c);
        }

        public void write(char cbuf[]) throws IOException {
            super.write(cbuf);
            buf.write(cbuf);
        }

        public void write(char cbuf[], int off, int len) throws IOException {
            super.write(cbuf, off, len);
            buf.write(cbuf, off, len);
        }

        public void write(String str) throws IOException {
            super.write(str);
            buf.write(str);
        }

        public void write(String str, int off, int len) throws IOException {
            super.write(str, off, len);
            buf.write(str, off, len);
        }
    }

    /**
     * Writer which throws away all input.
     */
    private static class NullWriter extends Writer {
        public void write(char cbuf[], int off, int len) throws IOException {
        }

        public void flush() throws IOException {
        }

        public void close() throws IOException {
        }
    }

    /**
     * Creates a {@link TeeWriter} which captures everything which goes through
     * {@link #traceOut} from now on.
     */
    public static synchronized TeeWriter startTracing() {
        TeeWriter tw;
        if (traceOut == null) {
            tw = new TeeWriter(new NullWriter());
        } else {
            tw = new TeeWriter(RolapUtil.traceOut);
        }
        traceOut = new PrintWriter(tw);
        return tw;
    }

    /**
     * Gets the semaphore which controls how many people can run queries
     * simultaneously.
     */
    static synchronized Semaphore getQuerySemaphore() {
        if (querySemaphore == null) {
            int queryCount = MondrianProperties.instance().QueryLimit.get();
            querySemaphore = new Semaphore(queryCount);
        }
        return querySemaphore;
    }

    /**
     * Creates a dummy evaluator.
     */
    public static Evaluator createEvaluator(Query query) {
        final RolapResult result = new RolapResult(query, false);
        return result.getRootEvaluator();
    }

    /**
     * A <code>Semaphore</code> is a primitive for process synchronization.
     *
     * <p>Given a semaphore initialized with <code>count</code>, no more than
     * <code>count</code> threads can acquire the semaphore using the
     * {@link #enter} method. Waiting threads block until enough threads have
     * called {@link #leave}.
     */
    static class Semaphore {
        private int count;
        Semaphore(int count) {
            if (count < 0) {
                count = Integer.MAX_VALUE;
            }
            this.count = count;
        }
        synchronized void enter() {
            if (count == Integer.MAX_VALUE) {
                return;
            }
            if (count == 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw Util.newInternal(e, "while waiting for semaphore");
                }
            }
            Util.assertTrue(count > 0);
            count--;
        }
        synchronized void leave() {
            if (count == Integer.MAX_VALUE) {
                return;
            }
            count++;
            notify();
        }
    }

    static interface ExecuteQueryHook {
        void onExecuteQuery(String sql);
    }

}

// End RolapUtil.java
