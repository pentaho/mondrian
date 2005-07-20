/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;

import org.apache.log4j.Logger;
import java.io.*;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Utility methods for classes in the <code>mondrian.rolap</code> package.
 *
 * @author jhyde
 * @since 22 December, 2001
 * @version $Id$
 **/
public class RolapUtil {

    private static final Logger LOGGER = Logger.getLogger(RolapUtil.class);


    static final RolapMember[] emptyMemberArray = new RolapMember[0];
    public static PrintWriter debugOut = null;
    public static Boolean produceDebugOut = null;
    private static Semaphore querySemaphore;

    /**
     * Special cell value indicates that the value is not in cache yet.
     */
    public static final RuntimeException valueNotReadyException =
            new RuntimeException("value not ready");

    /**
     * Special value represents a null key.
     */
    public static final Object sqlNullValue = new Object() {
        public boolean equals(Object o) {
            return o == this;
        }
        public int hashCode() {
            return super.hashCode();
        }
        public String toString() {
            return "null";
        }
    };

    /**
     * Names of classes of drivers we've loaded (or have tried to load).
     * @synchronization Lock the {@link RolapConnection} class.
     */
    private static final HashSet loadedDrivers = new HashSet();

    /**
     * Encloses a value in single-quotes, to make a SQL string value. Examples:
     * <code>singleQuoteForSql(null)</code> yields <code>NULL</code>;
     * <code>singleQuoteForSql("don't")</code> yields <code>'don''t'</code>.
     */
    static String singleQuoteForSql(String val) {
        StringBuffer buf = new StringBuffer(64);
        singleQuoteForSql(val, buf);
        return buf.toString();
    }

    static void singleQuoteForSql(String val, StringBuffer buf) {
        if (val == null) {
            buf.append("NULL");
        } else {
            buf.append('\'');

            String s0 = replace(val, "'", "''");
            buf.append(s0);

            buf.append('\'');
        }
    }

    /**
     * Returns <code>s</code> with <code>find</code> replaced everywhere with
     * <code>replace</code>.
     **/
    static String replace(String s, String find, String replace) {
        // let's be optimistic
        int found = s.indexOf(find);
        if (found == -1) {
            return s;
        }
        StringBuffer sb = new StringBuffer(s.length() + 20);
        char[] chars = s.toCharArray();
        int start = 0;
        for (;;) {
            sb.append(chars, start, found-start);
            if (found == s.length()) {
                break;
            }
            sb.append(replace);
            start += (find.length() + found);
            found = s.indexOf(find,start);
            if (found == -1) {
                found = s.length();
            }
        }
        return sb.toString();
    }

    static final void add(List list, Object[] array) {
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
    }

    static final RolapMember[] toArray(List v) {
        return v.isEmpty()
            ? emptyMemberArray
            : (RolapMember[]) v.toArray(new RolapMember[v.size()]);
    }

    static RolapMember lookupMember(MemberReader reader,
                                    String[] uniqueNameParts,
                                    boolean failIfNotFound) {

        RolapMember member = null;
        for (int i = 0; i < uniqueNameParts.length; i++) {
            String name = uniqueNameParts[i];
            List children;
            if (member == null) {
                children = reader.getRootMembers();
            } else {
                children = new ArrayList();
                reader.getMemberChildren(member, children);
                member = null;
            }
            for (int j = 0, n = children.size(); j < n; j++) {
                RolapMember child = (RolapMember) children.get(j);
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
            throw Util.getRes().newMdxCantFindMember(Util.implode(uniqueNameParts));
        }
        return member;
    }

    /**
     * Adds an object to the end of an array.  The resulting array is of the
     * same type (e.g. <code>String[]</code>) as the input array.
     **/
    static Object[] addElement(Object[] a, Object o) {
        Class clazz = a.getClass().getComponentType();
        Object[] a2 = (Object[]) Array.newInstance(clazz, a.length + 1);
        System.arraycopy(a, 0, a2, 0, a.length);
        a2[a.length] = o;
        return a2;
    }

    /**
     * Adds an array to the end of an array.  The resulting array is of the
     * same type (e.g. <code>String[]</code>) as the input array.
     */
    static Object[] addElements(Object[] a, Object[] b) {
        Class clazz = a.getClass().getComponentType();
        Object[] c = (Object[]) Array.newInstance(clazz, a.length + b.length);
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    /**
     * Enables tracing if the {@link MondrianProperties#TraceLevel} property
     * is greater than 0 and a debug output file
     * ({@link MondrianProperties#DebugOutFile}) is configured.
     */
    public static void checkTracing() {
        if (produceDebugOut == null) {
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
                produceDebugOut = Boolean.TRUE;
            } else {
                produceDebugOut = Boolean.FALSE;
            }
        }
    }

    /**
     * redirect debug output to another PrintWriter
     * @param pw
     */
    static public void setDebugOut( PrintWriter pw) {
        debugOut = pw;
    }

    /**
     * Executes a query, printing to the trace log if tracing is enabled.
     * If the query fails, it throws the same {@link SQLException}, and closes
     * the result set. If it succeeds, the caller must close the returned
     * {@link ResultSet}.
     */
    public static ResultSet executeQuery(Connection jdbcConnection,
                                         String sql,
                                         String component)
                                         throws SQLException {
        checkTracing();
        getQuerySemaphore().enter();
        Statement statement = null;
        ResultSet resultSet = null;
        String status = "failed";
        if (produceDebugOut == Boolean.TRUE) {
            RolapUtil.debugOut.print(
                component + ": executing sql [" + sql + "]");
            RolapUtil.debugOut.flush();
        }
        try {
            statement = jdbcConnection.createStatement();
            Date date = new Date();
            resultSet = statement.executeQuery(sql);
            long time = (new Date().getTime() - date.getTime());
            status = ", " + time + " ms";
            return resultSet;
        } catch (SQLException e) {
            status = ", failed (" + e + ")";
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e2) {
                // ignore
            }
            throw (SQLException) e.fillInStackTrace();
        } finally {
            if (produceDebugOut == Boolean.TRUE) {
                RolapUtil.debugOut.println(status);
            }
            if (LOGGER.isDebugEnabled()) {
            	LOGGER.debug(component + ": executing sql [" + sql + "]" + status);
            }
            getQuerySemaphore().leave();
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
                    LOGGER.error("Mondrian: Warning: JDBC driver "
                        + jdbcDriver + " not found");
                }
            }
        }
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
     * {@link #debugOut} from now on.
     */
    public static synchronized TeeWriter startTracing() {
        TeeWriter tw;
        if (debugOut == null) {
            tw = new TeeWriter(new NullWriter());
        } else {
            tw = new TeeWriter(RolapUtil.debugOut);
        }
        debugOut = new PrintWriter(tw);
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

    static void getMemberDescendants(MemberReader memberReader,
                                     RolapMember ancestor,
                                     RolapLevel level,
                                     List result,
                                     boolean before,
                                     boolean self,
                                     boolean after) {
        // We find the descendants of a member by making breadth-first passes
        // down the hierarchy. Initially the list just contains the ancestor.
        // Then we find its children. We add those children to the result if
        // they fulfill the before/self/after conditions relative to the level.
        //
        // We add a child to the "fertileMembers" list if some of its children
        // might be in the result. Again, this depends upon the
        // before/self/after conditions.
        //
        // Note that for some member readers -- notably the
        // RestrictedMemberReader, when it is reading a ragged hierarchy -- the
        // children of a member do not always belong to the same level. For
        // example, the children of USA include WA (a state) and Washington
        // (a city). This is why we repeat the before/self/after logic for
        // each member.
        final int levelDepth = level.getDepth();
        List members = new ArrayList();
        members.add(ancestor);
        // Each pass, "fertileMembers" has the same contents as "members",
        // except that we omit members whose children we are not interested
        // in. We allocate it once, and clear it each pass, to save a little
        // memory allocation.
        List fertileMembers = new ArrayList();
        do {
            fertileMembers.clear();
            for (int i = 0; i < members.size(); i++) {
                RolapMember member = (RolapMember) members.get(i);
                final int currentDepth = member.getLevel().getDepth();
                if (currentDepth == levelDepth) {
                    if (self) {
                        result.add(member);
                    }
                    if (after) {
                        // we are interested in member's children
                        fertileMembers.add(member);
                    }
                } else if (currentDepth < levelDepth) {
                    if (before) {
                        result.add(member);
                    }
                    fertileMembers.add(member);
                } else {
                    if (after) {
                        result.add(member);
                        fertileMembers.add(member);
                    }
                }
            }
            members.clear();
            memberReader.getMemberChildren(fertileMembers, members);
        }
        while (members.size() > 0);
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
}

// End RolapUtil.java
