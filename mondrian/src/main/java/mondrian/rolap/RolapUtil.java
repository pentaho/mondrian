/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
//
// jhyde, 22 December, 2001
*/
package mondrian.rolap;

import mondrian.calc.ExpCompiler;
import mondrian.olap.*;
import mondrian.olap.Member;
import mondrian.olap.fun.FunUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapHierarchy.LimitedRollupMember;
import mondrian.server.*;
import mondrian.spi.Dialect;
import mondrian.util.ClassResolver;

import org.apache.log4j.Logger;

import org.eigenbase.util.property.StringProperty;

import java.io.*;
import java.lang.reflect.*;
import java.sql.SQLException;
import java.util.*;

import javax.sql.DataSource;

/**
 * Utility methods for classes in the <code>mondrian.rolap</code> package.
 *
 * @author jhyde
 * @since 22 December, 2001
 */
public class RolapUtil {
    public static final Logger MDX_LOGGER = Logger.getLogger("mondrian.mdx");
    public static final Logger SQL_LOGGER = Logger.getLogger("mondrian.sql");
    public static final Logger MONITOR_LOGGER =
        Logger.getLogger("mondrian.server.monitor");
    public static final Logger PROFILE_LOGGER =
        Logger.getLogger("mondrian.profile");

    static final Logger LOGGER = Logger.getLogger(RolapUtil.class);

    /**
     * Special cell value indicates that the value is not in cache yet.
     */
    public static final Object valueNotReadyException = new Double(0);

    /**
     * Hook to run when a query is executed. This should not be
     * used at runtime but only for testing.
     */
    private static ExecuteQueryHook queryHook = null;

    /**
     * Special value represents a null key.
     */
    public static final Comparable<?> sqlNullValue =
        RolapUtilComparable.INSTANCE;

    public static Util.Functor1<Void, java.sql.Statement> getDefaultCallback(
        final Locus locus)
    {
        return new Util.Functor1<Void, java.sql.Statement>() {
            public Void apply(java.sql.Statement stmt) {
                locus.execution.registerStatement(locus, stmt);
                return null;
            }
        };
    }

    /**
     * Wraps a schema reader in a proxy so that each call to schema reader
     * has a locus for profiling purposes.
     *
     * @param connection Connection
     * @param schemaReader Schema reader
     * @return Wrapped schema reader
     */
    public static SchemaReader locusSchemaReader(
        RolapConnection connection,
        final SchemaReader schemaReader)
    {
        final Statement statement = connection.getInternalStatement();
        final Execution execution = new Execution(statement, 0);
        final Locus locus =
            new Locus(
                execution,
                "Schema reader",
                null);
        return (SchemaReader) Proxy.newProxyInstance(
            SchemaReader.class.getClassLoader(),
            new Class[]{SchemaReader.class},
            new InvocationHandler() {
                public Object invoke(
                    Object proxy,
                    Method method,
                    Object[] args)
                    throws Throwable
                {
                    Locus.push(locus);
                    try {
                        return method.invoke(schemaReader, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    } finally {
                        Locus.pop(locus);
                    }
                }
            }
        );
    }

    /**
     * Sets the query-execution hook used by tests. This method and
     * {@link #setHook(mondrian.rolap.RolapUtil.ExecuteQueryHook)} are
     * synchronized to ensure a memory barrier.
     *
     * @return Query execution hook
     */
    public static synchronized ExecuteQueryHook getHook() {
        return queryHook;
    }

    public static synchronized void setHook(ExecuteQueryHook hook) {
        queryHook = hook;
    }

    /**
     * Comparable value, equal only to itself. Used to represent the NULL value,
     * as returned from a SQL query.
     */
    private static final class RolapUtilComparable
        implements Comparable, Serializable
    {
        private static final long serialVersionUID = -2595758291465179116L;

        public static final RolapUtilComparable INSTANCE =
            new RolapUtilComparable();

        // singleton
        private RolapUtilComparable() {
        }

        // do not override equals and hashCode -- use identity

        public String toString() {
            return "#null";
        }

        public int compareTo(Object o) {
            // collates after everything (except itself)
            return o == this ? 0 : -1;
        }
    }

    /**
     * A comparator singleton instance which can handle the presence of
     * {@link RolapUtilComparable} instances in a collection.
     */
    public static final Comparator ROLAP_COMPARATOR =
        new RolapUtilComparator();

    private static final class RolapUtilComparator<T extends Comparable<T>>
        implements Comparator<T>
    {
        public int compare(T o1, T o2) {
            try {
                return o1.compareTo(o2);
            } catch (ClassCastException cce) {
                if (o2 == RolapUtilComparable.INSTANCE) {
                    return 1;
                }
                throw new MondrianException(cce);
            }
        }
    }

    /**
     * Runtime NullMemberRepresentation property change not taken into
     * consideration
     */
    private static String mdxNullLiteral = null;
    public static final String sqlNullLiteral = "null";

    public static String mdxNullLiteral() {
        if (mdxNullLiteral == null) {
            reloadNullLiteral();
        }
        return mdxNullLiteral;
    }

    public static void reloadNullLiteral() {
        mdxNullLiteral =
            MondrianProperties.instance().NullMemberRepresentation.get();
    }

    /**
     * Names of classes of drivers we've loaded (or have tried to load).
     *
     * <p>NOTE: Synchronization policy: Lock the {@link RolapConnection} class
     * before modifying or using this member.
     */
    private static final Set<String> loadedDrivers = new HashSet<String>();

    static RolapMember[] toArray(List<RolapMember> v) {
        return v.isEmpty()
            ? new RolapMember[0]
            : v.toArray(new RolapMember[v.size()]);
    }

    static RolapMember lookupMember(
        MemberReader reader,
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound)
    {
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
        List<Id.Segment> segments,
        RolapMember member,
        MemberReader reader,
        boolean failIfNotFound)
    {
        for (Id.Segment segment : segments) {
            if (!(segment instanceof Id.NameSegment)) {
                break;
            }
            final Id.NameSegment nameSegment = (Id.NameSegment) segment;
            List<RolapMember> children;
            if (member == null) {
                children = reader.getRootMembers();
            } else {
                children = new ArrayList<RolapMember>();
                reader.getMemberChildren(member, children);
                member = null;
            }
            for (RolapMember child : children) {
                if (child.getName().equals(nameSegment.name)) {
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
                Util.implode(segments));
        }
        return member;
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
     * @param locus Locus of execution
     * @return ResultSet
     */
    public static SqlStatement executeQuery(
        DataSource dataSource,
        String sql,
        Locus locus)
    {
        return executeQuery(
            dataSource, sql, null, 0, 0, locus, -1, -1,
            getDefaultCallback(locus));
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
     *
     * @param dataSource DataSource
     * @param sql SQL string
     * @param types Suggested types of columns, or null;
     *     if present, must have one element for each SQL column;
     *     each not-null entry overrides deduced JDBC type of the column
     * @param maxRowCount Maximum number of rows to retrieve, <= 0 if unlimited
     * @param firstRowOrdinal Ordinal of row to skip to (1-based), or 0 to
     *   start from beginning
     * @param locus Execution context of this statement
     * @param resultSetType Result set type, or -1 to use default
     * @param resultSetConcurrency Result set concurrency, or -1 to use default
     * @return ResultSet
     */
    public static SqlStatement executeQuery(
        DataSource dataSource,
        String sql,
        List<SqlStatement.Type> types,
        int maxRowCount,
        int firstRowOrdinal,
        Locus locus,
        int resultSetType,
        int resultSetConcurrency,
        Util.Functor1<Void, java.sql.Statement> callback)
    {
        SqlStatement stmt =
            new SqlStatement(
                dataSource, sql, types, maxRowCount, firstRowOrdinal, locus,
                resultSetType, resultSetConcurrency,
                callback == null
                    ? getDefaultCallback(locus)
                    : callback);
        stmt.execute();
        return stmt;
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
        String functionName,
        String reason)
        throws NativeEvaluationUnsupportedException
    {
        // No i18n for log message, but yes for excn
        String alertMsg =
            "Unable to use native SQL evaluation for '" + functionName
            + "'; reason:  " + reason;

        StringProperty alertProperty =
            MondrianProperties.instance().AlertNativeEvaluationUnsupported;
        String alertValue = alertProperty.get();

        if (alertValue.equalsIgnoreCase(
                org.apache.log4j.Level.WARN.toString()))
        {
            LOGGER.warn(alertMsg);
        } else if (alertValue.equalsIgnoreCase(
                org.apache.log4j.Level.ERROR.toString()))
        {
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
                    ClassResolver.INSTANCE.forName(jdbcDriver, true);
                    LOGGER.info(
                        "Mondrian: JDBC driver "
                        + jdbcDriver + " loaded successfully");
                } catch (ClassNotFoundException e) {
                    LOGGER.warn(
                        "Mondrian: Warning: JDBC driver "
                        + jdbcDriver + " not found");
                }
            }
        }
    }

    /**
     * Creates a compiler which will generate programs which will test
     * whether the dependencies declared via
     * {@link mondrian.calc.Calc#dependsOn(Hierarchy)} are accurate.
     */
    public static ExpCompiler createDependencyTestingCompiler(
        ExpCompiler compiler)
    {
        return new RolapDependencyTestingEvaluator.DteCompiler(compiler);
    }

    /**
     * Locates a member specified by its member name, from an array of
     * members.  If an exact match isn't found, but a matchType of BEFORE
     * or AFTER is specified, then the closest matching member is returned.
     *
     *
     * @param members array of members to search from
     * @param parent parent member corresponding to the member being searched
     * for
     * @param level level of the member
     * @param searchName member name
     * @param matchType match type
     * @return matching member (if it exists) or the closest matching one
     * in the case of a BEFORE or AFTER search
     */
    public static Member findBestMemberMatch(
        List<? extends Member> members,
        RolapMember parent,
        RolapLevel level,
        Id.Segment searchName,
        MatchType matchType)
    {
        if (!(searchName instanceof Id.NameSegment)) {
            return null;
        }
        final Id.NameSegment nameSegment = (Id.NameSegment) searchName;
        switch (matchType) {
        case FIRST:
            return members.get(0);
        case LAST:
            return members.get(members.size() - 1);
        default:
            // fall through
        }
        // create a member corresponding to the member we're trying
        // to locate so we can use it to hierarchically compare against
        // the members array
        Member searchMember =
            level.getHierarchy().createMember(
                parent, level, nameSegment.name, null);
        Member bestMatch = null;
        for (Member member : members) {
            int rc;
            if (searchName.quoting == Id.Quoting.KEY
                && member instanceof RolapMember)
            {
                if (((RolapMember) member).getKey().toString().equals(
                        nameSegment.name))
                {
                    return member;
                }
            }
            if (matchType.isExact()) {
                rc = Util.compareName(member.getName(), nameSegment.name);
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
                if (rc < 0
                    && (bestMatch == null
                        || FunUtil.compareSiblingMembers(member, bestMatch)
                        > 0))
                {
                    bestMatch = member;
                }
            } else if (matchType == MatchType.AFTER) {
                if (rc > 0
                    && (bestMatch == null
                        || FunUtil.compareSiblingMembers(member, bestMatch)
                        < 0))
                {
                    bestMatch = member;
                }
            }
        }
        if (matchType.isExact()) {
            return null;
        }
        return bestMatch;
    }

    public static MondrianDef.Relation convertInlineTableToRelation(
        MondrianDef.InlineTable inlineTable,
        final Dialect dialect)
    {
        MondrianDef.View view = new MondrianDef.View();
        view.alias = inlineTable.alias;

        final int columnCount = inlineTable.columnDefs.array.length;
        List<String> columnNames = new ArrayList<String>();
        List<String> columnTypes = new ArrayList<String>();
        for (int i = 0; i < columnCount; i++) {
            columnNames.add(inlineTable.columnDefs.array[i].name);
            columnTypes.add(inlineTable.columnDefs.array[i].type);
        }
        List<String[]> valueList = new ArrayList<String[]>();
        for (MondrianDef.Row row : inlineTable.rows.array) {
            String[] values = new String[columnCount];
            for (MondrianDef.Value value : row.values) {
                final int columnOrdinal = columnNames.indexOf(value.column);
                if (columnOrdinal < 0) {
                    throw Util.newError(
                        "Unknown column '" + value.column + "'");
                }
                values[columnOrdinal] = value.cdata;
            }
            valueList.add(values);
        }
        view.addCode(
            "generic",
            dialect.generateInline(
                columnNames,
                columnTypes,
                valueList));
        return view;
    }

    public static RolapMember strip(RolapMember member) {
        if (member instanceof RolapCubeMember) {
            return ((RolapCubeMember) member).getRolapMember();
        }
        return member;
    }

    public static ExpCompiler createProfilingCompiler(ExpCompiler compiler) {
        return new RolapProfilingEvaluator.ProfilingEvaluatorCompiler(
            compiler);
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
     * Creates a dummy evaluator.
     */
    public static Evaluator createEvaluator(
        Statement statement)
    {
        Execution dummyExecution = new Execution(statement, 0);
        final RolapResult result = new RolapResult(dummyExecution, false);
        return result.getRootEvaluator();
    }

    public static interface ExecuteQueryHook {
        void onExecuteQuery(String sql);
    }

    /**
     * Modifies a bitkey so that it includes the proper bits
     * for members in an array which should be considered
     * as a limited rollup member.
     */
    public static void constraintBitkeyForLimitedMembers(
        Evaluator evaluator,
        Member[] members,
        RolapCube cube,
        BitKey levelBitKey)
    {
        // Limited Rollup Members have to be included in the bitkey
        // so that we can pick the correct agg table.
        for (Member curMember : members) {
            if (curMember instanceof LimitedRollupMember) {
                final int savepoint = evaluator.savepoint();
                try {
                    // set NonEmpty to false to avoid the possibility of
                    // constraining member retrieval by context, which itself
                    // requires determination of limited members, resulting
                    // in infinite loop.
                    evaluator.setNonEmpty(false);
                    List<Member> lowestMembers =
                        ((RolapHierarchy)curMember.getHierarchy())
                            .getLowestMembersForAccess(
                                evaluator,
                                ((LimitedRollupMember)curMember)
                                    .hierarchyAccess,
                                FunUtil.getNonEmptyMemberChildrenWithDetails(
                                    evaluator,
                                    curMember));

                    assert lowestMembers.size() > 0;

                    Member lowMember = lowestMembers.get(0);

                    while (true) {
                        RolapStar.Column curColumn =
                            ((RolapCubeLevel)lowMember.getLevel())
                                .getBaseStarKeyColumn(cube);

                        if (curColumn != null) {
                            levelBitKey.set(curColumn.getBitPosition());
                        }

                        // If the level doesn't have unique members, we have to
                        // add the parent levels until the keys are unique,
                        // or all of them are added.
                        if (!((RolapCubeLevel)lowMember
                            .getLevel()).isUnique())
                        {
                            lowMember = lowMember.getParentMember();
                            if (lowMember.isAll()) {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                } finally {
                    evaluator.restore(savepoint);
                }
            }
        }
    }

    /**Generates rolap star key based on the fact
     * using fact alias and SQl filter data
     * if this one is present in the fact
     * @param fact the fact based on which is generated the rolap star key
     * @return the rolap star key
     */
    public static List<String> makeRolapStarKey(
        final MondrianDef.Relation fact)
    {
      List<String> rlStarKey = new ArrayList<String>();
      MondrianDef.Table table = null;
      rlStarKey.add(fact.getAlias());
      if (fact instanceof MondrianDef.Table) {
        table = (MondrianDef.Table) fact;
      }
      // Add SQL filter to the key
      if (!Util.isNull(table) && !Util.isNull(table.filter)
          && !Util.isBlank(table.filter.cdata))
      {
        rlStarKey.add(table.filter.dialect);
        rlStarKey.add(table.filter.cdata);
      }
      return Collections.unmodifiableList(rlStarKey);
    }

    /**Generates rolap star key based on the fact table name.
     * @param factTableName the fact table name
     * based on which is generated the rolap star key
     * @return the rolap star key
     */
    public static List<String> makeRolapStarKey(String factTableName) {
      return Collections.unmodifiableList(Arrays.asList(factTableName));
    }


}

// End RolapUtil.java
