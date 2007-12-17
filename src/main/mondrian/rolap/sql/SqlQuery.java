/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 21, 2002
*/

package mondrian.rolap.sql;

import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.RolapUtil;
import org.eigenbase.util.property.Property;
import org.eigenbase.util.property.Trigger;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * <code>SqlQuery</code> allows us to build a <code>select</code>
 * statement and generate it in database-specific SQL syntax.
 *
 * <p> Notable differences in database syntax are:<dl>
 *
 * <dt> Identifier quoting </dt>
 * <dd> Oracle (and all JDBC-compliant drivers) uses double-quotes,
 * for example <code>select * from "emp"</code>. Access prefers brackets,
 * for example <code>select * from [emp]</code>. mySQL allows single- and
 * double-quotes for string literals, and therefore does not allow
 * identifiers to be quoted, for example <code>select 'foo', "bar" from
 * emp</code>. </dd>
 *
 * <dt> AS in from clause </dt>
 * <dd> Oracle doesn't like AS in the from * clause, for example
 * <code>select from emp as e</code> vs. <code>select * from emp
 * e</code>. </dd>
 *
 * <dt> Column aliases </dt>
 * <dd> Some databases require that every column in the select list
 * has a valid alias. If the expression is an expression containing
 * non-alphanumeric characters, an explicit alias is needed. For example,
 * Oracle will barfs at <code>select empno + 1 from emp</code>. </dd>
 *
 * <dt> Parentheses around table names </dt>
 * <dd> Oracle doesn't like <code>select * from (emp)</code> </dd>
 *
 * <dt> Queries in FROM clause </dt>
 * <dd> PostgreSQL and hsqldb don't allow, for example, <code>select * from
 * (select * from emp) as e</code>.</dd>
 *
 * <dt> Uniqueness of index names </dt>
 * <dd> In PostgreSQL and Oracle, index names must be unique within the
 * database; in Access and hsqldb, they must merely be unique within their
 * table </dd>
 *
 * <dt> Datatypes </dt>
 * <dd> In Oracle, BIT is CHAR(1), TIMESTAMP is DATE.
 *      In PostgreSQL, DOUBLE is DOUBLE PRECISION, BIT is BOOL. </dd>
 * </ul>
 *
 * <p>
 * NOTE: Instances of this class are NOT thread safe so the user must make
 * sure this is accessed by only one thread at a time.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlQuery {
    /** Controls the formatting of the sql string. */
    private static boolean generateFormattedSql =
        MondrianProperties.instance().GenerateFormattedSql.get();

    static {
        // Trigger is used to lookup and change the value of the
        // variable that controls formatting.
        // Using a trigger means we don't have to look up the property eveytime.
        MondrianProperties.instance().GenerateFormattedSql.addTrigger(
            new Trigger() {
                public boolean isPersistent() {
                    return true;
                }
                public int phase() {
                    return Trigger.PRIMARY_PHASE;
                }
                public void execute(Property property, String value) {
                    generateFormattedSql = property.booleanValue();
                }
            }
        );
    }

    private boolean distinct;

    private final ClauseList select;
    private final ClauseList from;
    private final ClauseList where;
    private final ClauseList groupBy;
    private final ClauseList having;
    private final ClauseList orderBy;
    private final List<ClauseList> groupingSet;
    private final ClauseList groupingFunction;

    /**
     * This list is used to keep track of what aliases have been  used in the
     * FROM clause. One might think that a java.util.Set would be a more
     * appropriate Collection type, but if you only have a couple of "from
     * aliases", then iterating over a list is faster than doing a hash lookup
     * (as is used in java.util.HashSet).
     */
    private final List<String> fromAliases;

    /** The SQL dialect this query is to be generated in. */
    private final Dialect dialect;

    /** Scratch buffer. Clear it before use. */
    private final StringBuilder buf;

    /**
     * Base constructor used by all other constructors to create an empty
     * instance.
     */
    public SqlQuery(Dialect dialect) {

        // both select and from allow duplications
        this.select = new ClauseList(true);
        this.from = new ClauseList(true);

        this.groupingFunction = new ClauseList(false);
        this.where = new ClauseList(false);
        this.groupBy = new ClauseList(false);
        this.having = new ClauseList(false);
        this.orderBy = new ClauseList(false);
        this.fromAliases = new ArrayList<String>();
        this.buf = new StringBuilder(128);
        this.groupingSet = new ArrayList<ClauseList>();
        this.dialect = dialect;
    }

    /**
     * Creates a <code>SqlQuery</code>
     *
     * @param databaseMetaData used to determine which dialect of
     *     SQL to generate. Must not be held beyond the constructor.
     */
    public SqlQuery(final DatabaseMetaData databaseMetaData) {
        this(Dialect.create(databaseMetaData));
    }

    /**
     * Creates an empty <code>SqlQuery</code> with the same environment as this
     * one. (As per the Gang of Four 'prototype' pattern.)
     */
    public SqlQuery cloneEmpty()
    {
        return new SqlQuery(dialect);
    }

    public void setDistinct(final boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * The size required to add quotes around a string - this ought to be
     * large enough to prevent a reallocation.
     */
    private static final int SINGLE_QUOTE_SIZE = 10;
    /**
     * Two strings are quoted and the character '.' is placed between them.
     */
    private static final int DOUBLE_QUOTE_SIZE = 2 * SINGLE_QUOTE_SIZE + 1;

    /**
     * Adds a subquery to the FROM clause of this Query with a given alias.
     * If the query already exists it either, depending on
     * <code>failIfExists</code>, throws an exception or does not add the query
     * and returns false.
     *
     * @param query Subquery
     * @param alias (if not null, must not be zero length).
     * @param failIfExists if true, throws exception if alias already exists
     * @return true if query *was* added
     *
     * @pre alias != null
     */
    public boolean addFromQuery(
        final String query,
        final String alias,
        final boolean failIfExists)
    {
        assert alias != null;

        if (fromAliases.contains(alias)) {
            if (failIfExists) {
                throw Util.newInternal(
                        "query already contains alias '" + alias + "'");
            } else {
                return false;
            }
        }

        buf.setLength(0);

        buf.append('(');
        buf.append(query);
        buf.append(')');
        if (alias != null) {
            Util.assertTrue(alias.length() > 0);

            if (dialect.allowsAs()) {
                buf.append(" as ");
            } else {
                buf.append(' ');
            }
            dialect.quoteIdentifier(alias, buf);
            fromAliases.add(alias);
        }

        from.add(buf.toString());
        return true;
    }

    /**
     * Adds <code>[schema.]table AS alias</code> to the FROM clause.
     *
     * @param schema schema name; may be null
     * @param table table name
     * @param alias table alias, may not be null
     *              (if not null, must not be zero length).
     * @param failIfExists Whether to throw a RuntimeException if from clause
     *   already contains this alias
     *
     * @pre alias != null
     * @return true if table was added
     */
    boolean addFromTable(
        final String schema,
        final String table,
        final String alias,
        final String filter,
        final boolean failIfExists)
    {
        if (fromAliases.contains(alias)) {
            if (failIfExists) {
                throw Util.newInternal(
                        "query already contains alias '" + alias + "'");
            } else {
                return false;
            }
        }

        buf.setLength(0);
        dialect.quoteIdentifier(schema, table, buf);
        if (alias != null) {
            Util.assertTrue(alias.length() > 0);

            if (dialect.allowsAs()) {
                buf.append(" as ");
            } else {
                buf.append(' ');
            }
            dialect.quoteIdentifier(alias, buf);
            fromAliases.add(alias);
        }

        from.add(buf.toString());

        if (filter != null) {
            // append filter condition to where clause
            addWhere("(", filter, ")");
        }
        return true;
    }

    public void addFrom(final SqlQuery sqlQuery,
                        final String alias,
                        final boolean failIfExists)
    {
        addFromQuery(sqlQuery.toString(), alias, failIfExists);
    }

    /**
     * Adds a relation to a query, adding appropriate join conditions, unless
     * it is already present.
     *
     * <p>Returns whether the relation was added to the query.
     *
     * @param relation Relation to add
     * @param alias Alias of relation. If null, uses relation's alias.
     * @param failIfExists Whether to fail if relation is already present
     * @return true, if relation *was* added to query
     */
    public boolean addFrom(
        final MondrianDef.RelationOrJoin relation,
        final String alias,
        final boolean failIfExists)
    {
        if (relation instanceof MondrianDef.View) {
            final MondrianDef.View view = (MondrianDef.View) relation;
            final String viewAlias = (alias == null)
                    ? view.getAlias()
                    : alias;
            final String sqlString = view.getCodeSet().chooseQuery(dialect);
            return addFromQuery(sqlString, viewAlias, false);

        } else if (relation instanceof MondrianDef.InlineTable) {
            final MondrianDef.Relation relation1 =
                RolapUtil.convertInlineTableToRelation(
                    (MondrianDef.InlineTable) relation, dialect);
            return addFrom(relation1, alias, failIfExists);

        } else if (relation instanceof MondrianDef.Table) {
            final MondrianDef.Table table = (MondrianDef.Table) relation;
            final String tableAlias = (alias == null)
                    ? table.getAlias()
                    : alias;
            return addFromTable(
                table.schema, table.name, tableAlias,
                table.getFilter(), failIfExists);

        } else if (relation instanceof MondrianDef.Join) {
            final MondrianDef.Join join = (MondrianDef.Join) relation;
            final String leftAlias = join.getLeftAlias();
            final String rightAlias = join.getRightAlias();

            boolean addLeft = addFrom(join.left, leftAlias, failIfExists);
            boolean addRight = addFrom(join.right, rightAlias, failIfExists);

            boolean added = addLeft || addRight;
            if (added) {
                buf.setLength(0);

                dialect.quoteIdentifier(leftAlias, join.leftKey, buf);
                buf.append(" = ");
                dialect.quoteIdentifier(rightAlias, join.rightKey, buf);

                addWhere(buf.toString());
            }
            return added;

        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Adds an expression to the select clause, automatically creating a
     * column alias.
     */
    public void addSelect(final String expression) {
        // Some DB2 versions (AS/400) throw an error if a column alias is
        //  *not* used in a subsequent order by (Group by).
        // Derby fails on 'SELECT... HAVING' if column has alias.
        if (dialect.isAS400() || dialect.isDerby()) {
            addSelect(expression, null);
        } else {
            addSelect(expression, nextColumnAlias());
        }
    }

    public int getCurrentSelectListSize()
    {
        return select.size();
    }

    public String nextColumnAlias() {
        return "c" + select.size();
    }

    /** Adds an expression to the select clause, with a specified column
     * alias. */
    public void addSelect(final String expression, final String alias) {
        buf.setLength(0);

        buf.append(expression);
        if (alias != null) {
            buf.append(" as ");
            dialect.quoteIdentifier(alias, buf);
        }

        select.add(buf.toString());
    }

    public void addWhere(
            final String exprLeft,
            final String exprMid,
            final String exprRight)
    {
        int len = exprLeft.length() + exprMid.length() + exprRight.length();
        StringBuilder buf = new StringBuilder(len);

        buf.append(exprLeft);
        buf.append(exprMid);
        buf.append(exprRight);

        addWhere(buf.toString());
    }

    public void addWhere(final String expression)
    {
        where.add(expression);
    }

    public void addGroupBy(final String expression)
    {
        groupBy.add(expression);
    }

    public void addHaving(final String expression)
    {
        having.add(expression);
    }

    /**
     * Adds an item to the ORDER BY clause.
     *
     * @param expr the expr to order by
     * @param ascending sort direction
     * @param prepend whether to prepend to the current list of items
     * @param nullable whether the expression might be null
     */
    public void addOrderBy(
        String expr,
        boolean ascending,
        boolean prepend,
        boolean nullable)
    {
        if (nullable && !dialect.isNullsCollateLast()) {
            expr = dialect.forceNullsCollateLast(expr);
        }

        if (ascending) {
            expr = expr + " ASC";
        } else {
            expr = expr + " DESC";
        }
        if (prepend) {
            orderBy.add(0, expr);
        } else {
            orderBy.add(expr);
        }
    }

    public String toString()
    {
        if (generateFormattedSql) {
            StringWriter sw = new StringWriter(256);
            PrintWriter pw = new PrintWriter(sw);
            print(pw, "");
            pw.flush();
            return sw.toString();

        } else {
            buf.setLength(0);

            select.toBuffer(buf,
                distinct ? "select distinct " : "select ", ", ");
            buf.append(getGroupingFunction(""));
            from.toBuffer(buf, " from ", ", ");
            where.toBuffer(buf, " where ", " and ");
            if (hasGroupingSet()) {
                StringWriter stringWriter = new StringWriter();
                printGroupingSets(new PrintWriter(stringWriter), "");
                buf.append(stringWriter.toString());
            } else {
                groupBy.toBuffer(buf, " group by ", ", ");
            }
            having.toBuffer(buf, " having ", " and ");
            orderBy.toBuffer(buf, " order by ", ", ");

            return buf.toString();
        }
    }

    /**
     * Prints this SqlQuery to a PrintWriter with each clause on a separate
     * line, and with the specified indentation prefix.
     *
     * @param pw Print writer
     * @param prefix Prefix for each line
     */
    public void print(PrintWriter pw, String prefix) {
        select.print(pw, prefix,
            distinct ? "select distinct " : "select ", ", ");
        pw.print(getGroupingFunction(prefix));
        from.print(pw, prefix, "from ", ", ");
        where.print(pw, prefix, "where ", " and ");
        if (hasGroupingSet()) {
            printGroupingSets(pw, prefix);
        } else {
            groupBy.print(pw, prefix, "group by ", ", ");
        }
        having.print(pw, prefix, "having ", " and ");
        orderBy.print(pw, prefix, "order by ", ", ");
    }

    private String getGroupingFunction(String prefix) {
        if (!hasGroupingSet()) {
            return "";
        }
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < groupingFunction.size(); i++) {
            if (generateFormattedSql) {
                buf.append("    " + prefix);
            }
            buf.append(", ");
            buf.append("grouping(");
            buf.append(groupingFunction.get(i));
            buf.append(") as ");
            dialect.quoteIdentifier("g" + i, buf);
            if (generateFormattedSql) {
                buf.append(Util.nl);
            }
        }
        return buf.toString();
    }


    private void printGroupingSets(PrintWriter pw, String prefix) {
        pw.print(" group by grouping sets (");
        for (int i = 0; i < groupingSet.size(); i++) {
            if (i > 0) {
                pw.print(",");
            }
            pw.print("(");
            groupingSet.get(i).print(pw, prefix, "", ",", "", "");
            pw.print(")");
        }
        pw.print(")");
    }

    private boolean hasGroupingSet() {
        return !groupingSet.isEmpty();
    }

    public Dialect getDialect() {
        return dialect;
    }

    public static SqlQuery newQuery(Connection jdbcConnection, String err) {
        try {
            final Dialect dialect =
                    Dialect.create(jdbcConnection.getMetaData());
            return new SqlQuery(dialect);
        } catch (SQLException e) {
            throw Util.newInternal(e, err);
        }
    }

    public static SqlQuery newQuery(DataSource dataSource, String err) {
        Connection jdbcConnection = null;
        try {
            jdbcConnection = dataSource.getConnection();
            final Dialect dialect =
                    Dialect.create(jdbcConnection.getMetaData());
            return new SqlQuery(dialect);
        } catch (SQLException e) {
            throw Util.newInternal(e, err);
        } finally {
            if (jdbcConnection != null) {
                try {
                    jdbcConnection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    public void addGroupingSet(List<String> groupingColumnsExpr) {
        ClauseList groupingList = new ClauseList(false);
        for (String columnExp : groupingColumnsExpr) {
            groupingList.add(columnExp);
        }
        groupingSet.add(groupingList);
    }

    public void addGroupingFunction(String columnExpr) {
        groupingFunction.add(columnExpr);
    }

    static class ClauseList extends ArrayList<String> {
        private final boolean allowDups;

        ClauseList(final boolean allowDups) {
            this.allowDups = allowDups;
        }


        /**
         * Adds an element to this ClauseList if either duplicates are allowed
         * or if it has not already been added.
         *
         * @param element Element to add
         * @return whether element was added, per
         * {@link java.util.Collection#add(Object)}
         */
        public boolean add(final String element) {
            if (allowDups || !contains(element)) {
                return super.add(element);
            }
            return false;
        }

        void toBuffer(final StringBuilder buf,
                      final String first,
                      final String sep) {
            boolean firstTime = true;
            for (String s : this) {
                if (firstTime) {
                    buf.append(first);
                    firstTime = false;
                } else {
                    buf.append(sep);
                }
                buf.append(s);
            }
        }

        void print(final PrintWriter pw,
                   final String prefix,
                   final String first,
                   final String sep) {
            print(pw, prefix, first, sep, "", "");
        }

        void print(final PrintWriter pw,
                   final String prefix,
                   final String first,
                   final String sep,
                   final String suffix,
                   final String last) {
            String subprefix = prefix + "    ";
            boolean firstTime = true;
            for (String s : this) {
                if (firstTime) {
                    if (generateFormattedSql) {
                        pw.print(prefix);
                    }
                    pw.print(first);
                    firstTime = false;
                } else {
                    pw.print(sep);
                }
                if (generateFormattedSql) {
                    pw.println();
                    pw.print(subprefix);
                }
                pw.print(s);
                if (generateFormattedSql) {
                    pw.print(suffix);
                }
            }
            pw.print(last);
            if (!firstTime && generateFormattedSql) {
                pw.println();
            }
        }
    }

    /**
     * Description of a SQL dialect. It is immutable.
     */
    public static class Dialect {
        private final String quoteIdentifierString;
        private final String productName;
        private final String productVersion;
        private final Set<List<Integer>> supportedResultSetTypes;
        private final boolean readOnly;

        private static final int[] RESULT_SET_TYPE_VALUES = {
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.TYPE_SCROLL_SENSITIVE};
        private static final int[] CONCURRENCY_VALUES = {
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CONCUR_UPDATABLE};

        Dialect(
            String quoteIdentifierString,
            String productName,
            String productVersion,
            Set<List<Integer>> supportedResultSetTypes,
            boolean readOnly)
        {
            this.quoteIdentifierString = quoteIdentifierString;
            this.productName = productName;
            this.productVersion = productVersion;
            this.supportedResultSetTypes = supportedResultSetTypes;
            this.readOnly = readOnly;
        }

        /**
         * Creates a {@link SqlQuery.Dialect} from a {@link DatabaseMetaData}.
         */
        public static Dialect create(final DatabaseMetaData databaseMetaData) {
            String productName;
            try {
                productName = databaseMetaData.getDatabaseProductName();
            } catch (SQLException e1) {
                throw Util.newInternal(e1, "while detecting database product");
            }

            String quoteIdentifierString;
            try {
                quoteIdentifierString =
                        databaseMetaData.getIdentifierQuoteString();
            } catch (SQLException e) {
                throw Util.newInternal(e, "while quoting identifier");
            }

            if ((quoteIdentifierString == null) ||
                    (quoteIdentifierString.trim().length() == 0)) {
                if (productName.toUpperCase().equals("MYSQL")) {
                    // mm.mysql.2.0.4 driver lies. We know better.
                    quoteIdentifierString = "`";
                } else {
                    // Quoting not supported
                    quoteIdentifierString = null;
                }
            }

            String productVersion;
            try {
                productVersion = databaseMetaData.getDatabaseProductVersion();
            } catch (SQLException e11) {
                throw Util.newInternal(e11,
                        "while detecting database product version");
            }

            Set<List<Integer>> supports = new HashSet<List<Integer>>();
            try {
                for (int type : RESULT_SET_TYPE_VALUES) {
                    for (int concurrency : CONCURRENCY_VALUES) {
                        if (databaseMetaData.supportsResultSetConcurrency(
                                type, concurrency)) {
                            String driverName =
                                databaseMetaData.getDriverName();
                            if (type != ResultSet.TYPE_FORWARD_ONLY &&
                                driverName.equals(
                                    "JDBC-ODBC Bridge (odbcjt32.dll)"))
                            {
                                // In JDK 1.6, the Jdbc-Odbc bridge announces
                                // that it can handle TYPE_SCROLL_INSENSITIVE
                                // but it does so by generating a 'COUNT(*)'
                                // query, and this query is invalid if the query
                                // contains a single-quote. So, override the
                                // driver.
                                continue;
                            }
                            supports.add(
                                new ArrayList<Integer>(
                                    Arrays.asList(type, concurrency)));
                        }
                    }
                }
            } catch (SQLException e11) {
                throw Util.newInternal(e11,
                    "while detecting result set concurrency");
            }

            final boolean readOnly;
            try {
                readOnly = databaseMetaData.isReadOnly();
            } catch (SQLException e) {
                throw Util.newInternal(e,
                    "while detecting isReadOnly");
            }

            return new Dialect(
                quoteIdentifierString,
                productName,
                productVersion,
                supports,
                readOnly);
        }

        /**
         * Creates a {@link SqlQuery.Dialect} from a
         * {@link javax.sql.DataSource}.
         *
         * <p>NOTE: This method is not cheap. The implementation gets a
         * connection from the connection pool.
         *
         * @return Dialect
         */
        public static Dialect create(DataSource dataSource) {
            Connection conn = null;
            try {
                conn = dataSource.getConnection();
                return create(conn.getMetaData());
            } catch (SQLException e) {
                throw Util.newInternal(
                    e, "Error while creating SQL dialect");
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    // ignore
                }
            }
        }

        // -- detect various databases --

        public boolean isAccess() {
            return productName.equals("ACCESS");
        }

        public boolean isDerby() {
            return productName.trim().toUpperCase().equals("APACHE DERBY");
        }

        public boolean isCloudscape() {
            return productName.trim().toUpperCase().equals("DBMS:CLOUDSCAPE");
        }

        public boolean isDB2() {
            // DB2 on NT returns "DB2/NT"
            return productName.startsWith("DB2");
        }

        public boolean isAS400() {
            // DB2/AS400 Product String = "DB2 UDB for AS/400"
            return productName.startsWith("DB2 UDB for AS/400");
        }

        public boolean isOldAS400() {
            if (!isAS400()) {
                return false;
            }
            // TB "04.03.0000 V4R3m0"
            //  this version cannot handle subqueries and is considered "old"
            // DEUKA "05.01.0000 V5R1m0" is ok
            String[] version_release = productVersion.split("\\.", 3);
            /*
            if ( version_release.length > 2 &&
                "04".compareTo(version_release[0]) > 0 ||
                ("04".compareTo(version_release[0]) == 0
                && "03".compareTo(version_release[1]) >= 0) )
                return true;
            */
            // assume, that version <= 04 is "old"
            return ("04".compareTo(version_release[0]) >= 0);
        }

        // Note: its not clear that caching the best name would actually save
        // very much time, so we do not do so.
        private String getBestName() {
            String best;
            if (isOracle()) {
                best = "oracle";
            } else if (isMSSQL()) {
                best = "mssql";
            } else if (isMySQL()) {
                best = "mysql";
            } else if (isAccess()) {
                best = "access";
            } else if (isPostgres()) {
                best = "postgres";
            } else if (isSybase()) {
                best = "sybase";
            } else if (isCloudscape() || isDerby()) {
                best = "derby";
            } else if (isDB2()) {
                best = "db2";
            } else if (isFirebird()) {
                best = "firebird";
            } else if (isInterbase()) {
                best = "interbase";
            } else if (isIngres()) {
                best = "ingres";
            } else if (isLucidDB()) {
                best = "luciddb";
            } else if (isTeradata()) {
                best = "teradata";
            } else {
                best = "generic";
            }
            return best;
        }

        /**
         * @return SQL syntax that converts <code>expr</code>
         * into upper case.
         */
        public String toUpper(String expr) {
            if (isDB2() || isAccess())
                return "UCASE(" + expr + ")";
            return "UPPER(" + expr + ")";
        }

        public String caseWhenElse(String cond, String thenExpr, String elseExpr) {
            if (isAccess()) {
                return "IIF(" + cond + "," + thenExpr + "," + elseExpr + ")";
            }
            return "CASE WHEN " + cond + " THEN " + thenExpr + " ELSE " + elseExpr + " END";
        }

        /**
         * Encloses an identifier in quotation marks appropriate for the
         * current SQL dialect. For example,
         * <code>quoteIdentifier("emp")</code> yields a string containing
         * <code>"emp"</code> in Oracle, and a string containing
         * <code>[emp]</code> in Access.
         */
        public String quoteIdentifier(final String val) {
            int size = val.length() + SINGLE_QUOTE_SIZE;
            StringBuilder buf = new StringBuilder(size);

            quoteIdentifier(val, buf);

            return buf.toString();
        }

        /**
         * Appends to a buffer an identifier, quoted appropriately for this
         * Dialect.
         *
         * @param val identifier to quote (must not be null).
         * @param buf Buffer
         */
        public void quoteIdentifier(final String val, final StringBuilder buf) {
            String q = getQuoteIdentifierString();
            if (q == null) {
                // quoting is not supported
                buf.append(val);
                return;
            }
            // if the value is already quoted, do nothing
            //  if not, then check for a dot qualified expression
            //  like "owner.table".
            //  In that case, prefix the single parts separately.
            if (val.startsWith(q) && val.endsWith(q)) {
                // already quoted - nothing to do
                buf.append(val);
                return;
            }

            int k = val.indexOf('.');
            if (k > 0) {
                // qualified
                String val1 = Util.replace(val.substring(0,k), q, q + q);
                String val2 = Util.replace(val.substring(k+1), q, q + q);
                buf.append(q);
                buf.append(val1);
                buf.append(q);
                buf.append(".");
                buf.append(q);
                buf.append(val2);
                buf.append(q);

            } else {
                // not Qualified
                String val2 = Util.replace(val, q, q + q);
                buf.append(q);
                buf.append(val2);
                buf.append(q);
            }
        }

        /**
         * Encloses an identifier in quotation marks appropriate for the
         * current SQL dialect. For example, in Oracle, where the identifiers
         * are quoted using double-quotes,
         * <code>quoteIdentifier("schema","table")</code> yields a string
         * containing <code>"schema"."table"</code>.
         *
         * @param qual Qualifier. If it is not null,
         *             <code>"<em>qual</em>".</code> is prepended.
         * @param name Name to be quoted.
         */
        public String quoteIdentifier(final String qual, final String name) {
            // We know if the qalifier is null, then only the name is going
            // to be quoted.
            int size = name.length()
                + ((qual == null)
                    ? SINGLE_QUOTE_SIZE
                    : (qual.length() + DOUBLE_QUOTE_SIZE));
            StringBuilder buf = new StringBuilder(size);

            quoteIdentifier(qual, name, buf);

            return buf.toString();
        }

        /**
         * Appends to a buffer an identifier and optional qualifier, quoted
         * appropriately for this Dialect.
         *
         * @param qual optional qualifier to be quoted.
         * @param name name to be quoted (must not be null).
         * @param buf Buffer
         */
        public void quoteIdentifier(
            final String qual,
            final String name,
            final StringBuilder buf)
        {
            if (qual == null) {
                quoteIdentifier(name, buf);

            } else {
                Util.assertTrue(
                    (qual.length() != 0),
                    "qual should probably be null, not empty");

                quoteIdentifier(qual, buf);
                buf.append('.');
                quoteIdentifier(name, buf);
            }
        }

        /**
         * Returns the character which is used to quote identifiers, or null
         * if quoting is not supported.
         */
        public String getQuoteIdentifierString() {
            return quoteIdentifierString;
        }

        /**
         * Appends to a buffer a single-quoted SQL string.
         *
         * <p>For example, in the default dialect,
         * <code>quoteStringLiteral(buf, "Can't")</code> appends
         * "<code>'Can''t'</code>" to <code>buf</code>.
         */
        public void quoteStringLiteral(StringBuilder buf, String s) {
            Util.singleQuoteString(s, buf);
        }

        /**
         * Appends to a buffer a numeric literal.
         *
         * <p>In the default dialect, numeric literals are printed as is.
         */
        public void quoteNumericLiteral(StringBuilder buf, String value) {
            buf.append(value);
        }

        /**
         * Appends to a buffer a boolean literal.
         *
         * <p>In the default dialect, boolean literals are printed as is.
         */
        public void quoteBooleanLiteral(StringBuilder buf, String value) {
            // NOTE jvs 1-Jan-2007:  See quoteDateLiteral for explanation.
            // In addition, note that we leave out UNKNOWN (even though
            // it is a valid SQL:2003 literal) because it's really
            // NULL in disguise, and NULL is always treated specially.
            if (!value.equalsIgnoreCase("TRUE")
                && !(value.equalsIgnoreCase("FALSE"))) {
                throw new NumberFormatException(
                    "Illegal BOOLEAN literal:  " + value);
            }
            buf.append(value);
        }

        /**
         * Appends to a buffer a date literal.
         *
         * <p>For example, in the default dialect,
         * <code>quoteStringLiteral(buf, "1969-03-17")</code>
         * appends <code>DATE '1969-03-17'</code>.
         */
        public void quoteDateLiteral(StringBuilder buf, String value) {
            // NOTE jvs 1-Jan-2007: Check that the supplied literal is in valid
            // SQL:2003 date format.  A hack in
            // RolapSchemaReader.lookupMemberChildByName looks for
            // NumberFormatException to suppress it, so that is why
            // we convert the exception here.
            try {
                java.sql.Date.valueOf(value);
            } catch (IllegalArgumentException ex) {
                throw new NumberFormatException(
                    "Illegal DATE literal:  " + value);
            }
            buf.append("DATE ");
            Util.singleQuoteString(value, buf);
        }

        /**
         * Appends to a buffer a time literal.
         *
         * <p>For example, in the default dialect,
         * <code>quoteStringLiteral(buf, "12:34:56")</code>
         * appends <code>TIME '12:34:56'</code>.
         */
        public void quoteTimeLiteral(StringBuilder buf, String value) {
            // NOTE jvs 1-Jan-2007:  See quoteDateLiteral for explanation.
            try {
                java.sql.Time.valueOf(value);
            } catch (IllegalArgumentException ex) {
                throw new NumberFormatException(
                    "Illegal TIME literal:  " + value);
            }
            buf.append("TIME ");
            Util.singleQuoteString(value, buf);
        }

        /**
         * Appends to a buffer a timestamp literal.
         *
         * <p>For example, in the default dialect,
         * <code>quoteStringLiteral(buf, "1969-03-17 12:34:56")</code>
         * appends <code>TIMESTAMP '1969-03-17 12:34:56'</code>.
         */
        public void quoteTimestampLiteral(StringBuilder buf, String value) {
            // NOTE jvs 1-Jan-2007:  See quoteTimestampLiteral for explanation.
            try {
                java.sql.Timestamp.valueOf(value);
            } catch (IllegalArgumentException ex) {
                throw new NumberFormatException(
                    "Illegal TIMESTAMP literal:  " + value);
            }
            buf.append("TIMESTAMP ");
            Util.singleQuoteString(value, buf);
        }

        /**
         * Returns whether the underlying database is Firebird.
         */
        public boolean isFirebird() {
            return productName.toUpperCase().indexOf("FIREBIRD") >= 0;
        }

        /**
         * Returns whether the underlying database is Informix.
         */
        public boolean isInformix() {
            return productName.startsWith("Informix");
        }

        /**
         * Returns whether the underlying database is Ingres.
         */
        public boolean isIngres() {
            return productName.toUpperCase().equals("INGRES");
        }

        /**
         * Returns whether the underlying database is Interbase.
         */
        public boolean isInterbase() {
            return productName.equals("Interbase");
        }

        /**
         * Returns whether the underlying database is LucidDB.
         */
        public boolean isLucidDB() {
            return productName.toUpperCase().equals("LUCIDDB");
        }

        /**
         * Returns whether the underlying database is Microsoft SQL Server.
         */
        public boolean isMSSQL() {
            return productName.toUpperCase().indexOf("SQL SERVER") >= 0;
        }

        /**
         * Returns whether the underlying database is Oracle.
         */
        public boolean isOracle() {
            return productName.equals("Oracle");
        }

        /**
         * Returns whether the underlying database is Postgres.
         */
        public boolean isPostgres() {
            return productName.toUpperCase().indexOf("POSTGRE") >= 0;
        }

        /**
         * Returns whether the underlying database is MySQL.
         */
        public boolean isMySQL() {
            return productName.toUpperCase().equals("MYSQL");
        }

        /**
         * Returns whether the underlying database is Sybase.
         */
        public boolean isSybase() {
            return productName.toUpperCase().indexOf("SYBASE") >= 0;
        }

        /**
         * Returns whether the underlying database is Teradata.
         */
        public boolean isTeradata() {
            return productName.toUpperCase().indexOf("TERADATA") >= 0;
        }

        /**
         * Returns whether this Dialect requires subqueries in the FROM clause
         * to have an alias.
         *
         * @see #allowsFromQuery()
         */
        public boolean requiresAliasForFromQuery() {
            return isMySQL() ||
                isDerby() ||
                isPostgres();
        }

        /**
         * Returns whether the SQL dialect allows "AS" in the FROM clause.
         * If so, "SELECT * FROM t AS alias" is a valid query.
         */
        protected boolean allowsAs() {
            return !isOracle() && !isSybase() && !isFirebird() &&
                !isInterbase();
        }

        /**
         * Returns whether this Dialect allows a subquery in the from clause,
         * for example
         *
         * <blockquote><code>SELECT * FROM (SELECT * FROM t) AS x</code></blockquote>
         *
         * @see #requiresAliasForFromQuery()
         */
        public boolean allowsFromQuery() {
            // Older versions of AS400 and MySQL before 4.0 do not allow FROM
            // subqueries in the FROM clause.
            return !(isMySQL() && productVersion.compareTo("4.") < 0)
                && !isOldAS400() && !isInformix()
                && !isSybase() && !isInterbase();
        }

        /**
         * Returns whether this Dialect allows multiple arguments to the
         * <code>COUNT(DISTINCT ...) aggregate function, for example
         *
         * <blockquote><code>SELECT COUNT(DISTINCT x, y) FROM t</code></blockquote>
         *
         * @see #allowsCountDistinct()
         * @see #allowsMultipleCountDistinct()
         */
        public boolean allowsCompoundCountDistinct() {
            return isMySQL();
        }

        /**
         * Returns whether this Dialect supports distinct aggregations.
         *
         * <p>For example, Access does not allow
         * <blockquote>
         * <code>select count(distinct x) from t</code>
         * </blockquote>
         */
        public boolean allowsCountDistinct() {
            return !isAccess();
        }

        /**
         * Returns whether this Dialect supports more than one distinct
         * aggregation in the same query.
         *
         * <p>In Derby 10.1,
         * <blockquote>
         *   <code>select couunt(distinct x) from t</code>
         * </blockquote>
         * is OK, but
         * <blockquote>
         *   <code>select couunt(distinct x), count(distinct y) from t</code>
         * </blockquote>
         * gives "Multiple DISTINCT aggregates are not supported at this time."
         *
         * @return whether this Dialect supports more than one distinct
         * aggregation in the same query
         */
        public boolean allowsMultipleCountDistinct() {
            return allowsCountDistinct() &&
                !isDerby();
        }

        /**
         * Returns whether this Dialect has performant support of distinct SQL
         * measures in the same query.
         * 
         * @return whether this dialect supports multiple count(distinct subquery)
         * measures in one query.
         */
        public boolean allowsMultipleDistinctSqlMeasures() {
            return allowsMultipleCountDistinct() && !isLucidDB();
        }
        
        /**
         * Generates a SQL statement to represent an inline dataset.
         *
         * <p>For example, for Oracle, generates
         *
         * <pre>
         * SELECT 1 AS FOO, 'a' AS BAR FROM dual
         * UNION ALL
         * SELECT 2 AS FOO, 'b' AS BAR FROM dual
         * </pre>
         *
         * <p>For ANSI SQL, generates:
         *
         * <pre>
         * VALUES (1, 'a'), (2, 'b')
         * </pre>
         *
         * @param columnNames List of column names
         * @param columnTypes List of column types ("String" or "Numeric")
         * @param valueList List of rows values
         * @return SQL string
         */
        public String generateInline(
            List<String> columnNames,
            List<String> columnTypes,
            List<String[]> valueList)
        {
            if (isOracle()) {
                return generateInlineGeneric(
                    columnNames, columnTypes, valueList,
                    "from dual");
            } else if (isAccess()) {
                // Fall back to using the FoodMart 'days' table, because
                // Access SQL has no way to generate values not from a table.
                return generateInlineGeneric(
                    columnNames, columnTypes, valueList,
                    " from `days` where `day` = 1");
            } else if (isMySQL() || isIngres()) {
                return generateInlineGeneric(
                        columnNames, columnTypes, valueList, null);
            } else if (isLucidDB()) {
                // TODO jvs 26-Nov-2006:  Eliminate this once LucidDB
                // can support applying column names to a VALUES clause
                // (needed by generateInlineForAnsi).
                return generateInlineGeneric(
                    columnNames, columnTypes, valueList,
                    " from (values(0))");
            } else {
                return generateInlineForAnsi(
                    "t", columnNames, columnTypes, valueList);
            }
        }

        /**
          * Generic algorithm to generate inline values list,
          * using an optional FROM clause, specified by the caller of this
          * method, appropriate to the dialect of SQL.
          */
        private String generateInlineGeneric(
                List<String> columnNames,
                List<String> columnTypes,
                List<String[]> valueList,
                String fromClause) {
            final StringBuilder buf = new StringBuilder();
            for (int i = 0; i < valueList.size(); i++) {
                if (i > 0) {
                    buf.append(" union all ");
                }
                String[] values = valueList.get(i);
                buf.append("select ");
                for (int j = 0; j < values.length; j++) {
                    String value = values[j];
                    if (j > 0) {
                        buf.append(", ");
                    }
                    final String columnType = columnTypes.get(j);
                    final String columnName = columnNames.get(j);
                    Datatype datatype = Datatype.valueOf(columnType);
                    quote(buf, value, datatype);
                    if (allowsAs()) {
                        buf.append(" as ");
                    } else {
                        buf.append(' ');
                    }
                    quoteIdentifier(columnName, buf);
                }
                if (fromClause != null) {
                    buf.append(fromClause);
                }
            }
            return buf.toString();
        }

        /**
         * Generates inline values list using ANSI 'VALUES' syntax.
         * For example,
         *
         * <blockquote><code>SELECT * FROM
         *   (VALUES (1, 'a'), (2, 'b')) AS t(x, y)</code></blockquote>
         *
         * <p>If NULL values are present, we use a CAST to ensure that they
         * have the same type as other columns:
         *
         * <blockquote><code>SELECT * FROM
         * (VALUES (1, 'a'), (2, CASE(NULL AS VARCHAR(1)))) AS t(x, y)
         * </code></blockquote>
         *
         * <p>This syntax is known to work on Derby, but not Oracle 10 or
         * Access.
         */
        private String generateInlineForAnsi(
                String alias,
                List<String> columnNames,
                List<String> columnTypes,
                List<String[]> valueList) {
            final StringBuilder buf = new StringBuilder();
            buf.append("SELECT * FROM (VALUES ");
            // Derby pads out strings to a common length, so we cast the
            // string values to avoid this.  Determine the cast type for each
            // column.
            String[] castTypes = null;
            if (isDerby()) {
                castTypes = new String[columnNames.size()];
                for (int i = 0; i < columnNames.size(); i++) {
                    String columnType = columnTypes.get(i);
                    if (columnType.equals("String")) {
                        castTypes[i] =
                            guessSqlType(columnType, valueList, i);
                    }
                }
            }
            for (int i = 0; i < valueList.size(); i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                String[] values = valueList.get(i);
                buf.append("(");
                for (int j = 0; j < values.length; j++) {
                    String value = values[j];
                    if (j > 0) {
                        buf.append(", ");
                    }
                    final String columnType = columnTypes.get(j);
                    Datatype datatype = Datatype.valueOf(columnType);
                    if (value == null) {
                        String sqlType =
                            guessSqlType(columnType, valueList, j);
                        buf.append("CAST(NULL AS ")
                            .append(sqlType)
                            .append(")");
                    } else if (isDerby() && castTypes[j] != null) {
                        buf.append("CAST(");
                        quote(buf, value, datatype);
                        buf.append(" AS ")
                            .append(castTypes[j])
                            .append(")");
                    } else {
                        quote(buf, value, datatype);
                    }
                }
                buf.append(")");
            }
            buf.append(") AS ");
            quoteIdentifier(alias, buf);
            buf.append(" (");
            for (int j = 0; j < columnNames.size(); j++) {
                final String columnName = columnNames.get(j);
                if (j > 0) {
                    buf.append(", ");
                }
                quoteIdentifier(columnName, buf);
            }
            buf.append(")");
            return buf.toString();
        }

        /**
         * If Double values need to include additional exponent in its string
         * represenation. This is to make sure that Double literals will be
         * interpreted as doubles by LucidDB.
         * 
         * @param value Double value to generate string for
         * @param valueString java string representation for this value.
         * @return whether an additional exponent "E0" needs to be appended
         * 
         */
        private boolean needsExponent(Object value, String valueString) {
            if (isLucidDB() && 
                value instanceof Double &&
                !valueString.contains("E")) {
                return true;
            }
            return false;
        }
        
        /**
         * Appends to a buffer a value quoted for its type.
         */
        public void quote(StringBuilder buf, Object value, Datatype datatype) {
            if (value == null) {
                buf.append("null");
            } else {
                String valueString = value.toString();
                if (needsExponent(value, valueString)) {
                    valueString += "E0";
                }
                datatype.quoteValue(buf, this, valueString);
            }
        }

        /**
         * Guesses the type of a column based upon (a) its basic type,
         * (b) a list of values.
         */
        private static String guessSqlType(
                String basicType, List<String[]> valueList, int column) {
            if (basicType.equals("String")) {
                int maxLen = 1;
                for (String[] values : valueList) {
                    final String value = values[column];
                    if (value == null) {
                        continue;
                    }
                    maxLen = Math.max(maxLen, value.length());
                }
                return "VARCHAR(" + maxLen + ")";
            } else {
                return "INTEGER";
            }
        }

        /**
         * Returns whether this dialect supports common SQL Data Definition
         * Language (DDL) statements such as <code>CREATE TABLE</code> and
         * <code>DROP INDEX</code>.
         *
         * <p>Access seems to allow DDL iff the .mdb file is writeable.
         *
         * @see java.sql.DatabaseMetaData#isReadOnly()
         */
        public boolean allowsDdl() {
            return !readOnly;
        }

        /**
         * Returns whether NULL values appear last when sorted using ORDER BY.
         * According to the SQL standard, this is implementation-specific.
         */
        public boolean isNullsCollateLast() {
            if (isMySQL()) {
                return false;
            }
            return true;
        }

        /**
         * Modifies an expression in the ORDER BY clause to ensure that NULL
         * values collate after all non-NULL values.
         * If {@link #isNullsCollateLast()} is true, there's nothing to do.
         */
        public String forceNullsCollateLast(String expr) {
            // If we need to support other DBMSes, note that the SQL standard
            // provides the syntax 'ORDER BY x ASC NULLS LAST'.
            if (isMySQL()) {
                String addIsNull = "ISNULL(" + expr + "), ";
                expr = addIsNull + expr;
            }
            return expr;
        }

        /**
         * Returns whether this Dialect supports expressions in the GROUP BY
         * clause. Derby/Cloudscape do not.
         *
         * @return Whether this Dialect allows expressions in the GROUP BY
         *   clause
         */
        public boolean supportsGroupByExpressions() {
            return !(isDerby() || isCloudscape());
        }

        /**
         * Returns whether this Dialect allows the GROUPING SETS construct in
         * the GROUP BY clause. Currently Oracle, DB2 and Teradata.
         *
         * @return Whether this Dialect allows GROUPING SETS clause
         */
        public boolean supportsGroupingSets() {
            return isOracle() || isDB2() || isTeradata();
        }

        /**
         * Returns true if this Dialect can include expressions in the ORDER BY
         * clause only by adding an expression to the SELECT clause and using
         * its alias.
         *
         * <p>For example, in such a dialect,
         * <blockquote>
         * <code>SELECT x FROM t ORDER BY x + y</code>
         * </blockquote>
         * would be illegal, but
         * <blockquote>
         * <code>SELECT x, x + y AS z FROM t ORDER BY z</code>
         * </blockquote>
         *
         * would be legal.</p>
         *
         * <p>MySQL, DB2 and Ingres are examples of such dialects.</p>
         *
         * @return Whether this Dialect can include expressions in the ORDER BY
         *   clause only by adding an expression to the SELECT clause and using
         *   its alias
         */
        public boolean requiresOrderByAlias() {
            return isMySQL() || isDB2() || isIngres();
        }

        /**
         * Returns true if aliases defined in the SELECT clause can be used as
         * expressions in the ORDER BY clause.
         *
         * <p>For example, in such a dialect,
         * <blockquote>
         * <code>SELECT x, x + y AS z FROM t ORDER BY z</code>
         * </blockquote>
         *
         * would be legal.</p>
         *
         * <p>MySQL, DB2 and Ingres are examples of dialects where this is true;
         * Access is a dialect where this is false.</p>
         *
         * @return Whether aliases defined in the SELECT clause can be used as
         * expressions in the ORDER BY clause.
         */
        public boolean allowsOrderByAlias() {
            return requiresOrderByAlias();
        }

        /**
         * Returns true if this dialect supports multi-value IN expressions.
         * E.g.,
         *
         * <code>WHERE (col1, col2) IN ((val1a, val2a), (val1b, val2b))</code>
         *
         * @return true if the dialect supports multi-value IN expressions
         */
        public boolean supportsMultiValueInExpr() {
            return isLucidDB() || isMySQL();
        }

        /**
         * Returns whether this Dialect supports the given concurrency type
         * in combination with the given result set type.
         *
         * <p>The result is similar to
         * {@link java.sql.DatabaseMetaData#supportsResultSetConcurrency(int, int)},
         * except that the JdbcOdbc bridge in JDK 1.6 overstates its abilities.
         * See bug 1690406.
         *
         * @param type defined in {@link ResultSet}
         * @param concurrency type defined in {@link ResultSet}
         * @return <code>true</code> if so; <code>false</code> otherwise
         * @exception SQLException if a database access error occurs
         */
        public boolean supportsResultSetConcurrency(
            int type,
            int concurrency) {
            return supportedResultSetTypes.contains(
                Arrays.asList(type, concurrency));
        }


        public String toString() {
            return productName;
        }
    }

    /**
     * Datatype of a column.
     */
    public enum Datatype {
        String {
            public void quoteValue(StringBuilder buf, SqlQuery.Dialect dialect, String value) {
                dialect.quoteStringLiteral(buf, value);
            }
        },

        Numeric {
            public void quoteValue(StringBuilder buf, Dialect dialect, String value) {
                dialect.quoteNumericLiteral(buf, value);
            }

            public boolean isNumeric() {
                return true;
            }
        },

        Integer {
            public void quoteValue(StringBuilder buf, Dialect dialect, String value) {
                dialect.quoteNumericLiteral(buf, value);
            }

            public boolean isNumeric() {
                return true;
            }
        },

        Boolean {
            public void quoteValue(StringBuilder buf, Dialect dialect, String value) {
                dialect.quoteBooleanLiteral(buf, value);
            }
        },

        Date {
            public void quoteValue(StringBuilder buf, Dialect dialect, String value) {
                dialect.quoteDateLiteral(buf, value);
            }
        },

        Time {
            public void quoteValue(StringBuilder buf, Dialect dialect, String value) {
                dialect.quoteTimeLiteral(buf, value);
            }
        },

        Timestamp {
            public void quoteValue(StringBuilder buf, Dialect dialect, String value) {
                dialect.quoteTimestampLiteral(buf, value);
            }
        };

        /**
         * Appends to a buffer a value of this type, in the appropriate format
         * for this dialect.
         *
         * @param buf Buffer
         * @param dialect Dialect
         * @param value Value
         */
        public abstract void quoteValue(
            StringBuilder buf,
            Dialect dialect,
            String value);

        public boolean isNumeric() {
            return false;
        }
    }

    /**
     * Collection of alternative code for alternative dialects.
     */
    public static class CodeSet {
        private final Map<String, String> dialectCodes =
            new HashMap<String, String>();

        public String put(String dialect, String code) {
            return dialectCodes.put(dialect, code);
        }

        /**
         * Chooses the code variant which best matches the given Dialect.
         */
        public String chooseQuery(Dialect dialect) {
            String best = dialect.getBestName();
            String bestCode = dialectCodes.get(best);
            if (bestCode != null) {
                return bestCode;
            }
            String genericCode = dialectCodes.get("generic");
            if (genericCode == null) {
                throw Util.newError("View has no 'generic' variant");
            }
            return genericCode;
        }
    }
}

// End SqlQuery.java
