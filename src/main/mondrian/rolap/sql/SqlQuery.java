/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2008 Julian Hyde and others
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
import mondrian.spi.Dialect;
import mondrian.spi.impl.JdbcDialectImpl;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
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
    private final boolean generateFormattedSql;

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
     *
     * @param dialect Dialect
     * @param formatted Whether to generate SQL formatted on multiple lines
     */
    public SqlQuery(Dialect dialect, boolean formatted) {
        this.generateFormattedSql = formatted;

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
     * Creates a SqlQuery using a given dialect and inheriting the formatting
     * preferences from {@link MondrianProperties#GenerateFormattedSql}
     * property.
     *
     * @param dialect Dialect
     */
    public SqlQuery(Dialect dialect) {
        this(
            dialect,
            MondrianProperties.instance().GenerateFormattedSql.get());
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
     * @param filter Extra filter condition, or null
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
        dialect.quoteIdentifier(buf, schema, table);
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

                dialect.quoteIdentifier(buf, leftAlias, join.leftKey);
                buf.append(" = ");
                dialect.quoteIdentifier(buf, rightAlias, join.rightKey);

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
    public String addSelect(final String expression) {
        // Some DB2 versions (AS/400) throw an error if a column alias is
        //  *not* used in a subsequent order by (Group by).
        // Derby fails on 'SELECT... HAVING' if column has alias.
        switch (dialect.getDatabaseProduct()) {
        case DB2_AS400:
        case DERBY:
            return addSelect(expression, null);
        default:
            return addSelect(expression, nextColumnAlias());
        }
    }

    /**
     * Adds an expression to the SELECT and GROUP BY clauses. Uses the alias in
     * the GROUP BY clause, if the dialect requires it.
     *
     * @param expression Expression
     * @return Alias of expression
     */
    public String addSelectGroupBy(final String expression) {
        final String c = addSelect(expression);
        if (dialect.requiresGroupByAlias()) {
            addGroupBy(dialect.quoteIdentifier(c));
        } else {
            addGroupBy(expression);
        }
        return c;
    }

    public int getCurrentSelectListSize()
    {
        return select.size();
    }

    public String nextColumnAlias() {
        return "c" + select.size();
    }

    /**
     * Adds an expression to the select clause, with a specified column
     * alias.
     *
     * @param expression Expression
     * @param alias Column alias (or null for no alias)
     * @return Column alias
     */
    public String addSelect(final String expression, final String alias) {
        buf.setLength(0);

        buf.append(expression);
        if (alias != null) {
            buf.append(" as ");
            dialect.quoteIdentifier(alias, buf);
        }

        select.add(buf.toString());
        return alias;
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
        select.print(
            pw, generateFormattedSql, prefix,
            distinct ? "select distinct " : "select ",
            ", ");
        pw.print(getGroupingFunction(prefix));
        from.print(pw, generateFormattedSql, prefix, "from ", ", ");
        where.print(
            pw, generateFormattedSql, prefix, "where ", " and ");
        if (hasGroupingSet()) {
            printGroupingSets(pw, prefix);
        } else {
            groupBy.print(pw, generateFormattedSql, prefix, "group by ", ", ");
        }
        having.print(pw, generateFormattedSql, prefix, "having ", " and ");
        orderBy.print(pw, generateFormattedSql, prefix, "order by ", ", ");
    }

    private String getGroupingFunction(String prefix) {
        if (!hasGroupingSet()) {
            return "";
        }
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < groupingFunction.size(); i++) {
            if (generateFormattedSql) {
                buf.append("    ").append(prefix);
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
            groupingSet.get(i).print(
                pw, generateFormattedSql, prefix, "", ",", "", "");
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
                JdbcDialectImpl.create(jdbcConnection.getMetaData());
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
                JdbcDialectImpl.create(jdbcConnection.getMetaData());
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

        void print(
            final PrintWriter pw,
            boolean generateFormattedSql,
            final String prefix,
            final String first,
            final String sep)
        {
            print(pw, generateFormattedSql, prefix, first, sep, "", "");
        }

        void print(
            final PrintWriter pw,
            boolean generateFormattedSql,
            final String prefix,
            final String first,
            final String sep,
            final String suffix,
            final String last)
        {
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
            String best = getBestName(dialect);
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

        private static String getBestName(Dialect dialect) {
            return dialect.getDatabaseProduct().getFamily().name().toLowerCase();
        }
    }
}

// End SqlQuery.java
