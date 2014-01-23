/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
//
// jhyde, Mar 21, 2002
*/
package mondrian.rolap.sql;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.spi.Dialect;
import mondrian.util.Pair;

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
 */
public class SqlQuery {
    /** Controls the formatting of the sql string. */
    private final boolean generateFormattedSql;

    private boolean distinct;

    private final ClauseList select;
    private final FromClauseList from;
    private final ClauseList where;
    private final ClauseList groupBy;
    private final ClauseList having;
    private final ClauseList orderBy;
    private final List<ClauseList> groupingSets;
    private final ClauseList groupingFunctions;

    private final List<SqlStatement.Type> types =
        new ArrayList<SqlStatement.Type>();

    /** Controls whether table optimization hints are used */
    private boolean allowHints;

    /**
     * This list is used to keep track of what aliases have been  used in the
     * FROM clause. One might think that a java.util.Set would be a more
     * appropriate Collection type, but if you only have a couple of "from
     * aliases", then iterating over a list is faster than doing a hash lookup
     * (as is used in java.util.HashSet).
     */
    private final List<String> fromAliases;

    private int joinCount;

    /** The SQL dialect this query is to be generated in. */
    private final Dialect dialect;

    /** Scratch buffer. Clear it before use. */
    private final StringBuilder buf;

    private final Map<String, String> columnAliases =
        new HashMap<String, String>();

    private final List<String> columnAliases2 = new ArrayList<String>();

    private static final String INDENT = "    ";

    /**
     * Base constructor used by all other constructors to create an empty
     * instance.
     *
     * @param dialect Dialect
     * @param formatted Whether to generate SQL formatted on multiple lines
     */
    public SqlQuery(Dialect dialect, boolean formatted) {
        assert dialect != null;
        this.generateFormattedSql = formatted;

        // both select and from allow duplications
        this.select = new ClauseList(true);
        this.from = new FromClauseList(true);

        this.groupingFunctions = new ClauseList(false);
        this.where = new ClauseList(false);
        this.groupBy = new ClauseList(false);
        this.having = new ClauseList(false);
        this.orderBy = new ClauseList(false);
        this.fromAliases = new ArrayList<String>();
        this.buf = new StringBuilder(128);
        this.groupingSets = new ArrayList<ClauseList>();
        this.dialect = dialect;

        // REVIEW emcdermid 10-Jul-2009: It might be okay to allow
        // hints in all cases, but for initial implementation this
        // allows us to them on selectively in specific situations.
        // Usage will likely expand with experimentation.
        this.allowHints = false;
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

    public void setDistinct(final boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * Chooses whether table optimization hints may be used
     * (assuming the dialect supports it).
     *
     * @param t True to allow hints to be used, false otherwise
     */
    public void setAllowHints(boolean t) {
        this.allowHints = t;
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
    private boolean addFromQuery(
        final String query,
        final String alias,
        String parentAlias,
        String joinCondition,
        final boolean failIfExists)
    {
        assert alias != null;
        assert alias.length() > 0;

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
        if (dialect.allowsAs()) {
            buf.append(" as ");
        } else {
            buf.append(' ');
        }
        dialect.quoteIdentifier(alias, buf);

        if (parentAlias != null) {
            assert fromAliases.contains(parentAlias);
            assert joinCondition != null;
            if (dialect.allowsJoinOn()) {
                buf.append(" on ").append(joinCondition);
                ++joinCount;
            } else {
                where.add(joinCondition);
            }
        } else {
            assert joinCondition == null;
            assert from.isEmpty() || !dialect.allowsJoinOn();
        }

        fromAliases.add(alias);
        from.add(buf.toString());
        return true;
    }

    /**
     * Adds <code>[schema.]table AS alias</code> to the FROM clause.
     *
     * @param schema schema name; may be null
     * @param name table name
     * @param alias table alias, may not be null
     *              (if not null, must not be zero length).
     * @param filter Extra filter condition, or null
     * @param hintMap table optimization hints, if any
     * @param failIfExists Whether to throw a RuntimeException if from clause
     *   already contains this alias
     *
     * @pre alias != null
     * @return true if table was added
     */
    boolean addFromTable(
        final String schema,
        final String name,
        final String alias,
        final String filter,
        final Map<String, String> hintMap,
        final String parentAlias,
        final String joinCondition,
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
        dialect.quoteIdentifier(buf, schema, name);
        if (alias != null) {
            Util.assertTrue(alias.length() > 0);

            if (dialect.allowsAs()) {
                buf.append(" as ");
            } else {
                buf.append(' ');
            }
            dialect.quoteIdentifier(alias, buf);
        }

        if (this.allowHints) {
            dialect.appendHintsAfterFromClause(buf, hintMap);
        }

        if (parentAlias != null) {
            assert fromAliases.contains(parentAlias);
            assert joinCondition != null;
            if (dialect.allowsJoinOn()) {
                buf.append(" on ").append(joinCondition);
                ++joinCount;
            } else {
                where.add(joinCondition);
            }
        } else {
            assert joinCondition == null;
            assert from.isEmpty() || !dialect.allowsJoinOn();
        }

        fromAliases.add(alias);
        from.add(buf.toString());

        if (filter != null) {
            // append filter condition to where clause
            addWhere("(" + filter + ")");
        }
        return true;
    }

    public void addFrom(
        final SqlQuery sqlQuery,
        final String alias,
        final boolean failIfExists)
    {
        addFromQuery(sqlQuery.toString(), alias, null, null, failIfExists);
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
     * @return Whether relation was added to query
     */
    public boolean addFrom(
        final RolapSchema.PhysRelation relation,
        final String alias,
        final boolean failIfExists)
    {
        return addFrom_(relation, alias, null, null, failIfExists);
    }

    public boolean addFrom(
        RolapSchema.PhysRelation relation,
        String alias,
        String parentAlias,
        String joinCondition,
        final boolean failIfExists)
    {
        return addFrom_(
            relation, alias, parentAlias, joinCondition, failIfExists);
    }

    private boolean addFrom_(
        RolapSchema.PhysRelation relation,
        String alias,
        String parentAlias,
        String joinCondition,
        final boolean failIfExists)
    {
        Util.deprecated("alias param probably not necessary", false);
        Util.deprecated(
            "adopt visitor pattern and replace 'instanceof' below", false);
        if (relation instanceof RolapSchema.PhysView) {
            final RolapSchema.PhysView view = (RolapSchema.PhysView) relation;
            final String viewAlias =
                (alias == null)
                ? view.getAlias()
                : alias;
            final String sqlString = view.getSqlString();
            return addFromQuery(
                sqlString, viewAlias, parentAlias, joinCondition, false);

        } else if (relation instanceof RolapSchema.PhysTable) {
            final RolapSchema.PhysTable table =
                (RolapSchema.PhysTable) relation;
            final String tableAlias =
                (alias == null)
                ? table.getAlias()
                : alias;
            return addFromTable(
                table.getSchemaName(),
                table.getName(),
                tableAlias,
                /*table.getFilter()*/null,
                table.getHintMap(),
                parentAlias,
                joinCondition,
                failIfExists);
        } else if (relation instanceof RolapSchema.PhysInlineTable) {
            final RolapSchema.PhysInlineTable table =
                (RolapSchema.PhysInlineTable) relation;
            RolapSchema.PhysView physView =
                RolapUtil.convertInlineTableToRelation(table, dialect);
            return addFromQuery(
                physView.getSqlString(),
                table.getAlias(),
                parentAlias,
                joinCondition,
                failIfExists);
/*
        } else if (relation instanceof MondrianDef.Join) {
            final MondrianDef.Join join = (MondrianDef.Join) relation;
            return addJoin(
                join.left,
                join.getLeftAlias(),
                join.leftKey,
                join.right,
                join.getRightAlias(),
                join.rightKey,
                failIfExists);
*/
        } else {
            Util.deprecated("remove above commented section", false);
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Adds an expression to the select clause, automatically creating a
     * column alias.
     */
    public String addSelect(final String expression, SqlStatement.Type type) {
        return addSelect(expression, type, null);
    }

    /**
     * Adds an expression to the SELECT and GROUP BY clauses. Uses the alias in
     * the GROUP BY clause, if the dialect requires it.
     *
     * @param expression Expression
     * @return Alias of expression
     */
    public String addSelectGroupBy(
        final String expression,
        SqlStatement.Type type)
    {
        final String alias = addSelect(expression, type);
        addGroupBy(expression, alias);
        return alias;
    }

    public int getCurrentSelectListSize()
    {
        return select.size();
    }

    public String nextColumnAlias() {
        return "c" + select.size();
    }

    /**
     * Adds an expression to the select clause, with a specified type and
     * column alias.
     *
     * @param expression Expression
     * @param type Java type to be used to hold cursor column
     * @param alias Column alias (or null for no alias)
     * @return Column alias
     */
    public String addSelect(
        final String expression,
        final SqlStatement.Type type,
        String alias)
    {
        if (alias == null) {
            alias = nextColumnAlias();
        }

        // Some DB2 versions (AS/400) throw an error if a column alias is
        // *not* used in a subsequent order by (Group by).
        // Derby fails on 'SELECT... HAVING' if column has alias.
        switch (dialect.getDatabaseProduct()) {
        case DB2_AS400:
        case DERBY:
            alias = null;
        }

        buf.setLength(0);
        buf.append(expression);
        if (alias != null) {
            buf.append(" as ");
            dialect.quoteIdentifier(alias, buf);
        }

        select.add(buf.toString());
        types.add(type);
        columnAliases2.add(alias);
        columnAliases.put(expression, alias);
        return alias;
    }

    public String getAlias(String expression) {
        return columnAliases.get(expression);
    }

    public String getAlias(int i) {
        return columnAliases2.get(i);
    }

    public void addWhere(final String expression) {
        where.add(expression);
    }

    public void addGroupBy(final String expression) {
        groupBy.add(expression);
    }

    public void addGroupBy(final String expression, final String alias) {
        if (dialect.requiresGroupByAlias()) {
            addGroupBy(dialect.quoteIdentifier(alias));
        } else {
            addGroupBy(expression);
        }
    }

    public void addHaving(final String expression) {
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
        this.addOrderBy(expr, expr, ascending, prepend, nullable, true);
    }

    /**
     * Adds an item to the ORDER BY clause.
     *
     * @param expr the expr to order by
     * @param ascending sort direction
     * @param prepend whether to prepend to the current list of items
     * @param nullable whether the expression might be null
     * @param collateNullsLast whether null values should appear first or last.
     */
    public void addOrderBy(
        String expr,
        String alias,
        boolean ascending,
        boolean prepend,
        boolean nullable,
        boolean collateNullsLast)
    {
        String orderExpr =
            dialect.generateOrderItem(
                dialect.requiresOrderByAlias()
                    ? alias
                    : expr,
                nullable,
                ascending,
                collateNullsLast);
        if (prepend) {
            orderBy.add(0, orderExpr);
        } else {
            orderBy.add(orderExpr);
        }
    }

    public String toString() {
        // Don't use buf. There are problems with reentrancy, especially while
        // debugging.
        return toBuffer(new StringBuilder(), "").toString();
    }

    /**
     * Writes this SqlQuery to a StringBuilder with each clause on a separate
     * line, and with the specified indentation prefix.
     *
     * @param buf String builder
     * @param prefix Prefix for each line
     */
    public StringBuilder toBuffer(StringBuilder buf, String prefix) {
        final String first = distinct ? "select distinct " : "select ";
        select.toBuffer(buf, generateFormattedSql, prefix, first, ", ", "", "");
        groupingFunctionsToBuffer(buf, prefix);
        String fromSep = joinCount > 0 ? " join " : ", ";
        if (dialect.allowsJoinOn() && from.size() > 1) {
            if (joinCount <= 0) {
                throw new AssertionError();
            }
        }
        from.toBuffer(
            buf, generateFormattedSql, prefix, " from ", fromSep, "", "");
        where.toBuffer(
            buf, generateFormattedSql, prefix, " where ", " and ", "", "");
        if (groupingSets.isEmpty()) {
            groupBy.toBuffer(
                buf, generateFormattedSql, prefix, " group by ", ", ", "", "");
        } else {
            ClauseList.listToBuffer(
                buf,
                groupingSets,
                generateFormattedSql,
                prefix,
                " group by grouping sets (",
                ", ",
                ")");
        }
        having.toBuffer(
            buf, generateFormattedSql, prefix, " having ", " and ", "", "");
        orderBy.toBuffer(
            buf, generateFormattedSql, prefix, " order by ", ", ", "", "");
        return buf;
    }

    private void groupingFunctionsToBuffer(StringBuilder buf, String prefix) {
        if (groupingSets.isEmpty()) {
            return;
        }
        int n = 0;
        for (String groupingFunction : groupingFunctions) {
            if (generateFormattedSql) {
                buf.append(",")
                    .append(Util.nl)
                    .append(INDENT)
                    .append(prefix);
            } else {
                buf.append(", ");
            }
            buf.append("grouping(")
                .append(groupingFunction)
                .append(") as ");
            dialect.quoteIdentifier("g" + n++, buf);
        }
    }

    public Dialect getDialect() {
        return dialect;
    }

    public static SqlQuery newQuery(Dialect dialect, String err) {
        return new SqlQuery(dialect);
    }

    public void addGroupingSet(List<String> groupingColumnsExpr) {
        ClauseList groupingList = new ClauseList(false);
        for (String columnExp : groupingColumnsExpr) {
            groupingList.add(columnExp);
        }
        groupingSets.add(groupingList);
    }

    public void addGroupingFunction(String columnExpr) {
        groupingFunctions.add(columnExpr);

        // A grouping function will end up in the select clause implicitly. It
        // needs a corresponding type.
        types.add(null);
    }

    public String toSql() {
        return toString();
    }

    public Pair<String, List<SqlStatement.Type>> toSqlAndTypes() {
        assert types.size() == select.size() + groupingFunctions.size()
            : types.size() + " types, "
              + (select.size() + groupingFunctions.size())
              + " select items in query " + this;
        return Pair.of(toString(), types);
    }

    /**
     * Returns whether it is impossible for the query to ever return any rows.
     *
     * @return true if so; false otherwise
     */
    public boolean isUnsatisfiable() {
        return where.contains(RolapUtil.SQL_FALSE_LITERAL);
    }

    private static class JoinOnClause {
        private final String condition;
        private final String left;
        private final String right;

        JoinOnClause(String condition, String left, String right) {
            this.condition = condition;
            this.left = left;
            this.right = right;
        }
    }

    static class FromClauseList extends ClauseList {
        private final List<JoinOnClause> joinOnClauses =
            new ArrayList<JoinOnClause>();

        FromClauseList(boolean allowsDups) {
            super(allowsDups);
        }

        public void addOn(
            String leftAlias,
            String leftKey,
            String rightAlias,
            String rightKey,
            String condition)
        {
            if (leftAlias == null && rightAlias == null) {
                // do nothing
            } else if (leftAlias == null) {
                // left is the form of 'Table'.'column'
                leftAlias = rightAlias;
            } else if (rightAlias == null) {
                // Only left contains table name, Table.Column = abc
                // store the same name for right tables
                rightAlias = leftAlias;
            }
            joinOnClauses.add(
                new JoinOnClause(condition, leftAlias, rightAlias));
        }

        public void toBuffer(StringBuilder buf, List<String> fromAliases) {
            int n = 0;
            for (int i = 0; i < size(); i++) {
                final String s = get(i);
                final String alias = fromAliases.get(i);
                if (n++ == 0) {
                    buf.append(" from ");
                    buf.append(s);
                } else {
                    // Add "JOIN t ON (a = b ,...)" to the FROM clause. If there
                    // is no JOIN clause matching this alias (or no JOIN clauses
                    // at all), append just ", t".
                    appendJoin(fromAliases.subList(0, i), s, alias, buf);
                }
            }
        }

        void appendJoin(
            final List<String> addedTables,
            final String from,
            final String alias,
            final StringBuilder buf)
        {
            int n = 0;
            // first check when the current table is on the left side
            for (JoinOnClause joinOnClause : joinOnClauses) {
                // the first table was added before join, it has to be handled
                // specially: Table.column = expression
                if ((addedTables.size() == 1
                     && addedTables.get(0).equals(joinOnClause.left)
                     && joinOnClause.left.equals(joinOnClause.right))
                    || (alias.equals(joinOnClause.left)
                        && addedTables.contains(joinOnClause.right))
                    || (alias.equals(joinOnClause.right)
                        && addedTables.contains(joinOnClause.left)))
                {
                    if (n++ == 0) {
                        buf.append(" join ").append(from).append(" on ");
                    } else {
                        buf.append(" and ");
                    }
                    buf.append(joinOnClause.condition);
                }
            }
            if (n == 0) {
                // No "JOIN ... ON" clause matching this alias (or maybe no
                // JOIN ... ON clauses at all, if this is a database that
                // doesn't support ANSI-join syntax). Append an old-style FROM
                // item separated by a comma.
                buf.append(joinOnClauses.isEmpty() ? ", " : " cross join ")
                    .append(from);
            }
        }
    }

    public static String getBestName(Dialect dialect) {
        return dialect.getDatabaseProduct().getFamily().name().toLowerCase();
    }

    static class ClauseList extends ArrayList<String> {
        protected final boolean allowDups;

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

        final void toBuffer(
            StringBuilder buf,
            boolean generateFormattedSql,
            String prefix,
            String first,
            String sep,
            String last,
            String empty)
        {
            if (isEmpty()) {
                buf.append(empty);
                return;
            }
            first = foo(generateFormattedSql, prefix, first);
            sep = foo(generateFormattedSql, prefix, sep);
            toBuffer(buf, first, sep, last);
        }

        static String foo(
            boolean generateFormattedSql,
            String prefix,
            String s)
        {
            if (generateFormattedSql) {
                if (s.startsWith(" ")) {
                    // E.g. " and "
                    s = Util.nl + prefix + s.substring(1);
                }
                if (s.endsWith(" ")) {
                    // E.g. ", "
                    s =
                        s.substring(0, s.length() - 1) + Util.nl + prefix
                        +  INDENT;
                } else if (s.endsWith("(")) {
                    // E.g. "("
                    s = s + Util.nl + prefix + INDENT;
                }
            }
            return s;
        }

        final void toBuffer(
            final StringBuilder buf,
            final String first,
            final String sep,
            final String last)
        {
            int n = 0;
            buf.append(first);
            for (String s : this) {
                if (n++ > 0) {
                    buf.append(sep);
                }
                buf.append(s);
            }
            buf.append(last);
        }

        static void listToBuffer(
            StringBuilder buf,
            List<ClauseList> clauseListList,
            boolean generateFormattedSql,
            String prefix,
            String first,
            String sep,
            String last)
        {
            first = foo(generateFormattedSql, prefix, first);
            sep = foo(generateFormattedSql, prefix, sep);
            buf.append(first);
            int n = 0;
            for (ClauseList clauseList : clauseListList) {
                if (n++ > 0) {
                    buf.append(sep);
                }
                clauseList.toBuffer(
                    buf, false, prefix, "(", ", ", ")", "()");
            }
            buf.append(last);
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
    }

    private static class RelInfo {
        final MondrianDef.Relation relation;
        final String leftKey;
        final String leftAlias;
        final String rightKey;
        final String rightAlias;

        public RelInfo(
            MondrianDef.Relation relation,
            String leftKey,
            String leftAlias,
            String rightKey,
            String rightAlias)
        {
            this.relation = relation;
            this.leftKey = leftKey;
            this.leftAlias = leftAlias;
            this.rightKey = rightKey;
            this.rightAlias = rightAlias;
        }
    }
}

// End SqlQuery.java
