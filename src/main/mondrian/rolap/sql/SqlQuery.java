/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 21, 2002
*/

package mondrian.rolap.sql;

import mondrian.olap.MondrianDef;
import mondrian.olap.Util;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * <code>SqlQuery</code> allows us to build a <code>select</code>
 * statement and generate it in database-specific sql syntax.
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
 **/
public class SqlQuery
{
    DatabaseMetaData databaseMetaData;
    // todo: replace {select, selectCount} with a StringList; etc.
    boolean distinct;
    private final ClauseList select = new ClauseList(true),
        from = new ClauseList(true),
        where = new ClauseList(false),
        groupBy = new ClauseList(false),
        having = new ClauseList(false),
        orderBy = new ClauseList(false);
    private final ArrayList fromAliases = new ArrayList();
    private String quoteIdentifierString = null;

    /** Scratch buffer. Clear it before use. */
    private StringBuffer buf = new StringBuffer();

    /**
     * Creates a <code>SqlQuery</code>
     *
     * @param databaseMetaData used to determine which dialect of
     *     SQL to generate
     */
    public SqlQuery(DatabaseMetaData databaseMetaData) {
        this.databaseMetaData = databaseMetaData;
        initializeQuoteIdentifierString();

    }

    /**
     * Creates an empty <code>SqlQuery</code> with the same environment as this
     * one. (As per the Gang of Four 'prototype' pattern.)
     **/
    public SqlQuery cloneEmpty()
    {
        return new SqlQuery(databaseMetaData);
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    private void initializeQuoteIdentifierString() {
        try {
            quoteIdentifierString = databaseMetaData.getIdentifierQuoteString();
        } catch (SQLException e) {
            throw Util.getRes().newInternal("while quoting identifier", e);
        }
        if (quoteIdentifierString == null || quoteIdentifierString.trim().equals("")) {
            if (isMySQL()) {
                // mm.mysql.2.0.4 driver lies. We know better.
                quoteIdentifierString = "`";
            } else {
                // Quoting not supported
                quoteIdentifierString = "";
            }
        }
    }

    /**
     * Encloses an identifier in quotation marks appropriate for the
     * current SQL dialect. For example,
     * <code>quoteIdentifier("emp")</code> yields a string containing
     * <code>"emp"</code> in Oracle, and a string containing
     * <code>[emp]</code> in Access.
     **/
    public String quoteIdentifier(String val) {
        String q = getQuoteIdentifierString();
        if (q == null || q.trim().equals("")) {
            return val; // quoting is not supported
        }
        // if the value is already quoted, do nothing
        //  if not, then check for a dot qualified expression
        //  like "owner.table".
        //  In that case, prefix the single parts separately.
        if (val.startsWith(q) && val.endsWith(q)) {
            // already quoted - nothing to do
            return val;
        }
        int k = val.indexOf('.');
        if (k > 0) {
            // qualified
            String val1 = Util.replace(val.substring(0,k), q, q + q);
            String val2 = Util.replace(val.substring(k+1), q, q + q);
            return q + val1 + q + "." +  q + val2 + q ;
        } else {
            // not Qualified
            String val2 = Util.replace(val, q, q + q);
            return q + val2 + q;
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
     **/
    public String quoteIdentifier(String qual, String name) {
        if (qual == null) {
            return quoteIdentifier(name);
        } else {
            Util.assertTrue(
                !qual.equals(""),
                "qual should probably be null, not empty");

            return quoteIdentifier(qual) +
                "." +
                quoteIdentifier(name);
        }
    }

    public String getQuoteIdentifierString() {
        return quoteIdentifierString;
    }

    // -- detect various databases --

    private String getProduct() {
        try {
            String productName = databaseMetaData.getDatabaseProductName();
            return productName;
        } catch (SQLException e) {
            throw Util.getRes().newInternal(
                    "while detecting database product", e);
        }
    }


    private String getProductVersion() {
        try {
            String version = databaseMetaData.getDatabaseProductVersion();
            return version;
        } catch (SQLException e) {
            throw Util.getRes().newInternal(
            "while detecting database product version", e);
        }
    }


    public boolean isAccess() {
        return getProduct().equals("ACCESS");
    }

    public boolean isDB2() {
        // DB2 on NT returns "DB2/NT"
        return getProduct().startsWith("DB2");
    }

    public boolean isAS400() {
        // DB2/AS400 Product String = "DB2 UDB for AS/400"
        return getProduct().startsWith("DB2 UDB for AS/400");
    }

    public boolean isOldAS400() {
        if (!isAS400()) {
            return false;
        }
        // TB "04.03.0000 V4R3m0"
        //  this version cannot handle subqueries and is considered "old"
        // DEUKA "05.01.0000 V5R1m0" is ok
        String version = getProductVersion();
        String[] version_release = version.split("\\.", 3);
        /*
        if ( version_release.length > 2 &&
            "04".compareTo(version_release[0]) > 0 ||
            ("04".compareTo(version_release[0]) == 0
            && "03".compareTo(version_release[1]) >= 0) )
            return true;
        */
        // assume, that version <= 04 is "old"
        if ("04".compareTo(version_release[0]) >= 0) {
            return true;
        }
        return false;
    }


    public boolean isInformix() {
        return getProduct().startsWith("Informix");
    }
    public boolean isMSSql() {
        return getProduct().equalsIgnoreCase("Microsoft SQL Server");
    }
    public boolean isMSSQL() {
        return getProduct().toUpperCase().indexOf("SQL SERVER") >= 0;
    }
    public boolean isOracle() {
        return getProduct().equals("Oracle");
    }
    public boolean isPostgres() {
        return getProduct().toUpperCase().indexOf("POSTGRE") >= 0;
    }
    public boolean isMySQL() {
        return getProduct().toUpperCase().equals("MYSQL");
    }
    public boolean isSybase() {
        return getProduct().toUpperCase().indexOf("SYBASE") >= 0;
    }

    // -- behaviors --
    protected boolean requiresAliasForFromItems() {
        return isPostgres();
    }
    protected boolean allowsAs() {
        return !isOracle() && !isSybase();
    }
    /** Whether "select * from (select * from t)" is OK. **/
    public boolean allowsFromQuery() {
        // older versions of AS400 do not allow FROM subqueries
        return !isMySQL() && !isOldAS400() && !isInformix() && !isSybase();
    }
    /** Whether "select count(distinct x, y) from t" is OK. **/
    public boolean allowsCompoundCountDistinct() {
        return isMySQL();
    }
    /** Whether "select count(distinct x) from t" is OK. **/
    public boolean allowsCountDistinct() {
        return !isAccess();
    }

    /**
     * Chooses the variant within an array of {@link
     * mondrian.olap.MondrianDef.SQL} which best matches the current SQL
     * dialect.
     */
    public String chooseQuery(MondrianDef.SQL[] sqls) {
        String best;
        if (isOracle()) {
            best = "oracle";
        } else if (isMSSql() || isMSSQL()) {
            best = "mssql";
        } else if (isMySQL()) {
            best = "mysql";
        } else if (isAccess()) {
            best = "access";
        } else if (isPostgres()) {
            best = "postgres";
        } else if (isSybase()) {
            best = "sybase";
        } else {
            best = "generic";
        }
        String generic = null;
        for (int i = 0; i < sqls.length; i++) {
            MondrianDef.SQL sql = sqls[i];
            if (sql.dialect.equals(best)) {
                return sql.cdata;
            }
            if (sql.dialect.equals("generic")) {
                generic = sql.cdata;
            }
        }
        if (generic == null) {
            throw Util.newError("View has no 'generic' variant");
        }
        return generic;
    }

    /**
     * @pre alias != null
     * @return true if query *was* added
     */
    public boolean addFromQuery(
            String query, String alias, boolean failIfExists) {
        Util.assertPrecondition(alias != null);
        if (fromAliases.contains(alias)) {
            if (failIfExists) {
                throw Util.newInternal(
                        "query already contains alias '" + alias + "'");
            } else {
                return false;
            }
        }
        buf.setLength(0);
        buf.append("(");
        buf.append(query);
        buf.append(")");
        if (alias != null) {
            Util.assertTrue(!alias.equals(""));
            if (allowsAs()) {
                buf.append(" as ");
            } else {
                buf.append(" ");
            }
            buf.append(quoteIdentifier(alias));
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
     *
     * @pre alias != null
     * @return true if table *was* added
     */
    private boolean addFromTable(
            String schema, String table, String alias, String filter, boolean failIfExists) {
        if (fromAliases.contains(alias)) {
            if (failIfExists) {
                throw Util.newInternal(
                        "query already contains alias '" + alias + "'");
            } else {
                return false;
            }
        }
        buf.setLength(0);
        buf.append(quoteIdentifier(schema, table));
        if (alias != null) {
            Util.assertTrue(!alias.equals(""));
            if (allowsAs()) {
                buf.append(" as ");
            } else {
                buf.append(" ");
            }
            buf.append(quoteIdentifier(alias));
            fromAliases.add(alias);
        }
        from.add(buf.toString());
        if (filter != null) {
            // append filter condition to where clause
            where.add("(" + filter + ")");
        }
        return true;
    }

    public void addFrom(SqlQuery sqlQuery, String alias, boolean failIfExists)
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
    public boolean addFrom(MondrianDef.Relation relation, String alias,
        boolean failIfExists)
    {
        if (relation instanceof MondrianDef.View) {
            MondrianDef.View view = (MondrianDef.View) relation;
            if (alias == null) {
                alias = relation.getAlias();
            }
            String sqlString = chooseQuery(view.selects);
            if (!fromAliases.contains(alias)) {
                return addFromQuery(sqlString, alias, failIfExists);
            }
            return false;
        } else if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            if (alias == null) {
                alias = relation.getAlias();
            }
            return addFromTable(table.schema, table.name, alias,
                table.getFilter(), failIfExists);
        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            boolean added = false;
            final String leftAlias = join.getLeftAlias();
            if (addFrom(join.left, leftAlias, failIfExists)) {
                added = true;
            }
            final String rightAlias = join.getRightAlias();
            if (addFrom(join.right, rightAlias, failIfExists)) {
                added = true;
            }
            if (added)
                addWhere(
                    quoteIdentifier(leftAlias, join.leftKey) +
                    " = " +
                    quoteIdentifier(rightAlias, join.rightKey));
            return added;
        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }
    /**
     * @pre alias != null
     */
    public void addJoin(
            String type, String query, String alias, String condition) {
        Util.assertPrecondition(alias != null);
        Util.assertPrecondition(condition != null);
        Util.assertTrue(!fromAliases.contains(alias));
        Util.assertTrue(from.size() > 0);
        String last = (String) from.get(from.size() - 1);
        last = last +
            " " + type + " join " + query + " as " +
            quoteIdentifier(alias) + " on " + condition;
        from.set(from.size() - 1, last);
        fromAliases.add(alias);
    }

    /**
     * Adds an expression to the select clause, automatically creating a
     * column alias.
     */
    public void addSelect(String expression) {
        // some DB2 versions (AS/400) throw an error, if a column alias is
        //  *not* used in a subsequent order by (Group by)
        if (isAS400()) {
            addSelect(expression, null);
        } else {
            addSelect(expression, "c" + select.size());
        }
    }
    /** Adds an expression to the select clause, with a specified column
     * alias. **/
    public void addSelect(String expression, String alias) {
        if (alias != null) {
            expression += " as " + quoteIdentifier(alias);
        }
        select.add(expression);
    }

    public void addWhere(String expression)
    {
        where.add(expression);
    }

    public void addGroupBy(String expression)
    {
        groupBy.add(expression);
    }

    public void addHaving(String expression)
    {
        having.add(expression);
    }

    public void addOrderBy(String expression)
    {
        orderBy.add(expression);
    }

    public String toString()
    {
        buf.setLength(0);
        select.toBuffer(buf,
                distinct ? "select distinct " : "select ", ", ");
        from.toBuffer(buf, " from ", ", ");
        where.toBuffer(buf, " where ", " and ");
        groupBy.toBuffer(buf, " group by ", ", ");
        having.toBuffer(buf, " having ", " and ");
        orderBy.toBuffer(buf, " order by ", ", ");
        return buf.toString();
    }

    private class ClauseList extends ArrayList {
        private final boolean allowDups;

        ClauseList(boolean allowDups) {
            this.allowDups = allowDups;
        }

        public boolean add(String element) {
            if (!allowDups) {
                if (contains(element)) {
                    return false;
                }
            }
            return super.add(element);
        }

        void toBuffer(StringBuffer buf, String first, String sep) {
            for (int i = 0; i < this.size(); i++) {
                String s = (String) this.get(i);
                if (i == 0) {
                    buf.append(first);
                } else {
                    buf.append(sep);
                }
                buf.append(s);
            }
        }
    }
}

// End SqlQuery.java
