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
import java.util.Iterator;
import java.util.List;
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
 * 
 * <p>
 * NOTE: Instances of this class are NOT thread safe so the user must make 
 * sure this is accessed by only one thread at a time.
 */
public class SqlQuery
{
    private boolean distinct;
    private final DatabaseMetaData databaseMetaData;

    private final ClauseList select;
    private final ClauseList from;
    private final ClauseList where;
    private final ClauseList groupBy;
    private final ClauseList having;
    private final ClauseList orderBy;

    /** 
     * This list is used to keep track of what aliases have been  used in the
     * FROM clause. One might think that a java.util.Set would be a more
     * appropriate Collection type, but if you only have a couple of "from
     * aliases", then iterating over a list is faster than doing a hash lookup
     * (as is used in java.util.HashSet).
     */
    private final List fromAliases;
    
    private final String quoteIdentifierString;

    /** Scratch buffer. Clear it before use. */
    private final StringBuffer buf;

    /** 
     * Base constructor used by all other constructors to create an empty
     * instance.
     * 
     * @param databaseMetaData 
     */
    private SqlQuery(final DatabaseMetaData databaseMetaData,
                     final String quoteIdentifierString) {
        // the databaseMetaData instance variable must be set before calling
        // initializeQuoteIdentifierString method
        this.databaseMetaData = databaseMetaData;

        this.quoteIdentifierString = (quoteIdentifierString == null)
                ? initializeQuoteIdentifierString()
                : quoteIdentifierString;

        // both select and from allow duplications
        this.select = new ClauseList(true);
        this.from = new ClauseList(true);

        this.where = new ClauseList(false);
        this.groupBy = new ClauseList(false);
        this.having = new ClauseList(false);
        this.orderBy = new ClauseList(false);
        this.fromAliases = new ArrayList();
        this.buf = new StringBuffer(128);
    }
    /**
     * Creates a <code>SqlQuery</code>
     *
     * @param databaseMetaData used to determine which dialect of
     *     SQL to generate
     */
    public SqlQuery(final DatabaseMetaData databaseMetaData) {
        this(databaseMetaData, null);
    }

    /**
     * Creates an empty <code>SqlQuery</code> with the same environment as this
     * one. (As per the Gang of Four 'prototype' pattern.)
     **/
    public SqlQuery cloneEmpty()
    {
        return new SqlQuery(databaseMetaData, quoteIdentifierString);
    }

    public void setDistinct(final boolean distinct) {
        this.distinct = distinct;
    }

    private String initializeQuoteIdentifierString() {
        String s = null;
        try {
            s = databaseMetaData.getIdentifierQuoteString();
        } catch (SQLException e) {
            throw Util.getRes().newInternal("while quoting identifier", e);
        }

        if ((s == null) || (s.trim().length() == 0)) {
            if (isMySQL()) {
                // mm.mysql.2.0.4 driver lies. We know better.
                s = "`";
            } else {
                // Quoting not supported
                s = null;
            }
        }
        return s;
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
     * Encloses an identifier in quotation marks appropriate for the
     * current SQL dialect. For example,
     * <code>quoteIdentifier("emp")</code> yields a string containing
     * <code>"emp"</code> in Oracle, and a string containing
     * <code>[emp]</code> in Access.
     **/
    public String quoteIdentifier(final String val) {
        int size = val.length() + SINGLE_QUOTE_SIZE;
        StringBuffer buf = new StringBuffer(size);

        quoteIdentifier(val, buf);

        return buf.toString();
    }   
    
    /** 
     * This is the implementation of the quoteIdentifier method which quotes the
     * the val parameter (identifier) placing the result in the StringBuffer
     * parameter.
     * 
     * @param val identifier to quote (must not be null).
     * @param buf 
     */
    public void quoteIdentifier(final String val, final StringBuffer buf) {
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
     **/
    public String quoteIdentifier(final String qual, final String name) {
        // We know if the qalifier is null, then only the name is going
        // to be quoted.
        int size = name.length()
            + ((qual == null) 
                ? SINGLE_QUOTE_SIZE
                : (qual.length() + DOUBLE_QUOTE_SIZE));
        StringBuffer buf = new StringBuffer(size);

        quoteIdentifier(qual, name, buf);

        return buf.toString();
    } 

    /**     
     * This implements the quoting of a qualifier and name allowing one to 
     * pass in a StringBuffer thus saving the allocation and copying.
     *      
     * @param qual optional qualifier to be quoted.
     * @param name name to be quoted (must not be null).
     * @param buf 
     */     
    public void quoteIdentifier(final String qual, 
                                final String name, 
                                final StringBuffer buf) {
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
        return ("04".compareTo(version_release[0]) >= 0);
    }

    // Note: its not clear that caching the best name would actually save
    // very much time, so we do not do so.
    private String getBestName() {
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
        return best;
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
    public String chooseQuery(final MondrianDef.SQL[] sqls) {
        String best = getBestName();

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
     *
     *
     * @param query  
     * @param alias (if not null, must not be zero length).
     * @param failIfExists if true, throws exception if alias already exists
     * @return true if query *was* added
     *
     * @pre alias != null
     */
    public boolean addFromQuery(final String query,   
                                final String alias, 
                                final boolean failIfExists) {
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

        buf.append('(');
        buf.append(query);
        buf.append(')');
        if (alias != null) {
            Util.assertTrue(alias.length() > 0);

            if (allowsAs()) {
                buf.append(" as ");
            } else {
                buf.append(' ');
            }
            quoteIdentifier(alias, buf);
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
     * @param failIfExists 
     *
     * @pre alias != null
     * @return true if table *was* added
     */
    private boolean addFromTable(final String schema, 
                                 final String table, 
                                 final String alias, 
                                 final String filter, 
                                 final boolean failIfExists) {
        if (fromAliases.contains(alias)) {
            if (failIfExists) {
                throw Util.newInternal(
                        "query already contains alias '" + alias + "'");
            } else {
                return false;
            }
        }

        buf.setLength(0);
        quoteIdentifier(schema, table, buf);
        if (alias != null) {
            Util.assertTrue(alias.length() > 0);

            if (allowsAs()) {
                buf.append(" as ");
            } else {
                buf.append(' ');
            }
            quoteIdentifier(alias, buf);
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
    public boolean addFrom(final MondrianDef.Relation relation, 
                           final String alias,
                           final boolean failIfExists)
    {
        if (relation instanceof MondrianDef.View) {
            final MondrianDef.View view = (MondrianDef.View) relation;
            final String viewAlias = (alias == null)
                    ? view.getAlias()
                    : alias;
            final String sqlString = chooseQuery(view.selects);

            return addFromQuery(sqlString, viewAlias, false);

        } else if (relation instanceof MondrianDef.Table) {
            final MondrianDef.Table table = (MondrianDef.Table) relation;
            final String tableAlias = (alias == null)
                    ? table.getAlias()
                    : alias;

            return addFromTable(table.schema, table.name, tableAlias,
                table.getFilter(), failIfExists);

        } else if (relation instanceof MondrianDef.Join) {
            final MondrianDef.Join join = (MondrianDef.Join) relation;
            final String leftAlias = join.getLeftAlias();
            final String rightAlias = join.getRightAlias();

            boolean added = addFrom(join.left, leftAlias, failIfExists) ||
                            addFrom(join.right, rightAlias, failIfExists);

            if (added) {
                buf.setLength(0);

                quoteIdentifier(leftAlias, join.leftKey, buf);
                buf.append(" = ");
                quoteIdentifier(rightAlias, join.rightKey, buf);

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
    public void addSelect(final String expression, final String alias) {
        buf.setLength(0);

        buf.append(expression);
        if (alias != null) {
            buf.append(" as ");
            quoteIdentifier(alias, buf);
        }

        select.add(buf.toString());
    }

    public void addWhere(final String exprLeft, 
                         final String exprMid, 
                         final String exprRight)
    {
        int len = exprLeft.length() + exprMid.length() + exprRight.length();
        StringBuffer buf = new StringBuffer(len);

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

    public void addOrderBy(final String expression)
    {
        orderBy.add(expression);
    }

    public String toString()
    {
        buf.setLength(0);

        select.toBuffer(buf, distinct ? "select distinct " : "select ", ", ");
        from.toBuffer(buf, " from ", ", ");
        where.toBuffer(buf, " where ", " and ");
        groupBy.toBuffer(buf, " group by ", ", ");
        having.toBuffer(buf, " having ", " and ");
        orderBy.toBuffer(buf, " order by ", ", ");

        return buf.toString();
    }

    private class ClauseList extends ArrayList {
        private final boolean allowDups;

        ClauseList(final boolean allowDups) {
            this.allowDups = allowDups;
        }

        
        /** 
         * Parameter element is added if either duplicates are allowed or if
         * it has not already been added.
         * 
         * @param element 
         */
        void add(final String element) {
            if (allowDups || !contains(element)) {
                super.add(element);
            }
        }

        void toBuffer(final StringBuffer buf, 
                      final String first, 
                      final String sep) {
            Iterator it = iterator();
            boolean firstTime = true;
            while (it.hasNext()) {
                String s = (String) it.next();

                if (firstTime) {
                    buf.append(first);
                    firstTime = false;
                } else {
                    buf.append(sep);
                }

                buf.append(s);
            }
        }
    }
}

// End SqlQuery.java
