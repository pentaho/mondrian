/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 21, 2002
*/

package mondrian.rolap.sql;

import mondrian.olap.Util;
import mondrian.olap.MondrianDef;

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
	StringBuffer select = new StringBuffer(),
		from = new StringBuffer(),
		where = new StringBuffer(),
		groupBy = new StringBuffer(),
		having = new StringBuffer(),
		orderBy = new StringBuffer();
	int selectCount = 0,
		fromCount = 0,
		whereCount = 0,
		groupByCount = 0,
		havingCount = 0,
		orderByCount = 0;
	public ArrayList fromAliases = new ArrayList();

	/**
	 * Creates a <code>SqlQuery</code>
	 *
	 * @param databaseMetaData used to determine which dialect of
	 *     SQL to generate
	 */
	public SqlQuery(DatabaseMetaData databaseMetaData) {
		this.databaseMetaData = databaseMetaData;
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

	/**
	 * Encloses an identifier in quotation marks appropriate for the
	 * current SQL dialect. For example,
	 * <code>quoteIdentifier("emp")</code> yields a string containing
	 * <code>"emp"</code> in Oracle, and a string containing
	 * <code>[emp]</code> in Access.
	 **/
	public String quoteIdentifier(String val) {
		String q;
		try {
			q = databaseMetaData.getIdentifierQuoteString();
		} catch (SQLException e) {
			throw Util.getRes().newInternal("while quoting identifier", e);
		}
		if (q == null || q.trim().equals("")) {
			if (isMySQL()) {
				// mm.mysql.2.0.4 driver lies. We know better.
				q = "`";
			} else {
				return val; // quoting is not supported
			}
		}
		// if the value is already quoted, do nothing
		//  if not, then check for a dot qualified expression
		//  like "owner.table".
		//  In that case, prefix the single parts separately.
		if ( val.startsWith(q) && val.endsWith(q) ) {
			// already quoted - nothing to do
			return val;
	  }
	  int k = val.indexOf('.');
	  if ( k > 0 ) {
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
	public boolean isOracle() {
		return getProduct().equals("Oracle");
	}
	public boolean isAccess() {
		return getProduct().equals("ACCESS");
	}
	public boolean isPostgres() {
		return getProduct().toUpperCase().indexOf("POSTGRE") >= 0;
	}
	public boolean isMySQL() {
		return getProduct().toUpperCase().equals("MYSQL");
	}

	// -- behaviors --
	protected boolean requiresAliasForFromItems() {
		return isPostgres();
	}
	protected boolean allowsAs() {
		return !isOracle();
	}
	/** Whether "select * from (select * from t)" is OK. **/
	public boolean allowsFromQuery() {
		return !isMySQL();
	}
	/** Whether "select count(distinct x, y) from t" is OK. **/
	public boolean allowsCompoundCountDistinct() {
		return isMySQL();
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
		} else if (isMySQL()) {
			best = "mysql";
		} else if (isAccess()) {
			best = "access";
		} else if (isPostgres()) {
			best = "postgres";
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
	 */
	public void addFromQuery(
			String query, String alias, boolean failIfExists) {
		Util.assertPrecondition(alias != null);
		if (fromAliases.contains(alias)) {
			if (failIfExists) {
				throw Util.newInternal(
						"query already contains alias '" + alias + "'");
			} else {
				return;
			}
		}
		if (fromCount++ == 0) {
			from.append(" from ");
		} else {
			from.append(", ");
		}
		from.append("(");
		from.append(query);
		from.append(")");
		if (alias != null) {
			Util.assertTrue(!alias.equals(""));
			if (allowsAs()) {
				from.append(" as ");
			} else {
				from.append(" ");
			}
			from.append(quoteIdentifier(alias));
			fromAliases.add(alias);
		}
	}

	/**
	 * Adds <code>[schema.]table AS alias</code> to the FROM clause.
	 *
	 * @param schema schema name; may be null
	 * @param table table name
	 * @param alias table alias, may not be null
	 *
	 * @pre alias != null
	 */
	private void addFromTable(
			String schema, String table, String alias, boolean failIfExists) {
		if (fromAliases.contains(alias)) {
			if (failIfExists) {
				throw Util.newInternal(
						"query already contains alias '" + alias + "'");
			} else {
				return;
			}
		}
		if (fromCount++ == 0) {
			from.append(" from ");
		} else {
			from.append(", ");
		}
		from.append(quoteIdentifier(schema, table));
		if (alias != null) {
			Util.assertTrue(!alias.equals(""));
			if (allowsAs()) {
				from.append(" as ");
			} else {
				from.append(" ");
			}
			from.append(quoteIdentifier(alias));
			fromAliases.add(alias);
		}
	}

	public void addFrom(SqlQuery sqlQuery, String alias, boolean failIfExists)
	{
		addFromQuery(sqlQuery.toString(), alias, failIfExists);
	}

	public void addFrom(MondrianDef.Relation relation, boolean failIfExists) {
		if (relation instanceof MondrianDef.View) {
			MondrianDef.View view = (MondrianDef.View) relation;
			String sqlString = chooseQuery(view.selects);
			String alias = view.alias;
			if (!fromAliases.contains(alias)) {
				addFromQuery(sqlString, alias, failIfExists);
			}
		} else if (relation instanceof MondrianDef.Table) {
			MondrianDef.Table table = (MondrianDef.Table) relation;
			addFromTable(
					table.schema, table.name, table.getAlias(), failIfExists);
		} else if (relation instanceof MondrianDef.Join) {
			MondrianDef.Join join = (MondrianDef.Join) relation;
			addFrom(join.left, failIfExists);
			addFrom(join.right, failIfExists);
			addWhere(
					quoteIdentifier(join.getLeftAlias(), join.leftKey) +
					" = " +
					quoteIdentifier(join.getRightAlias(), join.rightKey));
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
		Util.assertTrue(fromCount > 0);
		from.append(
			" " + type + " join " + query + " as " +
			quoteIdentifier(alias) + " on " + condition);
		fromAliases.add(alias);
	}
	/** Adds an expression to the select clause, automatically creating a
	 * column alias. **/
	public void addSelect(String expression) {
		addSelect(expression, "c" + selectCount);
	}
	/** Adds an expression to the select clause, with a specified column
	 * alias. **/
	public void addSelect(String expression, String alias) {
		if (alias != null) {
			expression += " as " + quoteIdentifier(alias);
		}
		select.append(
			(selectCount++ == 0 ?
			 ("select " + (distinct ? "distinct " : "")) :
			 ", ") +
			expression);
	}
	public void addWhere(String expression)
	{
		where.append(
			(whereCount++ == 0 ? " where " : " and ") +
			expression);
	}
	public void addGroupBy(String expression)
	{
		groupBy.append(
			(groupByCount++ == 0 ? " group by " : ", ") + expression);
	}
	public void addHaving(String expression)
	{
		having.append(
			(havingCount++ == 0 ? " having " : " and ") +
			expression);
	}
	public void addOrderBy(String expression)
	{
		orderBy.append(
			(orderByCount++ == 0 ? " order by " : ", ") + expression);
	}
	public String toString()
	{
		return select.toString() + from.toString() +
			where.toString() + groupBy.toString() + having.toString() +
			orderBy.toString();
	}
}

// End SqlQuery.java
