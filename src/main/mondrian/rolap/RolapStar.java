/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.Util;
import mondrian.olap.MondrianDef;
import mondrian.rolap.sql.SqlQuery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.Date;
import java.io.PrintWriter;

/**
 * A <code>RolapStar</code> is a star schema. It is the means to read cell
 * values.
 *
 * todo: put this in package which specicializes in relational aggregation,
 * doesn't know anything about hierarchies etc.
 *
 * @author jhyde
 * @since 12 August, 2001
 * @version $Id$
 **/
public class RolapStar {
	public Connection jdbcConnection;
	Measure[] measures;
	public Table factTable;
	Table[] tables;
	/** todo: better, the dimensional model should hold the mapping **/
	Hashtable mapLevelToColumn = new Hashtable();

	/**
	 * Reads a cell of <code>measure</code>, where <code>columns</code> are
	 * constrained to <code>values</code>.  <code>values</code> must be the
	 * same length as <code>columns</code>; null values are left unconstrained.
	 **/
	Object getCell(CellRequest request)
	{
		Measure measure = request.getMeasure();
		Column[] columns = request.getColumns();
		Object[] values = request.getSingleValues();
		Util.assertTrue(columns.length == values.length);
		SqlQuery sqlQuery;
		try {
			sqlQuery = new SqlQuery(jdbcConnection.getMetaData());
		} catch (SQLException e) {
			throw Util.getRes().newInternal(e, "while computing single cell");
		}
		// add measure
		Util.assertTrue(measure.table == factTable);
		factTable.addToFrom(sqlQuery, true, true);
		sqlQuery.addSelect(
			measure.aggregator + "(" + measure.getExpression(sqlQuery) + ")");
		// add constraining dimensions
		for (int i = 0; i < columns.length; i++) {
			Object value = values[i];
			if (value == null) {
				continue; // not constrained
			}
			Column column = columns[i];
			Table table = column.table;
			if (table.isFunky()) {
				// this is a funky dimension -- ignore for now
				continue;
			}
			table.addToFrom(sqlQuery, true, true);
		}
		String sql = sqlQuery.toString();
		ResultSet resultSet = null;
		try {
			resultSet = RolapUtil.executeQuery(
					jdbcConnection, sql, "RolapStar.getCell");
			Object o = null;
			if (resultSet.next()) {
				o = resultSet.getObject(1);
			}
			if (o == null) {
				o = Util.nullValue; // convert to placeholder
			}
			return o;
		} catch (SQLException e) {
			throw Util.getRes().newInternal(
				e, "while computing single cell; sql=[" + sql + "]");
		} finally {
			try {
				if (resultSet != null) {
					resultSet.getStatement().close();
					resultSet.close();
				}
			} catch (SQLException e) {
				// ignore
			}
		}
	}

//  	static class Segment
//  	{
//  		Object[][] keys;
//  		Object[] values;
//  	};

//  	private static class Axis
//  	{
//  		Column column;
//  		Vector values = new Vector();
//  	};

	public static class Column
	{
		public Table table;
		public String name;
		boolean isNumeric;
		int cardinality = -1;

		public String getExpression(SqlQuery query) {
			return query.quoteIdentifier(table.getAlias(), name);
		}
		String quoteValue(Object value)
		{
			String s = value.toString();
			if (isNumeric) {
				return s;
			} else {
				return RolapUtil.singleQuoteForSql(s);
			}
		}
		public String quoteValues(Object[] values)
		{
			StringBuffer sb = new StringBuffer("(");
			for (int i = 0; i < values.length; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(quoteValue(values[i]));
			}
			sb.append(")");
			return sb.toString();
		}
		public int getCardinality()
		{
			if (cardinality == -1) {
				SqlQuery sqlQuery;
				try {
					sqlQuery = new SqlQuery(
						table.star.jdbcConnection.getMetaData());
				} catch (SQLException e) {
					throw Util.getRes().newInternal(
						e,
						"while counting distinct values of column '" +
						name + "'");
				}
				if (sqlQuery.isAccess()) {
					// Access doesn't like 'count(distinct)', so use,
					// e.g. "select count(*) from (select distinct product_id
					// from product)"
					SqlQuery inner = sqlQuery.cloneEmpty();
					inner.setDistinct(true);
					inner.addSelect(getExpression(inner));
					boolean failIfExists = true,
						joinToParent = false;
					table.addToFrom(inner, failIfExists, joinToParent);
					sqlQuery.addSelect("count(*)");
					sqlQuery.addFrom(inner, "foo", failIfExists);
				} else {
					// e.g. "select count(distinct product_id) from product"
					sqlQuery.addSelect(
						"count(distinct " + getExpression(sqlQuery) + ")");
					table.addToFrom(sqlQuery, true, true);
				}
				String sql = sqlQuery.toString();
				ResultSet resultSet = null;
				try {
					resultSet = RolapUtil.executeQuery(
							table.star.jdbcConnection, sql,
							"RolapStar.Column.getCardinality");
					Util.assertTrue(resultSet.next());
					cardinality = resultSet.getInt(1);
				} catch (SQLException e) {
					throw Util.getRes().newInternal(
						e, "while counting distinct values of column '" +
						name + "'; sql=[" + sql + "]");
				} finally {
					try {
						if (resultSet != null) {
							resultSet.getStatement().close();
							resultSet.close();
						}
					} catch (SQLException e) {
						// ignore
					}
				}
			}
			return cardinality;
		}
	}

	public static class Measure extends Column
	{
		public String aggregator;
	};

	public static class Table
	{
		public RolapStar star;
		private MondrianDef.Relation relation;
        private String schema;
        private String table;
		public String primaryKey;
		public String foreignKey;
		ArrayList columns = new ArrayList();
		public Table parent;
		public ArrayList children = new ArrayList();
		/** Condition with which it is connected to its parent. **/
		Condition joinCondition;

        public Table(String schema, String table) {
			this.relation = new MondrianDef.Table(schema, table, null);
        }
        public Table(
				MondrianDef.Relation relation, Table parent,
				Condition joinCondition) {
			this.relation = relation;
			if (relation instanceof MondrianDef.Table) {
				MondrianDef.Table table = (MondrianDef.Table) relation;
				this.schema = table.schema;
				this.table = table.name;
			} else {
				throw Util.newInternal("todo:");
			}
			this.parent = parent;
			this.joinCondition = joinCondition;
			Util.assertTrue((parent == null) == (joinCondition == null));
        }
		public String getAlias() {
			return relation.getAlias();
		}
		/**
		 * Returns a child table which maps onto a given relation, or null if
		 * there is none.
		 */
		public Table findChild(MondrianDef.Table table) {
			for (int i = 0; i < children.size(); i++) {
				Table childTable = (Table) children.get(i);
				if (childTable.table.equals(table.name) &&
						Util.equals(childTable.schema, table.schema)) {
					return childTable;
				}
			}
			return null;
		}
		/**
		 * Returns a descendant with a given alias, or null if none found.
		 */
		public Table findDescendant(String seekAlias) {
			if (getAlias().equals(seekAlias)) {
				return this;
			}
			for (int i = 0, n = children.size(); i < n; i++) {
				Table child = (Table) children.get(i);
				Table found = child.findDescendant(seekAlias);
				if (found != null) {
					return found;
				}
			}
			return null;
		}

		/**
		 * Adds this table to the from clause of a query.
		 *
		 * @param query Query to add to
		 * @param failIfExists Pass in false if you might have already added
		 *     the table before and if that happens you want to do nothing.
		 * @param joinToParent Pass in true if you are constraining a cell
		 *     calculcation, false if you are retrieving members.
		 */
		public void addToFrom(
				SqlQuery query, boolean failIfExists, boolean joinToParent) {
			query.addFrom(relation, failIfExists);
			Util.assertTrue((parent == null) == (joinCondition == null));
			if (joinToParent) {
				if (parent != null) {
					parent.addToFrom(query, failIfExists, joinToParent);
				}
				if (joinCondition != null) {
					query.addWhere(joinCondition.toString(query));
				}
			}
		}

		public boolean isFunky() {
			return relation == null;
		}
		/**
		 * Prints this table and its children.
		 */
		void print(PrintWriter pw, String prefix) {
			pw.print(prefix);
			pw.print("Table: alias=[" + this.getAlias());
			if (this.relation != null) {
				pw.print("] relation=[" + relation);
			}
			pw.print("] columns=[");
			for (int i = 0, n = columns.size(); i < n; i++) {
				Column column = (Column) columns.get(i);
				if (i > 0) {
					pw.print(",");
				}
				pw.print(column.name);
			}
			pw.println("]");
			for (int i = 0; i < children.size(); i++) {
				Table child = (Table) children.get(i);
				child.print(pw, prefix + "  ");
			}
		}
	}

	public static class Condition {
		String table1;
		String column1;
		String table2;
		String column2;
		Condition(
				String table1, String column1, String table2, String column2) {
			Util.assertTrue(table1 != null);
			Util.assertTrue(column1 != null);
			Util.assertTrue(table2 != null);
			Util.assertTrue(column2 != null);
			this.table1 = table1;
			this.column1 = column1;
			this.table2 = table2;
			this.column2 = column2;
		}
		String toString(SqlQuery query) {
			return query.quoteIdentifier(table1, column1) +
					" = " +
				query.quoteIdentifier(table2, column2);
		}
	}
}

// End RolapStar.java
