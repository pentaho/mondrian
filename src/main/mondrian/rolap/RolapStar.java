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
			sqlQuery = new SqlQuery(
				jdbcConnection.getMetaData());
		} catch (SQLException e) {
			throw Util.getRes().newInternal(e, "while computing single cell");
		}
		Hashtable tablesAdded = new Hashtable();
		// add measure
		Util.assertTrue(measure.table == factTable);
		tablesAdded.put(factTable,factTable);
		factTable.addToFrom(sqlQuery);
		sqlQuery.addSelect(
			measure.aggregator + "(" +
			sqlQuery.quoteIdentifier(factTable.alias, measure.name) +
			")");
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
			if (tablesAdded.get(table) == null) {
				tablesAdded.put(table,table);
				table.addToFrom(sqlQuery);
				sqlQuery.addWhere(
					sqlQuery.quoteIdentifier(
						table.alias, table.primaryKey) +
					" = " +
					sqlQuery.quoteIdentifier(
						factTable.alias, table.foreignKey));
				sqlQuery.addWhere(
					sqlQuery.quoteIdentifier(table.alias, column.name) +
					" = " +
					column.quoteValue(value));
			}
		}
		String sql = sqlQuery.toString();
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			if (RolapUtil.debugOut != null) {
				RolapUtil.debugOut.println(
					"RolapStar.getCell: executing sql [" + sql + "]");
			}
			statement = jdbcConnection.createStatement();
			resultSet = statement.executeQuery(sql);
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
					resultSet.close();
				}
				if (statement != null) {
					statement.close();
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

	;

	public static class Column
	{
		public Table table;
		public String name;
		boolean isNumeric;
		int cardinality = -1;

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
					inner.addSelect(inner.quoteIdentifier(name));
					table.addToFrom(inner);
					sqlQuery.addSelect("count(*)");
					sqlQuery.addFrom(inner, null);
				} else {
					// e.g. "select count(distinct product_id) from product"
					sqlQuery.addSelect(
						"count(distinct " +
						sqlQuery.quoteIdentifier(name) +
						")");
					table.addToFrom(sqlQuery);
				}
				String sql = sqlQuery.toString();
				if (RolapUtil.debugOut != null) {
					RolapUtil.debugOut.println(
						"RolapStar.Column.getCardinality: executing sql [" +
						sql + "]");
				}
				Statement statement = null;
				ResultSet resultSet = null;
				try {
					statement = table.star.jdbcConnection.createStatement();
					resultSet = statement.executeQuery(sql);
					Util.assertTrue(resultSet.next());
					cardinality = resultSet.getInt(1);
				} catch (SQLException e) {
					throw Util.getRes().newInternal(
						e, "while counting distinct values of column '" +
						name + "'; sql=[" + sql + "]");
				} finally {
					try {
						if (resultSet != null) {
							resultSet.close();
						}
						if (statement != null) {
							statement.close();
						}
					} catch (SQLException e) {
						// ignore
					}
				}
			}
			return cardinality;
		}
	};
	public static class Measure extends Column
	{
		public String aggregator;
	};
	public static class Table
	{
		public RolapStar star;
		public String alias;
		private MondrianDef.SQL[] selects;
        private String schema;
        private String table;
		public String primaryKey;
		public String foreignKey;
		Column[] columns;

        void setTable(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }
        void setQuery(MondrianDef.SQL[] selects) {
            this.selects = selects;
        }
		public void addToFrom(SqlQuery query) {
			if (selects != null) {
				String sqlString = query.chooseQuery(selects);
				query.addFromQuery(sqlString, alias);
			} else {
				query.addFromTable(schema,table,alias);
			}
		}
		public boolean isFunky() {
			return selects == null && table == null;
		}
	};
}


// End RolapStar.java
