/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.sql.SqlQuery;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

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
	RolapSchema schema;
	DataSource dataSource;
	Measure[] measures;
	public Table factTable;
	/** Maps {@link RolapCube} to a {@link HashMap} which maps
	 * {@link RolapLevel} to {@link Column}. The double indirection is
	 * necessary because in different cubes, a shared hierarchy might be joined
	 * onto the fact table at different levels. */
	final HashMap mapCubeToMapLevelToColumn = new HashMap();
    /** Maps {@link Column} to {@link String} for each column which is a key
     * to a level. */
    final HashMap mapColumnToName = new HashMap();

    /**
	 * Please use {@link Pool#getOrCreateStar} to create a {@link
	 * RolapStar}.
	 */
	RolapStar(RolapSchema schema, DataSource dataSource) {
		this.schema = schema;
		this.dataSource = dataSource;
	}

    /**
     * Allocates a connection to the underlying RDBMS.
     *
     * The client MUST close this connection; use the <code>try ...
     * finally</code> idiom to be sure of this.
     */
	public Connection getJdbcConnection() {
		Connection jdbcConnection;
		try {
			jdbcConnection = dataSource.getConnection();
		} catch (SQLException e) {
			throw Util.newInternal(
				e, "Error while creating connection from data source");
		}
		return jdbcConnection;
	}

	/** For testing purposes only. **/
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

    public DataSource getDataSource() {
        return dataSource;
    }

	/**
	 * Retrieves the {@link RolapStar.Measure} in which a measure is stored.
	 */
	public static Measure getStarMeasure(Member member) {
		return (Measure) ((RolapStoredMeasure) member).starMeasure;
	}

	/**
	 * Retrieves a named column, returns null if not found.
	 */
	public Column lookupColumn(String tableAlias, String columnName) {
		final Table table = factTable.findDescendant(tableAlias);
		if (table != null) {
			for (int i = 0; i < table.columns.size(); i++) {
				Column column = (Column) table.columns.get(i);
				if (column.expression instanceof MondrianDef.Column) {
					MondrianDef.Column columnExpr = (MondrianDef.Column) column.expression;
					if (columnExpr.name.equals(columnName)) {
						return column;
					}
				}
			}
		}
		return null;
	}
	/**
	 * Reads a cell of <code>measure</code>, where <code>columns</code> are
	 * constrained to <code>values</code>.  <code>values</code> must be the
	 * same length as <code>columns</code>; null values are left unconstrained.
	 **/
	Object getCell(CellRequest request)
	{
        Connection jdbcConnection = getJdbcConnection();
        try {
            return getCell(request, jdbcConnection);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                //ignore
            }
        }
    }

    private Object getCell(CellRequest request, Connection jdbcConnection) {
        Measure measure = request.getMeasure();
        Column[] columns = request.getColumns();
        Object[] values = request.getSingleValues();
        Util.assertTrue(columns.length == values.length);
        SqlQuery sqlQuery;
        try {
            sqlQuery = new SqlQuery(jdbcConnection.getMetaData());
        } catch (SQLException e) {
            throw Util.getRes().newInternal("while computing single cell", e);
        }
        // add measure
        Util.assertTrue(measure.table == factTable);
        factTable.addToFrom(sqlQuery, true, true);
        sqlQuery.addSelect(
            measure.aggregator.getExpression(measure.getExpression(sqlQuery)));
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
                    "while computing single cell; sql=[" + sql + "]", e);
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

    /**
     * Returns a printable name for a column, generally the name of the level
     * mapped into it; or null if no such mapping exists. The mapping is
     * approximate.
     */
    public String getColumnName(Column column) {
        return (String) mapColumnToName.get(column);
    }

    public static class Column
	{
		public Table table;
		public MondrianDef.Expression expression;
		boolean isNumeric;
		int cardinality = -1;

		public Column() {
		}
		public String getExpression(SqlQuery query) {
			return expression.getExpression(query);
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

        public int getCardinality()
		{
			if (cardinality == -1) {
				Connection jdbcConnection = table.star.getJdbcConnection();
                try {
                    cardinality = getCardinality(jdbcConnection);
                } finally {
                    try {
                        jdbcConnection.close();
                    } catch (SQLException e) {
                        //ignore
                    }
                }
            }
			return cardinality;
		}

        private int getCardinality(Connection jdbcConnection) {
            SqlQuery sqlQuery;
            try {
                sqlQuery = new SqlQuery(
                    jdbcConnection.getMetaData());
            } catch (SQLException e) {
                throw Util.getRes().newInternal(
                        "while counting distinct values of column '" +
                        expression.getGenericExpression() + "'", e);
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
                sqlQuery.addFrom(inner, "init", failIfExists);
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
                        jdbcConnection, sql,
                        "RolapStar.Column.getCardinality");
                Util.assertTrue(resultSet.next());
                return resultSet.getInt(1);
            } catch (SQLException e) {
                throw Util.getRes().newInternal(
                        "while counting distinct values of column '" +
                        expression.getGenericExpression() + "'; sql=[" + sql + "]",
                        e);
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

        /**
         * Generates a predicate that a column matches one of a list of values.
         *
         * <p>
         * Several possible outputs, depending upon whether the there are
         * nulls:<ul>
         *
         * <li>One not-null value: <code>foo.bar = 1</code>
         *
         * <li>All values not null: <code>foo.bar in (1, 2, 3)</code></li
         *
         * <li>Null and not null values:
         * <code>(foo.bar is null or foo.bar in (1, 2))</code></li>
         *
         * <li>Only null values:
         * <code>foo.bar is null</code></li>
         *
         * <li>String values: <code>foo.bar in ('a', 'b', 'c')</code></li></ul>
         */
        public String createInExpr(String expr, Object[] constraints) {
            if (constraints.length == 1) {
                final Object constraint = constraints[0];
                if (constraint != RolapUtil.sqlNullValue) {
                    // One value, not null, for example "x = 1".
                    return expr + " = " + quoteValue(constraint);
                }
            }
            int notNullCount = 0;
            StringBuffer sb = new StringBuffer(expr);
            sb.append(" in (");
            for (int i = 0; i < constraints.length; i++) {
                final Object constraint = constraints[i];
                if (constraint == RolapUtil.sqlNullValue) {
                    continue;
                }
                if (notNullCount > 0) {
                    sb.append(", ");
                }
                ++notNullCount;
                sb.append(quoteValue(constraint));
            }
            sb.append(")");
            if (notNullCount < constraints.length) {
                // There was at least one null.
                switch (notNullCount) {
                case 0:
                    // Special case -- there were no values besides null.
                    // Return, for example, "x is null".
                    return expr + " is null";
                case 1:
                    // Special case -- one not-null value, and null, for
                    // example "(x is null or x = 1)".
                    return "(" + expr + " = " + quoteValue(constraints[0]) +
                            " or " + expr + " is null)";
                default:
                    // Nulls and values, for example,
                    // "(x in (1, 2) or x IS NULL)".
                    return "(" + sb.toString() + " or " + expr +
                            "is null)";
                }
            } else {
                // No nulls. Return, for example, "x in (1, 2, 3)".
                return sb.toString();
            }
        }
    }

	public static class Measure extends Column
	{
		public RolapAggregator aggregator;
	};

	public static class Table
	{
		public RolapStar star;
		MondrianDef.Relation relation;
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
			Util.assertTrue(
					relation instanceof MondrianDef.Table ||
					relation instanceof MondrianDef.View,
					"todo: allow dimension which is not a Table or View, [" +
					relation + "]");
			this.parent = parent;
			this.joinCondition = joinCondition;
			Util.assertTrue((parent == null) == (joinCondition == null));
        }
		public String getAlias() {
			return relation.getAlias();
		}
		/**
		 * Extends this 'leg' of the star by adding <code>relation</code>
		 * joined by <code>joinCondition</code>. If the same expression is
		 * already present, does not create it again.
		 */
		synchronized Table addJoin(
				MondrianDef.Relation relation,
				RolapStar.Condition joinCondition) {
			if (relation instanceof MondrianDef.Table ||
					relation instanceof MondrianDef.View) {
				RolapStar.Table starTable = findChild(relation);
				if (starTable == null) {
					starTable = new RolapStar.Table(relation, this, joinCondition);
					starTable.star = this.star;
					this.children.add(starTable);
				}
				return starTable;
			} else if (relation instanceof MondrianDef.Join) {
				MondrianDef.Join join = (MondrianDef.Join) relation;
				RolapStar.Table leftTable = addJoin(join.left, joinCondition);
				String leftAlias = join.leftAlias;
				if (leftAlias == null) {
					leftAlias = join.left.getAlias();
					if (leftAlias == null) {
						throw Util.newError(
								"missing leftKeyAlias in " + relation);
					}
				}
				String rightAlias = join.rightAlias;
				if (rightAlias == null) {
					rightAlias = join.right.getAlias();
					if (rightAlias == null) {
						throw Util.newError(
								"missing rightKeyAlias in " + relation);
					}
				}
				joinCondition = new RolapStar.Condition(
						new MondrianDef.Column(leftAlias, join.leftKey),
						new MondrianDef.Column(rightAlias, join.rightKey));
				RolapStar.Table rightTable = leftTable.addJoin(
						join.right, joinCondition);
				return rightTable;
			} else {
				throw Util.newInternal("bad relation type " + relation);
			}
		}

		/**
		 * Returns a child relation which maps onto a given relation, or null if
		 * there is none.
		 */
		public Table findChild(MondrianDef.Relation relation) {
			for (int i = 0; i < children.size(); i++) {
				Table child = (Table) children.get(i);
				if (child.relation.equals(relation)) {
					return child;
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
		 * Returns an ancestor with a given alias, or null if not found.
		 */
		public Table findAncestor(String tableName) {
			for (Table t = this; t != null; t = t.parent) {
				if (t.getAlias().equals(tableName)) {
					return t;
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
				pw.print(column.expression.getGenericExpression());
			}
			pw.println("]");
			for (int i = 0; i < children.size(); i++) {
				Table child = (Table) children.get(i);
				child.print(pw, prefix + "  ");
			}
		}
	}

	public static class Condition {
		MondrianDef.Expression left;
		MondrianDef.Expression right;

		Condition(
				MondrianDef.Expression left, MondrianDef.Expression right) {
			Util.assertPrecondition(left != null);
			Util.assertPrecondition(right != null);
			this.left = left;
			this.right = right;
		}
		String toString(SqlQuery query) {
			return left.getExpression(query) + " = " + right.getExpression(query);
		}
	}

}

// End RolapStar.java
