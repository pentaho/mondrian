/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.Exp;
import mondrian.olap.MondrianDef;
import mondrian.olap.Property;
import mondrian.olap.Util;
import mondrian.rolap.sql.SqlQuery;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A <code>SqlMemberSource</code> reads members from a SQL database.
 *
 * <p>It's a good idea to put a {@link CacheMemberReader} on top of this.
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 **/
class SqlMemberSource implements MemberReader
{
	private RolapHierarchy hierarchy;
	private DataSource dataSource;
	private MemberCache cache;
	private int lastOrdinal = 0;

	private static Object sqlNullValue = new Object() {
		public boolean equals(Object o) {
			return o == this;
		}
		public int hashCode() {
			return super.hashCode();
		}
		public String toString() {
			return "null";
		}
	};

	SqlMemberSource(RolapHierarchy hierarchy)
	{
		this.hierarchy = hierarchy;
		this.dataSource =
				hierarchy.getSchema().getInternalConnection().dataSource;
	}

	// implement MemberSource
	public RolapHierarchy getHierarchy()
	{
		return hierarchy;
	}

	// implement MemberSource
	public boolean setCache(MemberCache cache)
	{
		this.cache = cache;
		return true; // yes, we support cache writeback
	}

	// implement MemberSource
	public int getMemberCount()
	{
		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		int count = 0;
		for (int i = 0; i < levels.length; i++) {
			count += getLevelMemberCount(levels[i]);
		}
		return count;
	}

	public RolapMember lookupMember(String[] uniqueNameParts, boolean failIfNotFound) {
		throw new UnsupportedOperationException();
	}

	private int getLevelMemberCount(RolapLevel level)
	{
		if (level.isAll()) {
			return 1;
		}
		Connection jdbcConnection;
		try {
			jdbcConnection = dataSource.getConnection();
		} catch (SQLException e) {
			throw Util.newInternal(
				e, "Error while creating connection from data source");
		}
        try {
            return getMemberCount(level, jdbcConnection);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private int getMemberCount(RolapLevel level, Connection jdbcConnection) {
        boolean[] mustCount = new boolean[1];
        String sql = makeLevelMemberCountSql(level, jdbcConnection, mustCount);
        ResultSet resultSet = null;
        try {
            resultSet = RolapUtil.executeQuery(
                    jdbcConnection, sql, "SqlMemberSource.getLevelMemberCount");
            if (! mustCount[0] ) {
              Util.assertTrue(resultSet.next());
              return resultSet.getInt(1);
            } else {
              // count distinct "manually"
              ResultSetMetaData rmd = resultSet.getMetaData();
              int nColumns = rmd.getColumnCount();
              int count = 0;
              String[] colStrings = new String[nColumns];
              while (resultSet.next()) {
                boolean isEqual = true;
                for (int i = 0; i < nColumns; i++ ) {
                  String colStr = resultSet.getString(i+1);
                  if (!colStr.equals(colStrings[i]))
                    isEqual = false;
                  colStrings[i] = colStr;
                }
                if (!isEqual)
                   count++;
              }
              return count;
            }
        } catch (SQLException e) {
            throw Util.getRes().newInternal(
                    "while counting members of level '" + level + "'; sql=[" +
                    sql + "]",
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


    private SqlQuery newQuery(Connection jdbcConnection, String err)
	{
		try {
            return new SqlQuery(jdbcConnection.getMetaData());
		} catch (SQLException e) {
			throw Util.newInternal(e, err);
		}
	}

	/**
	 * Generates the SQL statement to count the members in
	 * <code>level</code>. For example, <blockquote>
	 *
	 * <pre>SELECT count(*) FROM (
	 *   SELECT DISTINCT "country", "state_province"
	 *   FROM "customer") AS "init"</pre>
	 *
	 * </blockquote> counts the non-leaf "state_province" level. MySQL
	 * doesn't allow SELECT-in-FROM, so we use the syntax<blockquote>
	 *
	 * <pre>SELECT count(DISTINCT "country", "state_province")
	 * FROM "customer"</pre>
	 *
	 * </blockquote>. The leaf level requires a different query:<blockquote>
	 *
	 * <pre>SELECT count(*) FROM "customer"</pre>
	 *
	 * </blockquote> counts the leaf "name" level of the "customer" hierarchy.
	 **/
    private String makeLevelMemberCountSql(RolapLevel level,
                                           Connection jdbcConnection,
                                           boolean[] mustCount)
    {
		mustCount[0] = false;
		SqlQuery sqlQuery = newQuery(jdbcConnection,
				"while generating query to count members in level " + level);
		int levelDepth = level.getDepth();
		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		if (levelDepth == levels.length) {
			// "select count(*) from schema.customer"
			sqlQuery.addSelect("count(*)");
			hierarchy.addToFrom(sqlQuery, level.keyExp, null);
			return sqlQuery.toString();
		}
		if (!sqlQuery.allowsFromQuery()) {
			// "select count(distinct c1, c2) from table"
			String columnList = "";
			for (int i = levelDepth; i >= 0; i--) {
				RolapLevel level2 = levels[i];
				if (level2.isAll()) {
					 continue;
				}
				if (columnList != "") {
					// for databases where both SELECT-in-FROM and COUNT DISTINCT do not work,
					//  we do not generate any count and do the count distinct "manually".
					if (!sqlQuery.allowsCompoundCountDistinct())
						mustCount[0] = true;
					/*
					if (!sqlQuery.allowsCompoundCountDistinct()) {
						// I don't know know of a database where this would
						// happen. MySQL does not allow SELECT-in-FROM, but it
						// does allow compound COUNT DISTINCT.
						throw Util.newInternal(
							"Cannot generate query to count members of level '" +
							level.getUniqueName() +
							"': database supports neither SELECT-in-FROM nor compound COUNT DISTINCT");
					}
					*/
					columnList += ", ";
				}
				hierarchy.addToFrom(sqlQuery, level2.keyExp, null);
				columnList += level2.keyExp.getExpression(sqlQuery);
				if (level2.unique) {
					break; // no further qualification needed
				}
			}
			if (mustCount[0]) {
				sqlQuery.addSelect(columnList);
				sqlQuery.addOrderBy(columnList);
			} else {
				sqlQuery.addSelect("count(DISTINCT " + columnList + ")");
			}
			return sqlQuery.toString();
		} else {
			sqlQuery.setDistinct(true);
			for (int i = levelDepth; i >= 0; i--) {
				RolapLevel level2 = levels[i];
				if (level2.isAll()) {
					continue;
				}
				hierarchy.addToFrom(sqlQuery, level2.keyExp, null);
				sqlQuery.addSelect(level2.keyExp.getExpression(sqlQuery));
				if (level2.unique) {
					break; // no further qualification needed
				}
			}
			SqlQuery outerQuery = newQuery(
				jdbcConnection, "while generating query to count members in level " + level);
			outerQuery.addSelect("count(*)");
			// Note: the "init" is for Postgres, which requires
			// FROM-queries to have an alias
			boolean failIfExists = true;
			outerQuery.addFrom(sqlQuery, "init", failIfExists);
			return outerQuery.toString();
		}
    }


	public RolapMember[] getMembers()
	{
        Connection jdbcConnection;
        try {
            jdbcConnection = dataSource.getConnection();
        } catch (SQLException e) {
            throw Util.newInternal(
                e, "Error while creating connection from data source");
        }
        try {
            return getMembers(jdbcConnection);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private RolapMember[] getMembers(Connection jdbcConnection) {
        String sql = makeKeysSql(jdbcConnection);
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        ResultSet resultSet = null;
        try {
            resultSet = RolapUtil.executeQuery(
                jdbcConnection, sql, "SqlMemberSource.getMembers");
            ArrayList list = new ArrayList();
            HashMap map = new HashMap();
            RolapMember root = null;
            if (hierarchy.hasAll()) {
                root = new RolapMember(
                    null, (RolapLevel) hierarchy.getLevels()[0],
                    null, hierarchy.getAllMemberName());
                root.ordinal = lastOrdinal++;
                list.add(root);
            }
            while (resultSet.next()) {
                int column = 0;
                RolapMember member = root;
                for (int i = 0; i < levels.length; i++) {
                    RolapLevel level = levels[i];
                    if (level.isAll()) {
                        continue;
                    }
                    Object value = resultSet.getObject(column + 1);
                    if (value == null) {
                        value = sqlNullValue;
                    }
                    RolapMember parent = member;
                    MemberKey key = new MemberKey(parent, value);
                    member = (RolapMember) map.get(key);
                    if (member == null) {
                        member = new RolapMember(parent, level, value);
                        member.ordinal = lastOrdinal++;
                        if (value == sqlNullValue) {
                            addAsOldestSibling(list, member);
                        } else {
                            list.add(member);
                        }
                        map.put(key, member);
                    }
                    column++;
                    for (int j = 0; j < level.properties.length; j++) {
                        RolapProperty property = level.properties[j];
                        member.setProperty(
                                property.getName(), resultSet.getObject(column + 1));
                        column++;
                    }
                }
            }

            return RolapUtil.toArray(list);
        } catch (SQLException e) {
            throw Util.getRes().newInternal(
                    "while building member cache; sql=[" + sql + "]", e);
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
	 * Adds <code>member</code> just before the first element in
	 * <code>list</code> which has the same parent.
	 */
	private void addAsOldestSibling(List list, RolapMember member) {
		int i = list.size();
		while (--i >= 0) {
			RolapMember sibling = (RolapMember) list.get(i);
			if (sibling.getParentMember() != member.getParentMember()) {
				break;
			}
		}
		list.add(i + 1, member);
	}

	private String makeKeysSql(Connection jdbcConnection)
	{
		SqlQuery sqlQuery = newQuery(jdbcConnection,
                "while generating query to retrieve members of " + hierarchy);
		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		for (int i = 0; i < levels.length; i++) {
			RolapLevel level = levels[i];
			if (level.isAll()) {
				continue;
			}
			MondrianDef.Expression exp = level.keyExp;
			hierarchy.addToFrom(sqlQuery, exp, null);
            String expString = exp.getExpression(sqlQuery);
            sqlQuery.addSelect(expString);
            sqlQuery.addGroupBy(expString);
			exp = level.ordinalExp;
			hierarchy.addToFrom(sqlQuery, exp, null);
            expString = exp.getExpression(sqlQuery);
			sqlQuery.addOrderBy(expString);
            sqlQuery.addGroupBy(expString);
			for (int j = 0; j < level.properties.length; j++) {
				RolapProperty property = level.properties[j];
				exp = property.exp;
				hierarchy.addToFrom(sqlQuery, exp, null);
                expString = exp.getExpression(sqlQuery);
				sqlQuery.addSelect(expString);
                sqlQuery.addGroupBy(expString);
			}
		}
		return sqlQuery.toString();
	}

	/**
	 * Generates the SQL statement to access members of <code>level</code>. For
	 * example, <blockquote>
	 * <pre>SELECT "country", "state_province", "city"
	 * FROM "customer"
	 * GROUP BY "country", "state_province", "city", "init", "bar"</pre>
	 * </blockquote> accesses the "City" level of the "Customers"
	 * hierarchy. Note that:<ul>
	 *
	 * <li><code>"country", "state_province"</code> are the parent keys;</li>
	 *
	 * <li><code>"city"</code> is the level key;</li>
	 *
	 * <li><code>"init", "bar"</code> are member properties.</li>
	 * </ul>
	 *
	 * @pre !level.isAll()
	 **/
	String makeLevelSql(RolapLevel level, Connection jdbcConnection)
	{
		Util.assertPrecondition(!level.isAll());
		SqlQuery sqlQuery = newQuery(jdbcConnection,
                "while generating query to retrieve members of level " + level);
  		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		RolapLevel lastLevel = levels[levels.length - 1];
		final boolean needGroup = level != lastLevel;
		int levelDepth = level.getDepth();
		for (int i = 0; i <= levelDepth; i++) {
  			RolapLevel level2 = levels[i];
  			if (level2.isAll()) {
  				continue;
  			}
			hierarchy.addToFrom(sqlQuery, level2.keyExp, null);
			String q = level2.keyExp.getExpression(sqlQuery);
			sqlQuery.addSelect(q);
			if (needGroup) {
				sqlQuery.addGroupBy(q);
			}
			hierarchy.addToFrom(sqlQuery, level2.ordinalExp, null);
			sqlQuery.addOrderBy(level2.ordinalExp.getExpression(sqlQuery));
			for (int j = 0; j < level2.properties.length; j++) {
				RolapProperty property = level2.properties[j];
				String q2 = property.exp.getExpression(sqlQuery);
				sqlQuery.addSelect(q2);
				if (needGroup) {
					sqlQuery.addGroupBy(q2);
				}
			}
		}
		return sqlQuery.toString();
	}

	// implement MemberReader
	public List getMembersInLevel(
		RolapLevel level, int startOrdinal, int endOrdinal)
	{
		if (level.isAll()) {
			final String allMemberName = hierarchy.getAllMemberName();
			Object key = cache.makeKey(null, allMemberName);
			RolapMember root = cache.getMember(key);
			if (root == null) {
				root = new RolapMember(null, level, null, allMemberName);
				root.ordinal = lastOrdinal++;
				cache.putMember(key, root);
			}
			ArrayList list = new ArrayList(1);
			list.add(root);
			return list;
		}
		final RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		Connection jdbcConnection;
		try {
			jdbcConnection = dataSource.getConnection();
		} catch (SQLException e) {
			throw Util.newInternal(
				e, "Error while creating connection from data source");
		}
        try {
            return getMembersInLevel(level, jdbcConnection, levels);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private List getMembersInLevel(RolapLevel level, Connection jdbcConnection, final RolapLevel[] levels) {
        String sql = makeLevelSql(level, jdbcConnection);
        ResultSet resultSet = null;
        try {
            resultSet = RolapUtil.executeQuery(
                jdbcConnection, sql, "SqlMemberSource.getMembersInLevel");
            ArrayList list = new ArrayList();
            final int levelDepth = level.getDepth();
            RolapMember allMember = null;
            if (hierarchy.hasAll()) {
                final List rootMembers = getRootMembers();
                Util.assertTrue(rootMembers.size() == 1);
                allMember = (RolapMember) rootMembers.get(0);
            }
            // members[i] is the current member of level#i, and siblings[i]
            // is the current member of level#i plus its siblings
            RolapMember[] members = new RolapMember[levels.length];
            ArrayList[] siblings = new ArrayList[levels.length + 1];
            while (resultSet.next()) {
                int column = 0;
                RolapMember member = null;
                for (int i = 0; i <= levelDepth; i++) {
                    RolapLevel level2 = levels[i];
                    if (level2.isAll()) {
                        member = allMember;
                        continue;
                    }
                    Object value = resultSet.getObject(++column);
                    if (value == null) {
                        value = sqlNullValue;
                    }
                    RolapMember parent = member;
                    Object key = cache.makeKey(parent, value);
                    member = cache.getMember(key);
                    if (member == null) {
                        member = new RolapMember(parent, level2, value);
                        member.ordinal = lastOrdinal++;
                        for (int j = 0; j < level2.properties.length; j++) {
                            RolapProperty property = level2.properties[j];
                            member.setProperty(
                                    property.getName(),
                                    resultSet.getObject(++column));
                        }
                        cache.putMember(key, member);
                    } else {
                        column += level2.properties.length;
                    }
                    if (member != members[i]) {
                        // Flush list we've been building.
                        ArrayList children = siblings[i + 1];
                        if (children != null) {
                            cache.putChildren(members[i], children);
                        }
                        // Start a new list, if the cache needs one. (We don't
                        // synchronize, so it's possible that the cache will
                        // have one by the time we complete it.)
                        if (i < levelDepth &&
                                !cache.hasChildren(member)) {
                            siblings[i + 1] = new ArrayList();
                        } else {
                            siblings[i + 1] = null; // don't bother building up a list
                        }
                        // Record new current member of this level.
                        members[i] = member;
                        // If we're building a list of siblings at this level,
                        // we haven't seen this one before, so add it.
                        if (siblings[i] != null) {
                            if (value == sqlNullValue) {
                                addAsOldestSibling(siblings[i], member);
                            } else {
                                siblings[i].add(member);
                            }
                        }
                    }
                }
                list.add(member);
            }
            for (int i = 0; i < members.length; i++) {
                RolapMember member = members[i];
                final ArrayList children = siblings[i + 1];
                if (member != null &&
                        children != null) {
                    cache.putChildren(member, list);
                }
            }
            return list;
        } catch (Throwable e) {
            throw Util.getRes().newInternal(
                    "while populating member cache with members for level '" +
                    level.getUniqueName() + "'; sql=[" + sql + "]", e);
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

    // implement MemberSource
	public List getRootMembers() {
		return getMembersInLevel((RolapLevel) hierarchy.getLevels()[0], 0,
				Integer.MAX_VALUE);
	}

	/**
	 * Generates the SQL statement to access the children of
	 * <code>member</code>. For example, <blockquote>
	 *
	 * <pre>SELECT "city"
	 * FROM "customer"
	 * WHERE "country" = 'USA'
	 * AND "state_province" = 'BC'
	 * GROUP BY "city"</pre>
	 * </blockquote> retrieves the children of the member
	 * <code>[Canada].[BC]</code>. See also {@link #makeLevelSql}.
	 **/
	String makeChildMemberSql(RolapMember member, Connection jdbcConnection)
	{
		SqlQuery sqlQuery = newQuery(jdbcConnection,
                "while generating query to retrieve children of member " + member);
		for (RolapMember m = member; m != null; m = (RolapMember)
				 m.getParentMember()) {
			RolapLevel level = (RolapLevel) m.getLevel();
  			if (level.isAll()) {
  				continue;
  			}
			hierarchy.addToFrom(sqlQuery, level.keyExp, null);
			String q = level.keyExp.getExpression(sqlQuery);
			sqlQuery.addWhere(q + " = " + m.quoteKeyForSql());
			if (level.unique) {
				break; // no further qualification needed
			}
  		}

		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		RolapLevel level = levels[member.getLevel().getDepth() + 1];
		hierarchy.addToFrom(sqlQuery, level.keyExp, null);
		String q = level.keyExp.getExpression(sqlQuery);
		sqlQuery.addSelect(q);
		sqlQuery.addGroupBy(q);
		hierarchy.addToFrom(sqlQuery, level.ordinalExp, null);
		String orderBy = level.ordinalExp.getExpression(sqlQuery);
		sqlQuery.addOrderBy(orderBy);
		if (!orderBy.equals(q)) {
			sqlQuery.addGroupBy(orderBy);
		}
		for (int j = 0; j < level.properties.length; j++) {
			RolapProperty property = level.properties[j];
			final MondrianDef.Expression exp = property.exp;
			hierarchy.addToFrom(sqlQuery, exp, null);
			final String s = exp.getExpression(sqlQuery);
			sqlQuery.addSelect(s);
			sqlQuery.addGroupBy(s);
		}
		return sqlQuery.toString();
	}

	public void getMemberChildren(List parentMembers, List children)
	{
		for (int i = 0; i < parentMembers.size(); i++) {
			getMemberChildren((RolapMember) parentMembers.get(i), children);
		}
	}

	public void getMemberChildren(RolapMember parentMember, List children)
	{
        Connection jdbcConnection;
        try {
            jdbcConnection = dataSource.getConnection();
        } catch (SQLException e) {
            throw Util.newInternal(
                e, "Error while creating connection from data source");
        }
        try {
            getMemberChildren(parentMember, children, jdbcConnection);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private void getMemberChildren(RolapMember parentMember, List children, Connection jdbcConnection) {
        String sql;
        boolean parentChild;
        final RolapLevel parentLevel = (RolapLevel) parentMember.getLevel();
        RolapLevel childLevel;
        if (parentLevel.parentExp != null) {
            sql = makeChildMemberSqlPC(parentMember, jdbcConnection);
            parentChild = true;
            childLevel = parentLevel;
        } else {
            RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
            int childDepth = parentLevel.getDepth() + 1;
            if (childDepth >= levels.length) {
                // member is at last level, so can have no children
                return;
            }
            childLevel = levels[childDepth];
            if (childLevel.parentExp != null) {
                sql = makeChildMemberSql_PCRoot(parentMember, jdbcConnection);
                parentChild = true;
            } else {
                sql = makeChildMemberSql(parentMember, jdbcConnection);
                parentChild = false;
            }
        }
        ResultSet resultSet = null;
        try {
            resultSet = RolapUtil.executeQuery(
                jdbcConnection, sql, "SqlMemberSource.getMemberChildren");
            while (resultSet.next()) {
                Object value = resultSet.getObject(1);
                if (value == null) {
                    value = sqlNullValue;
                }
                Object key = cache.makeKey(parentMember, value);
                RolapMember member = cache.getMember(key);
                if (member == null) {
                    member = new RolapMember(parentMember, childLevel, value);
                    member.ordinal = lastOrdinal++;
                    if (parentChild) {
                        // Create a 'public' and a 'data' member. The public member is
                        // calculated, and its value is the aggregation of the data member and all
                        // of the children. The children and the data member belong to the parent
                        // member; the data member does not have any children.
                        final RolapParentChildMember parentChildMember =
                                new RolapParentChildMember(parentMember, childLevel, value, member);
                        member = parentChildMember;
                    }
                    for (int j = 0; j < childLevel.properties.length; j++) {
                        RolapProperty property = childLevel.properties[j];
                        member.setProperty(
                                property.getName(), resultSet.getObject(j + 2));
                    }
                    cache.putMember(key, member);
                }
                if (value == sqlNullValue) {
                    addAsOldestSibling(children, member);
                } else {
                    children.add(member);
                }
            }
        } catch (SQLException e) {
            throw Util.getRes().newInternal(
                    "while building member cache; sql=[" + sql + "]", e);
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
	 * Generates the SQL to find all root members of a parent-child hierarchy.
	 * For example, <blockquote>
	 *
	 * <pre>SELECT "employee_id"
	 * FROM "employee"
	 * WHERE "supervisor_id" IS NULL
	 * GROUP BY "employee_id"</pre>
	 * </blockquote> retrieves the root members of the <code>[Employee]</code>
	 * hierarchy.
	 *
	 * <p>Currently, parent-child hierarchies may have only one level (plus the
	 * 'All' level).
	 */
	private String makeChildMemberSql_PCRoot(RolapMember member,
            Connection jdbcConnection) {
		SqlQuery sqlQuery = newQuery(
                jdbcConnection, "while generating query to retrieve children of parent/child hierarchy member " + member);
		Util.assertTrue(member.isAll(), "In the current implementation, parent/child hierarchies must have only one level (plus the 'All' level).");
		RolapLevel level = (RolapLevel) member.getLevel().getChildLevel();
		Util.assertTrue(!level.isAll(), "all level cannot be parent-child");
		Util.assertTrue(level.unique, "parent-child level '" + level + "' must be unique");
		hierarchy.addToFrom(sqlQuery, level.parentExp, null);
		String parentId = level.parentExp.getExpression(sqlQuery);
		String condition;
		if (level.nullParentValue == null ||
				level.nullParentValue.equalsIgnoreCase("NULL")) {
			condition = parentId + " IS NULL";
		} else {
			// Quote the value if it doesn't seem to be a number.
			try {
				Util.discard(Double.parseDouble(level.nullParentValue));
				condition = parentId + " = " + level.nullParentValue;
			} catch (NumberFormatException e) {
				condition = parentId + " = " + RolapUtil.singleQuoteForSql(level.nullParentValue);
			}
		}
		sqlQuery.addWhere(condition);
		hierarchy.addToFrom(sqlQuery, level.keyExp, null);
		String childId = level.keyExp.getExpression(sqlQuery);
		sqlQuery.addSelect(childId);
		sqlQuery.addGroupBy(childId);
		hierarchy.addToFrom(sqlQuery, level.ordinalExp, null);
		String orderBy = level.ordinalExp.getExpression(sqlQuery);
		sqlQuery.addOrderBy(orderBy);
		if (!orderBy.equals(childId)) {
			sqlQuery.addGroupBy(orderBy);
		}
		for (int j = 0; j < level.properties.length; j++) {
			RolapProperty property = level.properties[j];
			final MondrianDef.Expression exp = property.exp;
			hierarchy.addToFrom(sqlQuery, exp, null);
			final String s = exp.getExpression(sqlQuery);
			sqlQuery.addSelect(s);
			sqlQuery.addGroupBy(s);
		}
		return sqlQuery.toString();
	}

	/**
	 * Generates the SQL statement to access the children of
	 * <code>member</code> in a parent-child hierarchy. For example,
	 * <blockquote>
	 *
	 * <pre>SELECT "employee_id"
	 * FROM "employee"
	 * WHERE "supervisor_id" = 5</pre>
	 * </blockquote> retrieves the children of the member
	 * <code>[Employee].[5]</code>. See also {@link #makeLevelSql}.
	 **/
	private String makeChildMemberSqlPC(RolapMember member, Connection jdbcConnection) {
		SqlQuery sqlQuery = newQuery(
                jdbcConnection, "while generating query to retrieve children of parent/child hierarchy member " + member);
		RolapLevel level = (RolapLevel) member.getLevel();
		Util.assertTrue(!level.isAll(), "all level cannot be parent-child");
		Util.assertTrue(level.unique, "parent-child level '" + level + "' must be unique");
		hierarchy.addToFrom(sqlQuery, level.parentExp, null);
		String parentId = level.parentExp.getExpression(sqlQuery);
		sqlQuery.addWhere(parentId + " = " + member.quoteKeyForSql());

		hierarchy.addToFrom(sqlQuery, level.keyExp, null);
		String childId = level.keyExp.getExpression(sqlQuery);
		sqlQuery.addSelect(childId);
		sqlQuery.addGroupBy(childId);
		hierarchy.addToFrom(sqlQuery, level.ordinalExp, null);
		String orderBy = level.ordinalExp.getExpression(sqlQuery);
		sqlQuery.addOrderBy(orderBy);
		if (!orderBy.equals(childId)) {
			sqlQuery.addGroupBy(orderBy);
		}
		for (int j = 0; j < level.properties.length; j++) {
			RolapProperty property = level.properties[j];
			final MondrianDef.Expression exp = property.exp;
			hierarchy.addToFrom(sqlQuery, exp, null);
			final String s = exp.getExpression(sqlQuery);
			sqlQuery.addSelect(s);
			sqlQuery.addGroupBy(s);
		}
		return sqlQuery.toString();
	}

	// implement MemberReader
	public RolapMember getLeadMember(RolapMember member, int n)
	{
		throw new UnsupportedOperationException();
	}

	public void getMemberRange(
			RolapLevel level, RolapMember startMember, RolapMember endMember,
			List memberList) {
		throw new UnsupportedOperationException();
	}

	public int compare(RolapMember m1, RolapMember m2, boolean siblingsAreEqual) {
		throw new UnsupportedOperationException();
	}

	private static class RolapParentChildMember extends RolapMember {
		private final RolapMember dataMember;
		public RolapParentChildMember(RolapMember parentMember, RolapLevel childLevel, Object value, RolapMember dataMember) {
			super(parentMember, childLevel, value);
			this.dataMember = dataMember;
		}
		public boolean isCalculated() {
			return true;
		}
		Exp getExpression() {
			return ((RolapHierarchy) super.getHierarchy()).getAggregateChildrenExpression();
		}

		public Object getPropertyValue(String name) {
			if (name.equals(Property.PROPERTY_CONTRIBUTING_CHILDREN)) {
				List list = new ArrayList();
				list.add(dataMember);
				((RolapHierarchy) super.getHierarchy()).memberReader.getMemberChildren(this, list);
				return list;
			} else {
				return super.getPropertyValue(name);
			}
		}
	}
}

// End SqlMemberSource.java
