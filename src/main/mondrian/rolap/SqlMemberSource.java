/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.rolap.sql.SqlQuery;

import java.sql.ResultSet;
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
	private java.sql.Connection jdbcConnection;
	private MemberCache cache;

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
		this.jdbcConnection =
				hierarchy.getSchema().getInternalConnection().jdbcConnection;
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
		ResultSet resultSet = null;
		String sql = makeLevelMemberCountSql(level);
		try {
			resultSet = RolapUtil.executeQuery(
					jdbcConnection, sql, "SqlMemberSource.getLevelMemberCount");
			Util.assertTrue(resultSet.next());
			return resultSet.getInt(1);
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


	private SqlQuery newQuery(String err)
	{
		try {
			return new SqlQuery(
				jdbcConnection.getMetaData());
		} catch (SQLException e) {
			throw Util.getRes().newInternal(err, e);
		}
	}

	/**
	 * Generates the SQL statement to count the members in
	 * <code>level</code>. For example, <blockquote>
	 *
	 * <pre>SELECT count(*) FROM (
	 *   SELECT DISTINCT "country", "state_province"
	 *   FROM "customer") AS "foo"</pre>
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
	String makeLevelMemberCountSql(RolapLevel level)
	{
		SqlQuery sqlQuery = newQuery(
			"while generating query to count members in level " + level);
		int levelDepth = level.getDepth();
  		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		if (levelDepth == levels.length) {
			// "select count(*) from schema.customer"
			sqlQuery.addSelect("count(*)");
			hierarchy.addToFrom(sqlQuery, level.nameExp, null);
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
					if (!sqlQuery.allowsCompoundCountDistinct()) {
						// I don't know know of a database where this would
						// happen. MySQL does not allow SELECT-in-FROM, but it
						// does allow compound COUNT DISTINCT.
						throw Util.newInternal(
								"Cannot generate query to count members of level '" +
								level.getUniqueName() +
								"': database supports neither SELECT-in-FROM nor compound COUNT DISTINCT");
					}
					columnList += ", ";
				}
				hierarchy.addToFrom(sqlQuery, level2.nameExp, null);
				columnList += level2.nameExp.getExpression(sqlQuery);
				if (level2.unique) {
					break; // no further qualification needed
				}
			}
			sqlQuery.addSelect("count(DISTINCT " + columnList + ")");
			return sqlQuery.toString();
		} else {
			sqlQuery.setDistinct(true);
			for (int i = levelDepth; i >= 0; i--) {
				RolapLevel level2 = levels[i];
				if (level2.isAll()) {
					continue;
				}
				hierarchy.addToFrom(sqlQuery, level2.nameExp, null);
				sqlQuery.addSelect(level2.nameExp.getExpression(sqlQuery));
				if (level2.unique) {
					break; // no further qualification needed
				}
			}
			SqlQuery outerQuery = newQuery(
					"while generating query to count members in level " + level);
			outerQuery.addSelect("count(*)");
			// Note: the "foo" is for Postgres, which requires
			// FROM-queries to have an alias
			boolean failIfExists = true;
			outerQuery.addFrom(sqlQuery, "foo", failIfExists);
			return outerQuery.toString();
		}
    }

	// implement MemberSource
	public RolapMember[] getMembers()
	{
		ResultSet resultSet = null;
		String sql = makeKeysSql();
		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
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

	private String makeKeysSql()
	{
		SqlQuery sqlQuery = newQuery(
			"while generating query to retrieve members of " + hierarchy);
		sqlQuery.setDistinct(true);
		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		for (int i = 0; i < levels.length; i++) {
			RolapLevel level = levels[i];
			if (level.isAll()) {
				continue;
			}
			MondrianDef.Expression exp = level.nameExp;
			hierarchy.addToFrom(sqlQuery, exp, null);
			sqlQuery.addSelect(exp.getExpression(sqlQuery));
			exp = level.ordinalExp;
			hierarchy.addToFrom(sqlQuery, exp, null);
			sqlQuery.addOrderBy(exp.getExpression(sqlQuery));
			for (int j = 0; j < level.properties.length; j++) {
				RolapProperty property = level.properties[j];
				exp = property.exp;
				hierarchy.addToFrom(sqlQuery, exp, null);
				sqlQuery.addSelect(exp.getExpression(sqlQuery));
			}
		}
		return sqlQuery.toString();
	}

	/**
	 * Generates the SQL statement to access members of <code>level</code>. For
	 * example, <blockquote>
	 * <pre>SELECT "country", "state_province", "city"
	 * FROM "customer"
	 * GROUP BY "country", "state_province", "city", "foo", "bar"</pre>
	 * </blockquote> accesses the "City" level of the "Customers"
	 * hierarchy. Note that:<ul>
	 *
	 * <li><code>"country", "state_province"</code> are the parent keys;</li>
	 *
	 * <li><code>"city"</code> is the level key;</li>
	 *
	 * <li><code>"foo", "bar"</code> are member properties.</li>
	 * </ul>
	 *
	 * @pre !level.isAll()
	 **/
	String makeLevelSql(RolapLevel level)
	{
		Util.assertPrecondition(!level.isAll());
		SqlQuery sqlQuery = newQuery(
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
			hierarchy.addToFrom(sqlQuery, level2.nameExp, null);
			String q = level2.nameExp.getExpression(sqlQuery);
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
				cache.putMember(key, root);
			}
			ArrayList list = new ArrayList(1);
			list.add(root);
			return list;
		}
		ResultSet resultSet = null;
		final RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		String sql = makeLevelSql(level);
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
	String makeChildMemberSql(RolapMember member)
	{
		SqlQuery sqlQuery = newQuery(
			"while generating query to retrieve children of member " + member);
		for (RolapMember m = member; m != null; m = (RolapMember)
				 m.getParentMember()) {
			RolapLevel level = (RolapLevel) m.getLevel();
  			if (level.isAll()) {
  				continue;
  			}
			hierarchy.addToFrom(sqlQuery, level.nameExp, null);
			String q = level.nameExp.getExpression(sqlQuery);
			sqlQuery.addWhere(q + " = " + m.quoteKeyForSql());
			if (level.unique) {
				break; // no further qualification needed
			}
  		}

		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		RolapLevel level = levels[member.getLevel().getDepth() + 1];
		hierarchy.addToFrom(sqlQuery, level.nameExp, null);
		String q = level.nameExp.getExpression(sqlQuery);
		sqlQuery.addSelect(q);
		sqlQuery.addGroupBy(q);
		hierarchy.addToFrom(sqlQuery, level.ordinalExp, null);
		String orderBy = level.ordinalExp.getExpression(sqlQuery);
		sqlQuery.addOrderBy(orderBy);
		if (!orderBy.equals(q)) {
			sqlQuery.addGroupBy(orderBy);
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
		ResultSet resultSet = null;
		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		int childDepth = parentMember.getLevel().getDepth() + 1;
		if (childDepth >= levels.length) {
			// member is at last level, so can have no children
			return;
		}
		RolapLevel childLevel = levels[childDepth];
		String sql = makeChildMemberSql(parentMember);
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
}

// End SqlMemberSource.java
