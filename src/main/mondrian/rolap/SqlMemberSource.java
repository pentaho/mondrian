/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.rolap.sql.SqlQuery;

import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.io.*;

/**
 * A <code>SqlMemberSource</code> reads members from a SQL database. Good idea
 * to put a {@link CacheMemberReader} on top of this.
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
	public void setCache(MemberCache cache)
	{
		this.cache = cache;
	}

	boolean canDoRolap()
	{
		if (false) {
			RolapLevel lastLevel = (RolapLevel) hierarchy.getLevels()[
				hierarchy.getLevels().length - 1];
			return lastLevel.ordinalExp != null;
		}
		return true; // todo: remove this method
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

	public RolapMember lookupMember(String uniqueName, boolean failIfNotFound) {
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
			hierarchy.addToFrom(sqlQuery, level, null);
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
				hierarchy.addToFrom(sqlQuery, level2, null);
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
				hierarchy.addToFrom(sqlQuery, level2, null);
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
			int ordinal = 0;
			if (hierarchy.hasAll()) {
				root = new RolapMember(
					null, (RolapLevel) hierarchy.getLevels()[0],
					null, hierarchy.getAllMemberName());
				root.ordinal = ordinal++;
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
						member.ordinal = ordinal++;
						list.add(member);
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
			hierarchy.addToFrom(sqlQuery, level, null);
			sqlQuery.addSelect(level.nameExp.getExpression(sqlQuery));
			sqlQuery.addOrderBy(level.ordinalExp.getExpression(sqlQuery));
			for (int j = 0; j < level.properties.length; j++) {
				RolapProperty property = level.properties[j];
				String q = property.exp.getExpression(sqlQuery);
				sqlQuery.addSelect(q);
			}
		}
		return sqlQuery.toString();
	}

	/**
	 * Returns the ordinal of the nearest level above <code>level</code>
	 * whose members are unique without being qualified by their parent member.
	 */
	static private int uniqueLevel(Level level) {
		RolapLevel[] levels = (RolapLevel[]) level.getHierarchy().getLevels();
		for (int i = level.getDepth(); i >= 0; i--) {
			if (levels[i].unique) {
				return i;
			}
		}
		return 0;
	}

	/**
	 * Generates the SQL statement to access members of <code>level</code>. For
	 * example, <blockquote>
	 * <pre>SELECT "country", "state_province", "city", min("ordinal") - 1
	 * FROM "customer"
	 * GROUP BY "country", "state_province", "city"</pre>
	 * </blockquote> accesses the "City" level of the "Customers"
	 * hierarchy. Note that:<ul>
	 *
	 * <li><code>"country", "state_province"</code> are the parent keys;</li>
	 *
	 * <li><code>"city"</code> is the level key;</li>
	 *
	 * <li><code>min("customer_id") * 4 - 1</code> computes the ordinal of each
	 * city. Here, 4 is the number of levels, and 1 is the distance of the
	 * "city" level from the leaf level "Name". It will result in the following
	 * ordinals:<table>
	 * <tr><th>Ordinal</th><th>Member</th></tr>
	 * <tr><td>0</td>  <td>[All]</td></tr>
	 * <tr><td>1</td>  <td>[Canada]</td></tr>
	 * <tr><td>2</td>  <td>[Canada].[BC]</td></tr>
	 * <tr><td>3</td>  <td>[Canada].[BC].[Burnaby]</td></tr>
	 * <tr><td>4</td>  <td>[Canada].[BC].[Burnaby].[Alex Wellington]</td></tr>
	 * <tr><td>8</td>  <td>[Canada].[BC].[Burnaby].[Ana Quick]</td></tr>
	 * <tr><td>...</td><td></td></tr>
	 * <tr><td>332</td><td>[Canada].[BC].[Burnaby].[Winston Barnett]</td></tr>
	 * <tr><td>335</td><td>[Canada].[BC].[Cliffside]</td></tr>
	 * <tr><td>336</td><td>[Canada].[BC].[Cliffside].[Amanda Thomas]</td></tr>
	 * </table></li>
	 *
	 * </ul>
	 **/
	String makeLevelSql(RolapLevel level)
	{
		SqlQuery sqlQuery = newQuery(
			"while generating query to retrieve members of level " + level);
		hierarchy.addToFrom(sqlQuery, level, null);
  		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		RolapLevel lastLevel = levels[levels.length - 1];
		int levelDepth = level.getDepth();
		for (int i = 0; i <= levelDepth; i++) {
  			RolapLevel level2 = levels[i];
  			if (level2.isAll()) {
  				continue;
  			}
			String q = level2.nameExp.getExpression(sqlQuery);
			sqlQuery.addSelect(q);
			if (level != lastLevel) {
				sqlQuery.addGroupBy(q);
			}
			sqlQuery.addOrderBy(level2.ordinalExp.getExpression(sqlQuery));
		}
		/*
		if (lastLevel.ordinalExp != null) {
			String q = lastLevel.ordinalExp.getExpression(sqlQuery);
			int distanceToLeaf = levels.length - 1 - level.getDepth();
			if (distanceToLeaf > 0) {
				q = "min(" + q + ")";
				int levelCount = levels.length;
				if (hierarchy.hasAll()) {
					levelCount--;
				}
				if (levelCount > 1) {
					q = q + " * " + levelCount;
				}
				q = q + " - " + distanceToLeaf;
			}
			sqlQuery.addSelect(q);
 		}
		*/
		for (int i = 0; i < level.properties.length; i++) {
			RolapProperty property = level.properties[i];
			String q = property.exp.getExpression(sqlQuery);
			sqlQuery.addSelect(q);
		}
		return sqlQuery.toString();
	}

	// implement MemberReader
	public RolapMember[] getMembersInLevel(
		RolapLevel level, int startOrdinal, int endOrdinal)
	{
		ResultSet resultSet = null;
		final RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		final RolapLevel lastLevel = levels[levels.length - 1];
//		boolean rolap = canDoRolap();
		String sql = makeLevelSql(level);
		try {
			resultSet = RolapUtil.executeQuery(
					jdbcConnection, sql, "SqlMemberSource.getMembersInLevel");
			ArrayList list = new ArrayList();
			int ordinal = 0;
			final int levelDepth = level.getDepth();
			RolapMember allMember = null;
			if (hierarchy.hasAll()) {
				Member[] rootMembers = hierarchy.getRootMembers();
				Util.assertTrue(rootMembers.length == 1);
				allMember = (RolapMember) rootMembers[0];
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
					Object value = resultSet.getObject(column + 1);
					if (value == null) {
						value = sqlNullValue;
					}
					RolapMember parent = member;
					Object key = cache.makeKey(parent, value);
					member = cache.getMember(key);
					if (member == null) {
						member = new RolapMember(parent, level2, value);
						member.ordinal = ordinal;
						for (int j = 0; j < level.properties.length; j++) {
							RolapProperty property = level.properties[j];
							member.setProperty(
									property.getName(),
									resultSet.getObject(column + 3 + j));
						}
						cache.putMember(key, member);
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
							siblings[i].add(member);
						}
					}
					ordinal++;
					column++;
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

	// implement MemberSource
	public RolapMember[] getRootMembers()
	{
		RolapLevel level0 = (RolapLevel) hierarchy.getLevels()[0];
		if (hierarchy.hasAll()) {
			RolapMember root = new RolapMember(
				null, level0, null, hierarchy.getAllMemberName());
			root.ordinal = 0;
			return new RolapMember[] {root};
		}
		return getMembersInLevel(level0, 0, Integer.MAX_VALUE);
	}

	/**
	 * Generates the SQL statement to access the children of
	 * <code>member</code>. For example, <blockquote>
	 *
	 * <pre>SELECT "city", min("ordinal") - 1
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
			String q = level.nameExp.getExpression(sqlQuery);
			sqlQuery.addWhere(q + " = " + m.quoteKeyForSql());
			if (level.unique) {
				break; // no further qualification needed
			}
  		}

		RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
		RolapLevel level = levels[member.getLevel().getDepth() + 1];
		hierarchy.addToFrom(sqlQuery, level, null);
		String q = level.nameExp.getExpression(sqlQuery);
		sqlQuery.addSelect(q);
		sqlQuery.addGroupBy(q);
		String orderBy = level.ordinalExp.getExpression(sqlQuery);
		sqlQuery.addOrderBy(orderBy);
		if (!orderBy.equals(q)) {
			sqlQuery.addGroupBy(orderBy);
		}

		RolapLevel lastLevel = levels[levels.length - 1];
		int distanceToLeaf = levels.length - 1 - member.getLevel().getDepth();
		/*
		if (lastLevel.ordinalExp != null) {
			q = lastLevel.ordinalExp.getExpression(sqlQuery);
			if (distanceToLeaf > 0) {
				q = "min(" + q + ")";
				int levelCount = levels.length;
				if (hierarchy.hasAll()) {
					levelCount--;
				}
				if (levelCount > 1) {
					q = q + " * " + levelCount;
				}
				q = q + " - " + distanceToLeaf;
			}
			sqlQuery.addSelect(q);
		}
		*/
		return sqlQuery.toString();
	}

	// implement MemberSource
	public RolapMember[] getMemberChildren(RolapMember[] parentMembers)
	{
//		Util.assertTrue(canDoRolap());
		ArrayList list = new ArrayList();
		for (int i = 0; i < parentMembers.length; i++) {
			getMemberChildren(list, parentMembers[i]);
		}
		return RolapUtil.toArray(list);
	}

	private void getMemberChildren(ArrayList list, RolapMember parentMember)
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
			int ordinal = 0;
			while (resultSet.next()) {
				Object value = resultSet.getObject(1);
				Object key = cache.makeKey(parentMember, value);
				RolapMember member = cache.getMember(key);
				if (member == null) {
					member = new RolapMember(
						parentMember, childLevel, value);
					member.ordinal = ordinal++; //resultSet.getInt(2);
					cache.putMember(key, member);
				}
				list.add(member);
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

	// implement MemberReader
	public RolapMember[] getPeriodsToDate(RolapLevel level, RolapMember member)
	{
		throw new UnsupportedOperationException();
	}
}

// End SqlMemberSource.java
