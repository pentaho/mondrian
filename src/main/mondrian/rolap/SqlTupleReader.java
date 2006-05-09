/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

/**
 * reads the members of a single level (level.members) or of multiple levels (crossjoin).
 * <p>
 * Allows the result to be restricted by a {@link mondrian.rolap.sql.TupleConstraint}. So
 * the SqlTupleReader can also read Member.Descendants (which is level.members restricted
 * to a common parent) and member.children (which is a special case of member.descendants).
 * Other constraints, especially for the current slicer or evaluation context, are possible.
 *
 * <p>
 * Caching:
 * <p>
 * When this class reads level.members, it groups the result into parent/children pairs
 * and puts them into the cache. In order that these can be found later when the
 * children of a parent are requested, a matching constraint must be provided for
 * every parent.
 *
 * <ul>
 * <li>When reading members from a single level, then the constraint is not required to
 * join the fact table in
 * {@link mondrian.rolap.sql.TupleConstraint#addLevelConstraint(SqlQuery, RolapLevel)}
 * although it may do so to restrict the result. Also it is permitted to cache the
 * parent/children from all members in MemberCache, so
 * {@link mondrian.rolap.sql.TupleConstraint#getMemberChildrenConstraint(RolapMember)}
 * should not return null.</li>
 *
 * <li>When reading multiple levels (i.e. we are performing a crossjoin),
 * then we can not store the parent/child pairs in the MemberCache and
 * {@link mondrian.rolap.sql.TupleConstraint#getMemberChildrenConstraint(RolapMember)}
 * must return null. Also
 * {@link mondrian.rolap.sql.TupleConstraint#addLevelConstraint(SqlQuery, RolapLevel)}
 * is required to join the fact table for the levels table.</li>
 * </ul>
 *
 * @author av
 * @since Nov 11, 2005
 */
public class SqlTupleReader implements TupleReader {
    TupleConstraint constraint;
    List targets = new ArrayList();
    int maxRows = 0;

    private class Target {
        RolapLevel level;
        RolapMember allMember;
        MemberCache cache;

        RolapLevel[] levels;
        RolapHierarchy hierarchy;
        List list;
        int levelDepth;
        boolean parentChild;
        RolapMember[] members;
        List[] siblings;
        MemberBuilder memberBuilder;

        public Target(RolapLevel level, MemberBuilder memberBuilder) {
            this.level = level;
            this.allMember = memberBuilder.getAllMember();
            this.cache = memberBuilder.getMemberCache();
            this.memberBuilder = memberBuilder;
        }

        public void open() {
            hierarchy = (RolapHierarchy) level.getHierarchy();
            levels = (RolapLevel[]) hierarchy.getLevels();
            list = new ArrayList();
            levelDepth = level.getDepth();
            parentChild = level.isParentChild();
            // members[i] is the current member of level#i, and siblings[i]
            // is the current member of level#i plus its siblings
            members = new RolapMember[levels.length];
            siblings = new ArrayList[levels.length + 1];
        }

        /**
         * scans a row of the resultset and creates a member
         * for the result
         *
         * @param column the column index to start with
         * @return index of the last column read + 1
         * @throws SQLException
         */
        public int addRow(ResultSet resultSet, int column) throws SQLException {
            synchronized (cache) {
                return internalAddRow(resultSet, column);
            }
        }

        private int internalAddRow(ResultSet resultSet, int column) throws SQLException {
            RolapMember member = null;
            for (int i = 0; i <= levelDepth; i++) {
                RolapLevel childLevel = levels[i];
                if (childLevel.isAll()) {
                    member = allMember;
                    continue;
                }
                Object value = resultSet.getObject(++column);
                if (value == null) {
                    value = RolapUtil.sqlNullValue;
                }
                Object captionValue;
                if (childLevel.hasCaptionColumn()) {
                    captionValue = resultSet.getObject(++column);
                } else {
                    captionValue = null;
                }
                RolapMember parentMember = member;
                Object key = cache.makeKey(parentMember, value);
                member = cache.getMember(key);
                if (member == null) {
                    member = memberBuilder.makeMember(parentMember, childLevel, value,
                            captionValue, parentChild, resultSet, key, column);
                }
                column += childLevel.getProperties().length;
                if (member != members[i]) {
                    // Flush list we've been building.
                    List children = siblings[i + 1];
                    if (children != null) {
                        MemberChildrenConstraint mcc = constraint
                                .getMemberChildrenConstraint(members[i]);
                        if (mcc != null)
                            cache.putChildren(members[i], mcc, children);
                    }
                    // Start a new list, if the cache needs one. (We don't
                    // synchronize, so it's possible that the cache will
                    // have one by the time we complete it.)
                    MemberChildrenConstraint mcc = constraint
                            .getMemberChildrenConstraint(member);
                    // we keep a reference to cachedChildren so they don't get garbage collected
                    List cachedChildren = cache.getChildrenFromCache(member, mcc);
                    if (i < levelDepth && cachedChildren == null) {
                        siblings[i + 1] = new ArrayList();
                    } else {
                        siblings[i + 1] = null; // don't bother building up a list
                    }
                    // Record new current member of this level.
                    members[i] = member;
                    // If we're building a list of siblings at this level,
                    // we haven't seen this one before, so add it.
                    if (siblings[i] != null) {
                        if (value == RolapUtil.sqlNullValue) {
                            addAsOldestSibling(siblings[i], member);
                        } else {
                            siblings[i].add(member);
                        }
                    }
                }
            }
            list.add(member);
            return column;
        }

        public List close() {
            synchronized (cache) {
                return internalClose();
            }
        }

        /**
         * clean up after all rows have been processed and return the list of members.
         */
        public List internalClose() {
            for (int i = 0; i < members.length; i++) {
                RolapMember member = members[i];
                final List children = siblings[i + 1];
                if (member != null && children != null) {
                    MemberChildrenConstraint mcc = constraint
                            .getMemberChildrenConstraint(member);
                    if (mcc != null)
                        cache.putChildren(member, mcc, children);
                }
            }
            return list;
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

        public RolapLevel getLevel() {
            return level;
        }

        public String toString() {
            return level.getUniqueName();
        }

    }

    public SqlTupleReader(TupleConstraint constraint) {
        this.constraint = constraint;
    }

    public void addLevelMembers(RolapLevel level, MemberBuilder memberBuilder) {
        targets.add(new Target(level, memberBuilder));
    }

    public Object getCacheKey() {
        List key = new ArrayList();
        key.add(constraint.getCacheKey());
        key.add(SqlTupleReader.class);
        for (Iterator it = targets.iterator(); it.hasNext();) {
            Target t = (Target) it.next();
            key.add(t.getLevel());
        }
        return key;
    }

    public List readTuples(DataSource dataSource) {
        Connection con;
        try {
            con = dataSource.getConnection();
        } catch (SQLException e) {
            throw Util.newInternal(e, "could not connect to DB");
        }
        try {
            return readTuples(con);
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                throw Util.newInternal(e, "could not close connection");
            }
        }
    }

    public List readTuples(Connection jdbcConnection) {

        String sql = makeLevelMembersSql(jdbcConnection);
        ResultSet resultSet = null;
        try {
            resultSet = RolapUtil.executeQuery(jdbcConnection, sql, maxRows,
                    "SqlTupleReader.readTuples " + targets);

            for (Iterator it = targets.iterator(); it.hasNext();) {
                ((Target) it.next()).open();
            }

            int limit = MondrianProperties.instance().ResultLimit.get();
            int nFetch = 0;

            while (resultSet.next()) {

                if (limit > 0 && limit < ++nFetch) {
                    // result limit exceeded, throw an exception
                    throw MondrianResource.instance().MemberFetchLimitExceeded
                            .ex(new Long(limit));
                }

                int column = 0;
                for (Iterator it = targets.iterator(); it.hasNext();) {
                    Target t = (Target) it.next();
                    column = t.addRow(resultSet, column);
                }
            }

            final int n = targets.size();
            if (n == 1) {
                // List of members
                return ((Target) targets.get(0)).close();
            }

            // List of tuples
            List tupleList = new ArrayList();
            Iterator[] iter = new Iterator[n];
            for (int i = 0; i < n; i++) {
                Target t = (Target) targets.get(i);
                iter[i] = t.close().iterator();
            }
            while (iter[0].hasNext()) {
                RolapMember[] tuples = new RolapMember[n];
                for (int i = 0; i < n; i++) {
                    tuples[i] = (RolapMember) iter[i].next();
                }
                tupleList.add(tuples);
            }
            return tupleList;

        } catch (Throwable e) {
            throw Util.newInternal(e, "while populating member cache with members for "
                    + targets + "'; sql=[" + sql + "]");
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

    String makeLevelMembersSql(Connection jdbcConnection) {
        String s = "while generating query to retrieve members of level(s) " + targets;
        SqlQuery sqlQuery = newQuery(jdbcConnection, s);
        // add the selects for all levels to fetch
        for (Iterator it = targets.iterator(); it.hasNext();) {
            Target t = (Target) it.next();
            addLevelMemberSql(sqlQuery, t.getLevel());
        }

        // additional constraints
        constraint.addConstraint(sqlQuery);

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
     */
    void addLevelMemberSql(SqlQuery sqlQuery, RolapLevel level) {
        RolapHierarchy hierarchy = (RolapHierarchy) level.getHierarchy();

        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        int levelDepth = level.getDepth();
        for (int i = 0; i <= levelDepth; i++) {
            RolapLevel level2 = levels[i];
            if (level2.isAll()) {
                continue;
            }
            hierarchy.addToFrom(sqlQuery, level2.getKeyExp());
            String keySql = level2.getKeyExp().getExpression(sqlQuery);
            sqlQuery.addSelect(keySql);
            sqlQuery.addGroupBy(keySql);
            hierarchy.addToFrom(sqlQuery, level2.getOrdinalExp());

            constraint.addLevelConstraint(sqlQuery, level2);

            if (level2.hasCaptionColumn()) {
                MondrianDef.Expression captionExp = level2.getCaptionExp();
                hierarchy.addToFrom(sqlQuery, captionExp);
                String captionSql = captionExp.getExpression(sqlQuery);
                sqlQuery.addSelect(captionSql);
                sqlQuery.addGroupBy(captionSql);
            }

            String ordinalSql = level2.getOrdinalExp().getExpression(sqlQuery);
            sqlQuery.addGroupBy(ordinalSql);
            sqlQuery.addOrderBy(ordinalSql, true, false);
            RolapProperty[] properties = level2.getRolapProperties();
            for (int j = 0; j < properties.length; j++) {
                RolapProperty property = properties[j];
                String propSql = property.getExp().getExpression(sqlQuery);
                sqlQuery.addSelect(propSql);
                sqlQuery.addGroupBy(propSql);
            }
        }
    }

    static SqlQuery newQuery(Connection jdbcConnection, String err) {
        try {
            final SqlQuery.Dialect dialect =
                    SqlQuery.Dialect.create(jdbcConnection.getMetaData());
            return new SqlQuery(dialect);
        } catch (SQLException e) {
            throw Util.newInternal(e, err);
        }
    }

    int getMaxRows() {
        return maxRows;
    }

    void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }
}

// End SqlTupleReader.java
