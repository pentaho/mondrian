/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.sql.*;

import org.eigenbase.util.property.IntegerProperty;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.*;
import java.util.*;

/**
 * A <code>SqlMemberSource</code> reads members from a SQL database.
 *
 * <p>It's a good idea to put a {@link CacheMemberReader} on top of this.
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 */
class SqlMemberSource implements MemberReader, SqlTupleReader.MemberBuilder {
    private final SqlConstraintFactory sqlConstraintFactory = SqlConstraintFactory.instance();
    private final RolapHierarchy hierarchy;
    private final DataSource dataSource;
    private MemberCache cache;
    private int lastOrdinal = 0;

    SqlMemberSource(RolapHierarchy hierarchy) {
        this.hierarchy = hierarchy;
        this.dataSource =
            hierarchy.getRolapSchema().getInternalConnection().getDataSource();
    }

    // implement MemberSource
    public RolapHierarchy getHierarchy() {
        return hierarchy;
    }

    // implement MemberSource
    public boolean setCache(MemberCache cache) {
        this.cache = cache;
        return true; // yes, we support cache writeback
    }

    // implement MemberSource
    public int getMemberCount() {
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        int count = 0;
        for (int i = 0; i < levels.length; i++) {
            count += getLevelMemberCount(levels[i]);
        }
        return count;
    }

    public RolapMember lookupMember(String[] uniqueNameParts,
                                    boolean failIfNotFound) {
        throw new UnsupportedOperationException();
    }

    private int getLevelMemberCount(RolapLevel level) {
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
                        if (!colStr.equals(colStrings[i])) {
                            isEqual = false;
                        }
                        colStrings[i] = colStr;
                    }
                    if (!isEqual) {
                        count++;
                    }
                }
                return count;
            }
        } catch (SQLException e) {
            throw Util.newInternal(e,
                    "while counting members of level '" + level +
                    "'; sql=[" + sql + "]");
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


    private SqlQuery newQuery(Connection jdbcConnection, String err) {
        try {
            final SqlQuery.Dialect dialect =
                    SqlQuery.Dialect.create(jdbcConnection.getMetaData());
            return new SqlQuery(dialect);
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
     */
    private String makeLevelMemberCountSql(
            RolapLevel level,
            Connection jdbcConnection,
            boolean[] mustCount) {
        mustCount[0] = false;
        SqlQuery sqlQuery = newQuery(jdbcConnection,
                "while generating query to count members in level " + level);
        int levelDepth = level.getDepth();
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        if (levelDepth == levels.length) {
            // "select count(*) from schema.customer"
            sqlQuery.addSelect("count(*)");
            hierarchy.addToFrom(sqlQuery, level.getKeyExp());
            return sqlQuery.toString();
        }
        if (!sqlQuery.getDialect().allowsFromQuery()) {
            String columnList = "";
            int columnCount = 0;
            for (int i = levelDepth; i >= 0; i--) {
                RolapLevel level2 = levels[i];
                if (level2.isAll()) {
                     continue;
                }
                if (columnCount > 0) {
                    if (sqlQuery.getDialect().allowsCompoundCountDistinct()) {
                        columnList += ", ";
                    } else if (true) {
                        // for databases where both SELECT-in-FROM and
                        // COUNT DISTINCT do not work, we do not
                        // generate any count and do the count
                        // distinct "manually".
                        mustCount[0] = true;
                    } else if (sqlQuery.getDialect().isSybase()) {
                        // "select count(distinct convert(varchar, c1) +
                        // convert(varchar, c2)) from table"
                        if (columnCount == 1) {
                            // Conversion to varchar is expensive, so we only
                            // do it when we know we are dealing with a
                            // compound key.
                            columnList = "convert(varchar, " + columnList + ")";
                        }
                        columnList += " + ";
                    } else {
                        // Apparently this database allows neither
                        // SELECT-in-FROM nor compound COUNT DISTINCT. I don't
                        // know any database where this happens. If you receive
                        // this error, try a workaround similar to the Sybase
                        // workaround above.
                        throw Util.newInternal(
                            "Cannot generate query to count members of level '" +
                            level.getUniqueName() +
                            "': database supports neither SELECT-in-FROM nor compound COUNT DISTINCT");
                    }
                }
                hierarchy.addToFrom(sqlQuery, level2.getKeyExp());

                String keyExp = level2.getKeyExp().getExpression(sqlQuery);
                if (columnCount > 0 &&
                    !sqlQuery.getDialect().allowsCompoundCountDistinct() &&
                    sqlQuery.getDialect().isSybase()) {

                    keyExp = "convert(varchar, " + columnList + ")";
                }
                columnList += keyExp;

                if (level2.isUnique()) {
                    break; // no further qualification needed
                }
                ++columnCount;
            }
            if (mustCount[0]) {
                sqlQuery.addSelect(columnList);
                sqlQuery.addOrderBy(columnList, true, false);
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
                hierarchy.addToFrom(sqlQuery, level2.getKeyExp());
                sqlQuery.addSelect(level2.getKeyExp().getExpression(sqlQuery));
                if (level2.isUnique()) {
                    break; // no further qualification needed
                }
            }
            SqlQuery outerQuery = newQuery(jdbcConnection,
                "while generating query to count members in level " + level);
            outerQuery.addSelect("count(*)");
            // Note: the "init" is for Postgres, which requires
            // FROM-queries to have an alias
            boolean failIfExists = true;
            outerQuery.addFrom(sqlQuery, "init", failIfExists);
            return outerQuery.toString();
        }
    }


    public RolapMember[] getMembers() {
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
            List list = new ArrayList();
            Map map = new HashMap();
            RolapMember root = null;
            if (hierarchy.hasAll()) {
                root = new RolapMember(null,
                                       (RolapLevel) hierarchy.getLevels()[0],
                                       null,
                                       hierarchy.getAllMemberName(),
                                       Member.ALL_MEMBER_TYPE);
                // assign "all member" caption
                if (hierarchy.xmlHierarchy != null &&
                    hierarchy.xmlHierarchy.allMemberCaption != null &&
                        hierarchy.xmlHierarchy.allMemberCaption.length() > 0)
                    root.setCaption(hierarchy.xmlHierarchy.allMemberCaption );

                root.setOrdinal(lastOrdinal++);
                list.add(root);
            }

            int limit = MondrianProperties.instance().ResultLimit.get();
            int nFetch = 0;

            while (resultSet.next()) {

                if (limit > 0 && limit < ++nFetch) {
                    // result limit exceeded, throw an exception
                    throw MondrianResource.instance().MemberFetchLimitExceeded.ex(
                            new Long(limit));
                }

                int column = 0;
                RolapMember member = root;
                for (int i = 0; i < levels.length; i++) {
                    RolapLevel level = levels[i];
                    if (level.isAll()) {
                        continue;
                    }
                    Object value = resultSet.getObject(column + 1);
                    if (value == null) {
                        value = RolapUtil.sqlNullValue;
                    }
                    RolapMember parent = member;
                    MemberKey key = new MemberKey(parent, value);
                    member = (RolapMember) map.get(key);
                    if (member == null) {
                        member = new RolapMember(parent, level, value);
                        member.setOrdinal(lastOrdinal++);
/*
RME is this right
                        if (level.getOrdinalExp() != level.getKeyExp()) {
                            member.setOrdinal(lastOrdinal++);
                        }
*/
                        if (value == RolapUtil.sqlNullValue) {
                            addAsOldestSibling(list, member);
                        } else {
                            list.add(member);
                        }
                        map.put(key, member);
                    }
                    column++;

                    Property[] properties = level.getProperties();
                    for (int j = 0; j < properties.length; j++) {
                        Property property = properties[j];
                        member.setProperty(property.getName(),
                                        resultSet.getObject(column + 1));
                        column++;
                    }
                }
            }

            return RolapUtil.toArray(list);
        } catch (SQLException e) {
            throw Util.newInternal(e,
                    "while building member cache; sql=[" + sql + "]");
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

    private String makeKeysSql(Connection jdbcConnection) {
        SqlQuery sqlQuery = newQuery(jdbcConnection,
                "while generating query to retrieve members of " + hierarchy);
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        for (int i = 0; i < levels.length; i++) {
            RolapLevel level = levels[i];
            if (level.isAll()) {
                continue;
            }
            MondrianDef.Expression exp = level.getKeyExp();
            hierarchy.addToFrom(sqlQuery, exp);
            String expString = exp.getExpression(sqlQuery);
            sqlQuery.addSelect(expString);
            sqlQuery.addGroupBy(expString);
            exp = level.getOrdinalExp();
            hierarchy.addToFrom(sqlQuery, exp);
            expString = exp.getExpression(sqlQuery);
            sqlQuery.addOrderBy(expString, true, false);
            sqlQuery.addGroupBy(expString);

            RolapProperty[] properties = level.getRolapProperties();
            for (int j = 0; j < properties.length; j++) {
                RolapProperty property = properties[j];
                exp = property.getExp();
                hierarchy.addToFrom(sqlQuery, exp);
                expString = exp.getExpression(sqlQuery);
                sqlQuery.addSelect(expString);
                sqlQuery.addGroupBy(expString);
            }
        }
        return sqlQuery.toString();
    }

    // implement MemberReader
    public List getMembersInLevel(
            RolapLevel level,
            int startOrdinal,
            int endOrdinal) {
        TupleConstraint constraint =
                sqlConstraintFactory.getLevelMembersConstraint(null);
        return getMembersInLevel(level, startOrdinal, endOrdinal, constraint);
    }

    public List getMembersInLevel(RolapLevel level,
            int startOrdinal,
            int endOrdinal,
            TupleConstraint constraint) {
        if (level.isAll()) {
            final String allMemberName = hierarchy.getAllMemberName();
            Object key = cache.makeKey(null, allMemberName);
            RolapMember root = cache.getMember(key);
            if (root == null) {
                root = new RolapMember(null, level, null, allMemberName,
                        Member.ALL_MEMBER_TYPE);
                root.setOrdinal(lastOrdinal++);
                cache.putMember(key, root);
                if (hierarchy.xmlHierarchy != null &&
                    hierarchy.xmlHierarchy.allMemberCaption != null &&
                    hierarchy.xmlHierarchy.allMemberCaption.length() > 0) {
                    root.setCaption(hierarchy.xmlHierarchy.allMemberCaption );
                }

            }
            List list = new ArrayList(1);
            list.add(root);
            return list;
        }
        Connection jdbcConnection;
        try {
            jdbcConnection = dataSource.getConnection();
        } catch (SQLException e) {
            throw Util.newInternal(
                e, "Error while creating connection from data source");
        }
        try {
            return getMembersInLevel(level, jdbcConnection, constraint);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private List getMembersInLevel(RolapLevel level,
                                   Connection jdbcConnection,
                                   TupleConstraint constraint) {
        TupleReader tupleReader = new SqlTupleReader(constraint);
        tupleReader.addLevelMembers(level, this);
        return tupleReader.readTuples(jdbcConnection);
    }

    public RolapMember getAllMember() {
        RolapMember allMember = null;
        if (hierarchy.hasAll()) {
            final List rootMembers = getRootMembers();
            Util.assertTrue(rootMembers.size() == 1);
            allMember = (RolapMember) rootMembers.get(0);
        }
        return allMember;
    }

    public MemberCache getMemberCache() {
        return cache;
    }

    // implement MemberSource
    public List getRootMembers() {
        return getMembersInLevel(
                (RolapLevel) hierarchy.getLevels()[0],
                0,
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
     * <code>[Canada].[BC]</code>.
     *
     * <p>See also {@link SqlTupleReader#makeLevelMembersSql}.
     */
    String makeChildMemberSql(
            RolapMember member,
            Connection jdbcConnection,
            MemberChildrenConstraint constraint) {
        SqlQuery sqlQuery = newQuery(jdbcConnection,
                "while generating query to retrieve children of member "
                + member);

        // create the condition, which is either the parent member or
        // the full context (non empty).
        constraint.addMemberConstraint(sqlQuery, member);

        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        RolapLevel level = levels[member.getLevel().getDepth() + 1];
        hierarchy.addToFrom(sqlQuery, level.getKeyExp());
        String q = level.getKeyExp().getExpression(sqlQuery);
        sqlQuery.addSelect(q);
        sqlQuery.addGroupBy(q);

        // in non empty mode the level table must be joined to the fact table
        constraint.addLevelConstraint(sqlQuery, level, null);

        if (level.hasCaptionColumn()){
            MondrianDef.Expression captionExp = level.getCaptionExp();
            hierarchy.addToFrom(sqlQuery, captionExp);
            String captionSql = captionExp.getExpression(sqlQuery);
            sqlQuery.addSelect(captionSql);
            sqlQuery.addGroupBy(captionSql);
        }

        hierarchy.addToFrom(sqlQuery, level.getOrdinalExp());
        String orderBy = level.getOrdinalExp().getExpression(sqlQuery);
        sqlQuery.addOrderBy(orderBy, true, false);
        if (!orderBy.equals(q)) {
            sqlQuery.addGroupBy(orderBy);
        }

        RolapProperty[] properties = level.getRolapProperties();
        for (int j = 0; j < properties.length; j++) {
            RolapProperty property = properties[j];
            final MondrianDef.Expression exp = property.getExp();
            hierarchy.addToFrom(sqlQuery, exp);
            final String s = exp.getExpression(sqlQuery);
            sqlQuery.addSelect(s);
            sqlQuery.addGroupBy(s);
        }
        return sqlQuery.toString();
    }

    public void getMemberChildren(List parentMembers, List children) {
        MemberChildrenConstraint constraint = sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMembers, children, constraint);
    }

    public void getMemberChildren(List parentMembers, List children, MemberChildrenConstraint mcc) {
        // try to fetch all children at once
        RolapLevel childLevel = getCommonChildLevelForDescendants(parentMembers);
        if (childLevel != null) {
            TupleConstraint lmc = sqlConstraintFactory.getDescendantsConstraint(parentMembers, mcc);
            List list = getMembersInLevel(childLevel, 0, Integer.MAX_VALUE, lmc);
            children.addAll(list);
            return;
        }

        // fetch them one by one
        for (int i = 0; i < parentMembers.size(); i++) {
            getMemberChildren((RolapMember) parentMembers.get(i), children, mcc);
        }
    }

    public void getMemberChildren(RolapMember parentMember, List children) {
        MemberChildrenConstraint constraint = sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMember, children, constraint);
    }

    public void getMemberChildren(RolapMember parentMember, List children, MemberChildrenConstraint constraint) {
//        if (!parentMember.isAll() && parentMember.isCalculated())
//            return;
        Connection jdbcConnection;
        try {
            jdbcConnection = dataSource.getConnection();
        } catch (SQLException e) {
            throw Util.newInternal(
                e, "Error while creating connection from data source");
        }
        try {
            getMemberChildren(parentMember, children, jdbcConnection, constraint);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * If all parents belong to the same level and no parent/child is involved,
     * returns that level; this indicates that all member children can be
     * fetched at once. Otherwise returns null.
     */
    private RolapLevel getCommonChildLevelForDescendants(List parents) {
        // at least two members required
        if (parents.size() < 2) {
            return null;
        }
        RolapLevel parentLevel = null;
        RolapLevel childLevel = null;
        for (Iterator it = parents.iterator(); it.hasNext();) {
            RolapMember member = (RolapMember) it.next();
            // we can not fetch children of calc members
            if (member.isCalculated()) {
                return null;
            }
            // first round?
            if (parentLevel == null) {
                parentLevel = (RolapLevel) member.getLevel();
                // check for parent/child
                if (parentLevel.isParentChild()) {
                    return null;
                }
                childLevel = getChildLevel(parentLevel);
                if (childLevel == null) {
                    return null;
                }
                if (childLevel.isParentChild()) {
                    return null;
                }
            } else if (parentLevel != member.getLevel()) {
                return null;
            }
        }
        return childLevel;
    }

    /**
     * @deprecated use LevelBase#getChildLevel instead
     */
    private RolapLevel getChildLevel(RolapLevel parentLevel) {
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        int childDepth = parentLevel.getDepth() + 1;
        if (childDepth >= levels.length) {
            // member is at last level, so can have no children
            return null;
        }
        return levels[childDepth];
    }

    private void getMemberChildren(RolapMember parentMember,
                                   List children,
                                   Connection jdbcConnection,
                                   MemberChildrenConstraint constraint) {
        String sql;
        boolean parentChild;
        final RolapLevel parentLevel = (RolapLevel) parentMember.getLevel();
        RolapLevel childLevel;
        if (parentLevel.isParentChild()) {
            sql = makeChildMemberSqlPC(parentMember, jdbcConnection);
            parentChild = true;
            childLevel = parentLevel;
        } else {
//            RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
//            int childDepth = parentLevel.getDepth() + 1;
//            if (childDepth >= levels.length) {
//                // member is at last level, so can have no children
//                return;
//            }
//            childLevel = levels[childDepth];
            childLevel = getChildLevel(parentLevel);
            if (childLevel == null) {
                // member is at last level, so can have no children
                return;
            }
            if (childLevel.isParentChild()) {
                sql = makeChildMemberSql_PCRoot(parentMember, jdbcConnection);
                parentChild = true;
            } else {
                sql = makeChildMemberSql(parentMember, jdbcConnection, constraint);
                parentChild = false;
            }
        }
        ResultSet resultSet = null;
        try {
            resultSet = RolapUtil.executeQuery(
                jdbcConnection, sql, "SqlMemberSource.getMemberChildren");

            IntegerProperty ip = MondrianProperties.instance().ResultLimit;
            int limit = ip.get();
            int nFetch = 0;

            while (resultSet.next()) {

                if (limit > 0 && limit < ++nFetch) {
                    // result limit exceeded, throw an exception
                    throw MondrianResource.instance().
                            MemberFetchLimitExceeded.ex(new Long(limit));
                }

                Object value = resultSet.getObject(1);
                if (value == null) {
                    value = RolapUtil.sqlNullValue;
                }
                Object captionValue;
                if (childLevel.hasCaptionColumn()){
                    captionValue=resultSet.getObject(2);
                } else {
                    captionValue = null;
                }
                Object key = cache.makeKey(parentMember, value);
                RolapMember member = cache.getMember(key);
                if (member == null) {
                    member = makeMember(
                            parentMember, childLevel, value, captionValue,
                            parentChild, resultSet, key, 1);
                }
                if (value == RolapUtil.sqlNullValue) {
                    addAsOldestSibling(children, member);
                } else {
                    children.add(member);
                }
            }
        } catch (SQLException e) {
            throw Util.newInternal(e,
                    "while building member cache; sql=[" + sql + "]");
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

    public RolapMember makeMember(
            RolapMember parentMember,
            RolapLevel childLevel,
            Object value,
            Object captionValue,
            boolean parentChild,
            ResultSet resultSet,
            Object key,
            int columnOffset)
            throws SQLException {

        RolapMember member = new RolapMember(parentMember, childLevel, value);
        if (childLevel.getOrdinalExp() != childLevel.getKeyExp()) {
            member.setOrdinal(lastOrdinal++);
        }
        if (captionValue != null) {
            member.setCaption(captionValue.toString());
        }
        if (parentChild) {
            // Create a 'public' and a 'data' member. The public member is
            // calculated, and its value is the aggregation of the data member
            // and all of the children. The children and the data member belong
            // to the parent member; the data member does not have any
            // children.
            final RolapParentChildMember parentChildMember =
                childLevel.hasClosedPeer() ?
                    new RolapParentChildMember(
                            parentMember, childLevel, value, member)
                    : new RolapParentChildMemberNoClosure(
                            parentMember, childLevel, value, member);

            member = parentChildMember;
        }
        Property[] properties = childLevel.getProperties();
        for (int j = 0; j < properties.length; j++) {
            Property property = properties[j];
            member.setProperty(
                    property.getName(),
                    resultSet.getObject(columnOffset + j + 1));
        }
        cache.putMember(key, member);
        return member;
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
        SqlQuery sqlQuery = newQuery(jdbcConnection,
            "while generating query to retrieve children of parent/child hierarchy member " + member);
        Util.assertTrue(member.isAll(), "In the current implementation, parent/child hierarchies must have only one level (plus the 'All' level).");

        RolapLevel level = (RolapLevel) member.getLevel().getChildLevel();

        Util.assertTrue(!level.isAll(), "all level cannot be parent-child");
        Util.assertTrue(level.isUnique(), "parent-child level '"
            + level + "' must be unique");

        hierarchy.addToFrom(sqlQuery, level.getParentExp());
        String parentId = level.getParentExp().getExpression(sqlQuery);
        StringBuffer condition = new StringBuffer(64);
        condition.append(parentId);
        if (level.getNullParentValue() == null ||
                level.getNullParentValue().equalsIgnoreCase("NULL")) {
            condition.append(" IS NULL");
        } else {
            // Quote the value if it doesn't seem to be a number.
            try {
                Util.discard(Double.parseDouble(level.getNullParentValue()));
                condition.append(" = ");
                condition.append(level.getNullParentValue());
            } catch (NumberFormatException e) {
                condition.append(" = ");
                Util.singleQuoteString(level.getNullParentValue(), condition);
            }
        }
        sqlQuery.addWhere(condition.toString());
        hierarchy.addToFrom(sqlQuery, level.getKeyExp());
        String childId = level.getKeyExp().getExpression(sqlQuery);
        sqlQuery.addSelect(childId);
        sqlQuery.addGroupBy(childId);
        hierarchy.addToFrom(sqlQuery, level.getOrdinalExp());
        String orderBy = level.getOrdinalExp().getExpression(sqlQuery);
        sqlQuery.addOrderBy(orderBy, true, false);
        if (!orderBy.equals(childId)) {
            sqlQuery.addGroupBy(orderBy);
        }

        RolapProperty[] properties = level.getRolapProperties();
        for (int j = 0; j < properties.length; j++) {
            RolapProperty property = properties[j];
            final MondrianDef.Expression exp = property.getExp();
            hierarchy.addToFrom(sqlQuery, exp);
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
     * <code>[Employee].[5]</code>.
     *
     * <p>See also {@link SqlTupleReader#makeLevelMembersSql}.
     */
    private String makeChildMemberSqlPC(
            RolapMember member,
            Connection jdbcConnection) {
        SqlQuery sqlQuery = newQuery(jdbcConnection,
            "while generating query to retrieve children of parent/child hierarchy member " + member);
        RolapLevel level = (RolapLevel) member.getLevel();

        Util.assertTrue(!level.isAll(), "all level cannot be parent-child");
        Util.assertTrue(level.isUnique(), "parent-child level '"
            + level + "' must be unique");

        hierarchy.addToFrom(sqlQuery, level.getParentExp());
        String parentId = level.getParentExp().getExpression(sqlQuery);
        sqlQuery.addWhere(parentId, " = ", member.quoteKeyForSql());

        hierarchy.addToFrom(sqlQuery, level.getKeyExp());
        String childId = level.getKeyExp().getExpression(sqlQuery);
        sqlQuery.addSelect(childId);
        sqlQuery.addGroupBy(childId);
        hierarchy.addToFrom(sqlQuery, level.getOrdinalExp());
        String orderBy = level.getOrdinalExp().getExpression(sqlQuery);
        sqlQuery.addOrderBy(orderBy, true, false);
        if (!orderBy.equals(childId)) {
            sqlQuery.addGroupBy(orderBy);
        }

        RolapProperty[] properties = level.getRolapProperties();
        for (int j = 0; j < properties.length; j++) {
            RolapProperty property = properties[j];
            final MondrianDef.Expression exp = property.getExp();
            hierarchy.addToFrom(sqlQuery, exp);
            final String s = exp.getExpression(sqlQuery);
            sqlQuery.addSelect(s);
            sqlQuery.addGroupBy(s);
        }
        return sqlQuery.toString();
    }

    // implement MemberReader
    public RolapMember getLeadMember(RolapMember member, int n) {
        throw new UnsupportedOperationException();
    }

    public void getMemberRange(RolapLevel level,
                               RolapMember startMember,
                               RolapMember endMember,
                               List memberList) {
        throw new UnsupportedOperationException();
    }

    public int compare(RolapMember m1,
                       RolapMember m2,
                       boolean siblingsAreEqual) {
        throw new UnsupportedOperationException();
    }

    /**
     * Member of a parent-child dimension which has a closure table.
     *
     * <p>When looking up cells, this member will automatically be converted
     * to a corresponding member of the auxiliary dimension which maps onto
     * the closure table.
     */
    private static class RolapParentChildMember extends RolapMember {
        private final RolapMember dataMember;
        private int depth = 0;
        public RolapParentChildMember(RolapMember parentMember,
                                      RolapLevel childLevel,
                                      Object value,
                                      RolapMember dataMember) {
            super(parentMember, childLevel, value);
            this.dataMember = dataMember;
            this.depth = (parentMember != null)
                ? parentMember.getDepth() + 1
                : 0;
        }

        public Member getDataMember() {
            return dataMember;
        }

        public Object getPropertyValue(String name) {
            if (name.equals(Property.CONTRIBUTING_CHILDREN.name)) {
                List list = new ArrayList();
                list.add(dataMember);
                RolapHierarchy hierarchy = (RolapHierarchy) getHierarchy();
                hierarchy.getMemberReader().getMemberChildren(this, list);
                return list;
            } else {
                return super.getPropertyValue(name);
            }
        }

        /**
         * @return the members's depth
         * @see mondrian.olap.Member#getDepth()
         */
        public int getDepth() {
            return depth;
        }
    }

    /**
     * Member of a parent-child dimension which has no closure table.
     *
     * <p>This member is calculcated. When you ask for its value, it returns
     * an expression which aggregates the values of its child members.
     * This calculation is very inefficient, and we can only support
     * aggregatable measures ("count distinct" is non-aggregatable).
     * Unfortunately it's the best we can do without a closure table.
     */
    private static class RolapParentChildMemberNoClosure
        extends RolapParentChildMember {

        public RolapParentChildMemberNoClosure(RolapMember parentMember,
                RolapLevel childLevel, Object value, RolapMember dataMember) {
            super(parentMember, childLevel, value, dataMember);
        }

        public boolean isCalculated() {
            return true;
        }

        public Exp getExpression() {
            final RolapHierarchy hierarchy = (RolapHierarchy) getHierarchy();
            return hierarchy.getAggregateChildrenExpression();
        }
    }

    public TupleReader.MemberBuilder getMemberBuilder() {
        return this;
    }
}

// End SqlMemberSource.java
