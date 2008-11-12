/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Reads the members of a single level (level.members) or of multiple levels
 * (crossjoin).
 *
 * <p>Allows the result to be restricted by a {@link TupleConstraint}. So
 * the SqlTupleReader can also read Member.Descendants (which is level.members
 * restricted to a common parent) and member.children (which is a special case
 * of member.descendants). Other constraints, especially for the current slicer
 * or evaluation context, are possible.
 *
 * <h3>Caching</h3>
 *
 * <p>When a SqlTupleReader reads level.members, it groups the result into
 * parent/children pairs and puts them into the cache. In order that these can
 * be found later when the children of a parent are requested, a matching
 * constraint must be provided for every parent.
 *
 * <ul>
 *
 * <li>When reading members from a single level, then the constraint is not
 * required to join the fact table in
 * {@link TupleConstraint#addLevelConstraint} although it may do so to restrict
 * the result. Also it is permitted to cache the parent/children from all
 * members in MemberCache, so
 * {@link TupleConstraint#getMemberChildrenConstraint(RolapMember)}
 * should not return null.</li>
 *
 * <li>When reading multiple levels (i.e. we are performing a crossjoin),
 * then we can not store the parent/child pairs in the MemberCache and
 * {@link TupleConstraint#getMemberChildrenConstraint(RolapMember)}
 * must return null. Also
 * {@link TupleConstraint#addConstraint(mondrian.rolap.sql.SqlQuery, mondrian.rolap.RolapCube)}
 * is required to join the fact table for the levels table.</li>
 * </ul>
 *
 * @author av
 * @since Nov 11, 2005
 * @version $Id$
 */
public class SqlTupleReader implements TupleReader {
    protected final TupleConstraint constraint;
    List<Target> targets = new ArrayList<Target>();
    int maxRows = 0;

    /**
     * TODO: Document this class.
     */
    private class Target {
        final RolapLevel level;
        final MemberCache cache;
        final Object cacheLock;

        RolapLevel[] levels;
        List<RolapMember> list;
        int levelDepth;
        boolean parentChild;
        List<RolapMember> members;
        List<List<RolapMember>> siblings;
        final MemberBuilder memberBuilder;
        // if set, the rows for this target come from the array rather
        // than native sql
        private final List<RolapMember> srcMembers;
        // current member within the current result set row
        // for this target
        private RolapMember currMember;

        public Target(
            RolapLevel level, MemberBuilder memberBuilder,
            List<RolapMember> srcMembers) {
            this.level = level;
            this.cache = memberBuilder.getMemberCache();
            this.cacheLock = memberBuilder.getMemberCacheLock();
            this.memberBuilder = memberBuilder;
            this.srcMembers = srcMembers;
        }

        public void open() {
            levels = (RolapLevel[]) level.getHierarchy().getLevels();
            list = new ArrayList<RolapMember>();
            levelDepth = level.getDepth();
            parentChild = level.isParentChild();
            // members[i] is the current member of level#i, and siblings[i]
            // is the current member of level#i plus its siblings
            members = new ArrayList<RolapMember>();
            for (int i = 0; i < levels.length; i++) {
                members.add(null);
            }
            siblings = new ArrayList<List<RolapMember>>();
            for (int i = 0; i < levels.length + 1; i++) {
                siblings.add(new ArrayList<RolapMember>());
            }
        }

        /**
         * Scans a row of the resultset and creates a member
         * for the result.
         *
         * @param resultSet result set to retrieve rows from
         * @param column the column index to start with
         *
         * @return index of the last column read + 1
         * @throws SQLException
         */
        public int addRow(ResultSet resultSet, int column) throws SQLException {
            synchronized (cacheLock) {
                return internalAddRow(resultSet, column);
            }
        }

        private int internalAddRow(ResultSet resultSet, int column) throws SQLException {
            RolapMember member = null;
            if (currMember != null) {
                member = currMember;
            } else {
                boolean checkCacheStatus = true;
                for (int i = 0; i <= levelDepth; i++) {
                    RolapLevel childLevel = levels[i];
                    if (childLevel.isAll()) {
                        member = level.getHierarchy().getAllMember();
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
                    member = cache.getMember(key, checkCacheStatus);
                    checkCacheStatus = false; /* Only check the first time */
                    if (member == null) {
                        member = memberBuilder.makeMember(
                            parentMember, childLevel, value, captionValue,
                            parentChild, resultSet, key, column);
                    }

                    // Skip over the columns consumed by makeMember
                    if (!childLevel.getOrdinalExp().equals(
                        childLevel.getKeyExp()))
                    {
                        ++column;
                    }
                    column += childLevel.getProperties().length;

                    if (member != members.get(i)) {
                        // Flush list we've been building.
                        List<RolapMember> children = siblings.get(i + 1);
                        if (children != null) {
                            MemberChildrenConstraint mcc =
                                constraint.getMemberChildrenConstraint(
                                    members.get(i));
                            if (mcc != null) {
                                cache.putChildren(members.get(i), mcc, children);
                            }
                        }
                        // Start a new list, if the cache needs one. (We don't
                        // synchronize, so it's possible that the cache will
                        // have one by the time we complete it.)
                        MemberChildrenConstraint mcc =
                            constraint.getMemberChildrenConstraint(member);
                        // we keep a reference to cachedChildren so they don't
                        // get garbage-collected
                        List cachedChildren =
                            cache.getChildrenFromCache(member, mcc);
                        if (i < levelDepth && cachedChildren == null) {
                            siblings.set(i + 1, new ArrayList<RolapMember>());
                        } else {
                            // don't bother building up a list
                            siblings.set(i + 1,  null);
                        }
                        // Record new current member of this level.
                        members.set(i, member);
                        // If we're building a list of siblings at this level,
                        // we haven't seen this one before, so add it.
                        if (siblings.get(i) != null) {
                            if (value == RolapUtil.sqlNullValue) {
                                addAsOldestSibling(siblings.get(i), member);
                            } else {
                                ((List)siblings.get(i)).add(member);
                            }
                        }
                    }
                }
                currMember = member;
            }
            ((List)list).add(member);
            return column;
        }

        public List<RolapMember> close() {
            synchronized (cacheLock) {
                return internalClose();
            }
        }

        /**
         * Cleans up after all rows have been processed, and returns the list of
         * members.
         *
         * @return list of members
         */
        public List<RolapMember> internalClose() {
            for (int i = 0; i < members.size(); i++) {
                RolapMember member = members.get(i);
                final List<RolapMember> children = siblings.get(i + 1);
                if (member != null && children != null) {
                    // If we are finding the members of a particular level, and
                    // we happen to find some of the children of an ancestor of
                    // that level, we can't be sure that we have found all of
                    // the children, so don't put them in the cache.
                    if (member.getDepth() < level.getDepth()) {
                        continue;
                    }
                    MemberChildrenConstraint mcc =
                        constraint.getMemberChildrenConstraint(member);
                    if (mcc != null) {
                        cache.putChildren(member, mcc, children);
                    }
                }
            }
            return list;
        }

        /**
         * Adds <code>member</code> just before the first element in
         * <code>list</code> which has the same parent.
         */
        private void addAsOldestSibling(List<RolapMember> list, RolapMember member) {
            int i = list.size();
            while (--i >= 0) {
                RolapMember sibling = list.get(i);
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

    public void addLevelMembers(
        RolapLevel level,
        MemberBuilder memberBuilder,
        List<RolapMember> srcMembers)
    {
        targets.add(new Target(level, memberBuilder, srcMembers));
    }

    public Object getCacheKey() {
        List<Object> key = new ArrayList<Object>();
        key.add(constraint.getCacheKey());
        key.add(SqlTupleReader.class);
        for (Target target : targets) {
            // don't include the level in the key if the target isn't
            // processed through native sql
            if (target.srcMembers != null) {
                key.add(target.getLevel());
            }
        }
        return key;
    }

    /**
     * @return number of targets that contain enumerated sets with calculated
     * members
     */
    public int getEnumTargetCount()
    {
        int enumTargetCount = 0;
        for (Target target : targets) {
            if (target.srcMembers != null) {
                enumTargetCount++;
            }
        }
        return enumTargetCount;
    }

    protected void prepareTuples(
        DataSource dataSource,
        List<List<RolapMember>> partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        String message = "Populating member cache with members for " + targets;
        SqlStatement stmt = null;
        final ResultSet resultSet;
        boolean execQuery = (partialResult == null);
        try {
            if (execQuery) {
                // we're only reading tuples from the targets that are
                // non-enum targets
                List<Target> partialTargets = new ArrayList<Target>();
                for (Target target : targets) {
                    if (target.srcMembers == null) {
                        partialTargets.add(target);
                    }
                }
                String sql = makeLevelMembersSql(dataSource);
                assert sql != null && !sql.equals("");
                stmt = RolapUtil.executeQuery(
                    dataSource, sql, maxRows,
                    "SqlTupleReader.readTuples " + partialTargets,
                    message,
                    -1, -1);
                resultSet = stmt.getResultSet();
            } else {
                resultSet = null;
            }

            for (Target target : targets) {
                target.open();
            }

            int limit = MondrianProperties.instance().ResultLimit.get();
            int fetchCount = 0;

            // determine how many enum targets we have
            int enumTargetCount = getEnumTargetCount();
            int[] srcMemberIdxes = null;
            if (enumTargetCount > 0) {
                srcMemberIdxes = new int[enumTargetCount];
            }

            boolean moreRows;
            int currPartialResultIdx = 0;
            if (execQuery) {
                moreRows = resultSet.next();
                if (moreRows) {
                    ++stmt.rowCount;
                }
            } else {
                moreRows = currPartialResultIdx < partialResult.size();
            }
            while (moreRows) {
                if (limit > 0 && limit < ++fetchCount) {
                    // result limit exceeded, throw an exception
                    throw MondrianResource.instance().MemberFetchLimitExceeded
                            .ex((long) limit);
                }

                if (enumTargetCount == 0) {
                    int column = 0;
                    for (Target target : targets) {
                        target.currMember = null;
                        column = target.addRow(resultSet, column);
                    }
                } else {
                    // find the first enum target, then call addTargets()
                    // to form the cross product of the row from resultSet
                    // with each of the list of members corresponding to
                    // the enumerated targets
                    int firstEnumTarget = 0;
                    for (; firstEnumTarget < targets.size();
                        firstEnumTarget++)
                    {
                        if (targets.get(firstEnumTarget).srcMembers != null) {
                            break;
                        }
                    }
                    List<RolapMember> partialRow;
                    if (execQuery) {
                        partialRow = null;
                    } else {
                        partialRow = partialResult.get(currPartialResultIdx);
                    }
                    resetCurrMembers(partialRow);
                    addTargets(
                        0, firstEnumTarget, enumTargetCount, srcMemberIdxes,
                        resultSet, message);
                    if (newPartialResult != null) {
                        savePartialResult(newPartialResult);
                    }
                }

                if (execQuery) {
                    moreRows = resultSet.next();
                    if (moreRows) {
                        ++stmt.rowCount;
                    }
                } else {
                    currPartialResultIdx++;
                    moreRows = currPartialResultIdx < partialResult.size();
                }
            }
        } catch (SQLException e) {
            if (stmt == null) {
                throw Util.newError(e, message);
            } else {
                stmt.handle(e);
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public List<RolapMember> readMembers(
        DataSource dataSource,
        List<List<RolapMember>> partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        prepareTuples(dataSource, partialResult, newPartialResult);
        assert targets.size() == 1;
        return targets.get(0).close();
    }

    public List<RolapMember[]> readTuples(
        DataSource jdbcConnection,
        List<List<RolapMember>> partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        prepareTuples(jdbcConnection, partialResult, newPartialResult);

        // List of tuples
        int n = targets.size();
        List<RolapMember[]> tupleList = new ArrayList<RolapMember[]>();
        Iterator<RolapMember>[] iter = new Iterator[n];
        for (int i = 0; i < n; i++) {
            Target t = targets.get(i);
            iter[i] = t.close().iterator();
        }
        while (iter[0].hasNext()) {
            RolapMember[] tuples = new RolapMember[n];
            for (int i = 0; i < n; i++) {
                tuples[i] = iter[i].next();
            }
            tupleList.add(tuples);
        }

        // need to hierarchize the columns from the enumerated targets
        // since we didn't necessarily add them in the order in which
        // they originally appeared in the cross product
        int enumTargetCount = getEnumTargetCount();
        if (enumTargetCount > 0) {
            FunUtil.hierarchizeTupleList(
                Util.<Member[]>cast(tupleList), false, n);
        }
        return tupleList;
    }

    /**
     * Sets the current member for those targets that retrieve their column
     * values from native sql
     *
     * @param partialRow if set, previously cached result set
     */
    private void resetCurrMembers(List<RolapMember> partialRow) {
        int nativeTarget = 0;
        for (Target target : targets) {
            if (target.srcMembers == null) {
                // if we have a previously cached row, use that by picking
                // out the column corresponding to this target; otherwise,
                // we need to retrieve a new column value from the current
                // result set
                if (partialRow != null) {
                    target.currMember = partialRow.get(nativeTarget++);
                } else {
                    target.currMember = null;
                }
            }
        }
    }

    /**
     * Recursively forms the cross product of a row retrieved through sql
     * with each of the targets that contains an enumerated set of members.
     *
     * @param currEnumTargetIdx current enum target that recursion
     * is being applied on
     * @param currTargetIdx index within the list of a targets that
     * currEnumTargetIdx corresponds to
     * @param nEnumTargets number of targets that have enumerated members
     * @param srcMemberIdxes for each enumerated target, the current member
     * to be retrieved to form the current cross product row
     * @param resultSet result set corresponding to rows retrieved through
     * native sql
     * @param message Message to issue on failure
     */
    private void addTargets(
        int currEnumTargetIdx,
        int currTargetIdx,
        int nEnumTargets,
        int[] srcMemberIdxes,
        ResultSet resultSet,
        String message)
    {

        // loop through the list of members for the current enum target
        Target currTarget = targets.get(currTargetIdx);
        for (int i = 0; i < currTarget.srcMembers.size(); i++) {
            srcMemberIdxes[currEnumTargetIdx] = i;
            // if we're not on the last enum target, recursively move
            // to the next one
            if (currEnumTargetIdx < nEnumTargets - 1) {
                int nextTargetIdx = currTargetIdx + 1;
                for (; nextTargetIdx < targets.size(); nextTargetIdx++) {
                    if (targets.get(nextTargetIdx).srcMembers != null) {
                        break;
                    }
                }
                addTargets(
                    currEnumTargetIdx + 1, nextTargetIdx, nEnumTargets,
                    srcMemberIdxes, resultSet, message);
            } else {
                // form a cross product using the columns from the current
                // result set row and the current members that recursion
                // has reached for the enum targets
                int column = 0;
                int enumTargetIdx = 0;
                for (Target target : targets) {
                    if (target.srcMembers == null) {
                        try {
                            column = target.addRow(resultSet, column);
                        } catch (Throwable e) {
                            throw Util.newError(e, message);
                        }
                    } else {
                        RolapMember member =
                            target.srcMembers.get(srcMemberIdxes[enumTargetIdx++]);
                        target.list.add(member);
                    }
                }
            }
        }
    }

    /**
     * Retrieves the current members fetched from the targets executed
     * through sql and form tuples, adding them to partialResult
     *
     * @param partialResult list containing the columns and rows corresponding
     * to data fetched through sql
     */
    private void savePartialResult(List<List<RolapMember>> partialResult) {
        List<RolapMember> row = new ArrayList<RolapMember>();
        for (Target target : targets) {
            if (target.srcMembers == null) {
                row.add(target.currMember);
            }
        }
        partialResult.add(row);
    }

    private String makeLevelMembersSql(DataSource dataSource) {
        // In the case of a virtual cube, if we need to join to the fact
        // table, we do not necessarily have a single underlying fact table,
        // as the underlying base cubes in the virtual cube may all reference
        // different fact tables.
        //
        // Therefore, we need to gather the underlying fact tables by going
        // through the list of measures referenced in the query.  And then
        // we generate one sub-select per fact table, joining against each
        // underlying fact table, unioning the sub-selects.
        RolapCube cube = null;
        boolean virtualCube = false;
        if (constraint instanceof SqlContextConstraint) {
            SqlContextConstraint sqlConstraint =
                (SqlContextConstraint) constraint;
            if (sqlConstraint.isJoinRequired()) {
                Query query = constraint.getEvaluator().getQuery();
                cube = (RolapCube) query.getCube();
                virtualCube = cube.isVirtual();
            }
        }

        if (virtualCube) {
            String selectString = "";
            Query query = constraint.getEvaluator().getQuery();

            // Make fact table appear in fixed sequence
            RolapCube.CubeComparator cubeComparator = new RolapCube.CubeComparator();
            TreeSet<RolapCube> baseCubes = new TreeSet<RolapCube>(cubeComparator);
            baseCubes.addAll(query.getBaseCubes());

            // generate sub-selects, each one joining with one of
            // the fact table referenced
            int k = -1;
            // Save the original measure in the context
            Member originalMeasure = constraint.getEvaluator().getMembers()[0];
            for (RolapCube baseCube : baseCubes) {
                // Use the measure from the corresponding base cube in the
                // context to find the correct join path to the base fact
                // table.
                //
                // Any measure is fine since the constraint logic only uses it
                // to find the correct fact table to join to.
                Member measureInCurrentbaseCube = baseCube.getMeasures().get(0);
                constraint.getEvaluator().setContext(measureInCurrentbaseCube);

                boolean finalSelect = (++k == baseCubes.size() - 1);
                WhichSelect whichSelect =
                    finalSelect ? WhichSelect.LAST : WhichSelect.NOT_LAST;
                selectString +=
                    generateSelectForLevels(dataSource, baseCube, whichSelect);
                if (!finalSelect) {
                    selectString += " union ";
                }
            }
            // Restore the original measure member
            constraint.getEvaluator().setContext(originalMeasure);
            return selectString;
        } else {
            return generateSelectForLevels(dataSource, cube, WhichSelect.ONLY);
        }
    }

    /**
     * Generates the SQL string corresponding to the levels referenced.
     *
     * @param dataSource jdbc connection that they query will execute against
     * @param baseCube this is the cube object for regular cubes, and the
     *   underlying base cube for virtual cubes
     * @param whichSelect Position of this select statement in a union
     * @return SQL statement string
     */
    private String generateSelectForLevels(
        DataSource dataSource,
        RolapCube baseCube,
        WhichSelect whichSelect)
    {
        String s = "while generating query to retrieve members of level(s) " + targets;
        SqlQuery sqlQuery = SqlQuery.newQuery(dataSource, s);

        // add the selects for all levels to fetch
        for (Target target : targets) {
            // if we're going to be enumerating the values for this target,
            // then we don't need to generate sql for it
            if (target.srcMembers == null) {
                addLevelMemberSql(
                    sqlQuery,
                    target.getLevel(),
                    baseCube,
                    whichSelect);
            }
        }

        constraint.addConstraint(sqlQuery, baseCube);

        return sqlQuery.toString();
    }

    /**
     * Generates the SQL statement to access members of <code>level</code>. For
     * example, <blockquote>
     * <pre>SELECT "country", "state_province", "city"
     * FROM "customer"
     * GROUP BY "country", "state_province", "city", "init", "bar"
     * ORDER BY "country", "state_province", "city"</pre>
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
     * @param sqlQuery the query object being constructed
     * @param level level to be added to the sql query
     * @param baseCube this is the cube object for regular cubes, and the
     *   underlying base cube for virtual cubes
     * @param whichSelect describes whether this select belongs to a larger
     * select containing unions or this is a non-union select
     */
    private void addLevelMemberSql(
        SqlQuery sqlQuery,
        RolapLevel level,
        RolapCube baseCube,
        WhichSelect whichSelect)
    {
        RolapHierarchy hierarchy = level.getHierarchy();

        // lookup RolapHierarchy of base cube that matches this hierarchy

        if (hierarchy instanceof RolapCubeHierarchy) {
            RolapCubeHierarchy cubeHierarchy = (RolapCubeHierarchy)hierarchy;
            if (baseCube != null && !cubeHierarchy.getDimension().getCube().equals(baseCube)) {
                // replace the hierarchy with the underlying base cube hierarchy
                // in the case of virtual cubes
                hierarchy = baseCube.findBaseCubeHierarchy(hierarchy);
            }
        }

        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        int levelDepth = level.getDepth();
        for (int i = 0; i <= levelDepth; i++) {
            RolapLevel currLevel = levels[i];
            if (currLevel.isAll()) {
                continue;
            }

            MondrianDef.Expression keyExp = currLevel.getKeyExp();
            MondrianDef.Expression ordinalExp = currLevel.getOrdinalExp();
            MondrianDef.Expression captionExp = currLevel.getCaptionExp();

            String keySql = keyExp.getExpression(sqlQuery);
            String ordinalSql = ordinalExp.getExpression(sqlQuery);

            hierarchy.addToFrom(sqlQuery, keyExp);
            hierarchy.addToFrom(sqlQuery, ordinalExp);

            String captionSql = null;
            if (captionExp != null) {
                captionSql = captionExp.getExpression(sqlQuery);
                hierarchy.addToFrom(sqlQuery, captionExp);
            }

            sqlQuery.addSelectGroupBy(keySql);

            if (!ordinalSql.equals(keySql)) {
                sqlQuery.addSelectGroupBy(ordinalSql);
            }

            if (captionSql != null) {
                sqlQuery.addSelectGroupBy(captionSql);
            }

            constraint.addLevelConstraint(sqlQuery, baseCube, null, currLevel);

            // If this is a select on a virtual cube, the query will be
            // a union, so the order by columns need to be numbers,
            // not column name strings or expressions.
            switch (whichSelect) {
            case LAST:
                sqlQuery.addOrderBy(
                    Integer.toString(
                        sqlQuery.getCurrentSelectListSize()),
                        true, false, true);
                break;
            case ONLY:
                sqlQuery.addOrderBy(ordinalSql, true, false, true);
                break;
            }

            RolapProperty[] properties = currLevel.getProperties();
            for (RolapProperty property : properties) {
                String propSql = property.getExp().getExpression(sqlQuery);
                sqlQuery.addSelectGroupBy(propSql);
            }
        }
    }

    int getMaxRows() {
        return maxRows;
    }

    void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    /**
     * Description of the position of a SELECT statement in a UNION. Queries
     * on virtual cubes tend to generate unions.
     */
    enum WhichSelect {
        /**
         * Select statement does not belong to a union.
         */
        ONLY,
        /**
         * Select statement belongs to a UNION, but is not the last. Typically
         * this occurs when querying a virtual cube.
         */
        NOT_LAST,
        /**
         * Select statement is the last in a UNION. Typically
         * this occurs when querying a virtual cube.
         */
        LAST
    }
}

// End SqlTupleReader.java
