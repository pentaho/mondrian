/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2007 Julian Hyde and others
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
import java.sql.Connection;
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
 * {@link TupleConstraint#addConstraint(mondrian.rolap.sql.SqlQuery, java.util.Map)}
 * is required to join the fact table for the levels table.</li>
 * </ul>
 *
 * @author av
 * @since Nov 11, 2005
 * @version $Id$
 */
public class SqlTupleReader implements TupleReader {
    TupleConstraint constraint;
    List<Target> targets = new ArrayList<Target>();
    int maxRows = 0;

    /**
     * TODO: Document this class.
     */
    private class Target {
        final RolapLevel level;
        final MemberCache cache;

        RolapLevel[] levels;
        List<RolapMember> list;
        int levelDepth;
        boolean parentChild;
        RolapMember[] members;
        List<RolapMember>[] siblings;
        final MemberBuilder memberBuilder;
        // if set, the rows for this target come from the array rather
        // than native sql
        private final RolapMember[] srcMembers;
        // current member within the current result set row
        // for this target
        private RolapMember currMember;

        public Target(
            RolapLevel level, MemberBuilder memberBuilder,
            RolapMember[] srcMembers) {
            this.level = level;
            this.cache = memberBuilder.getMemberCache();
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
            members = new RolapMember[levels.length];
            siblings = new List[levels.length + 1];
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
            synchronized (cache) {
                return internalAddRow(resultSet, column);
            }
        }

        private int internalAddRow(ResultSet resultSet, int column) throws SQLException {
            RolapMember member = null;
            if (currMember != null) {
                member = currMember;
            } else {
                boolean checkCacheStatus=true;
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
                    column += childLevel.getProperties().length;
                    if (member != members[i]) {
                        // Flush list we've been building.
                        List<RolapMember> children = siblings[i + 1];
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
                            siblings[i + 1] = new ArrayList<RolapMember>();
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
                currMember = member;
            }
            list.add(member);
            return column;
        }

        public List<RolapMember> close() {
            synchronized (cache) {
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
            for (int i = 0; i < members.length; i++) {
                RolapMember member = members[i];
                final List<RolapMember> children = siblings[i + 1];
                if (member != null && children != null) {
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
        RolapMember[] srcMembers)
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

    public List<RolapMember[]> readTuples(
        DataSource dataSource,
        List<List<RolapMember>> partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        Connection con;
        try {
            con = dataSource.getConnection();
        } catch (SQLException e) {
            throw Util.newInternal(e, "could not connect to DB");
        }
        try {
            return readTuples(con, partialResult, newPartialResult);
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                throw Util.newInternal(e, "could not close connection");
            }
        }
    }

    private void prepareTuples(
        Connection jdbcConnection,
        List<List<RolapMember>> partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        String sql = makeLevelMembersSql(jdbcConnection);
        ResultSet resultSet = null;
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
                resultSet = RolapUtil.executeQuery(
                    jdbcConnection, sql, maxRows,
                    "SqlTupleReader.readTuples " + partialTargets, -1, -1);
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
                    for ( ; firstEnumTarget < targets.size();
                        firstEnumTarget++)
                    {
                        if (targets.get(firstEnumTarget).
                            srcMembers != null) {
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
                        resultSet, sql);
                    if (newPartialResult != null) {
                        savePartialResult(newPartialResult);
                    }
                }

                if (execQuery) {
                    moreRows = resultSet.next();
                } else {
                    currPartialResultIdx++;
                    moreRows = currPartialResultIdx < partialResult.size();
                }
            }
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

    public List<RolapMember> readMembers(
        Connection jdbcConnection,
        List<List<RolapMember>> partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        prepareTuples(jdbcConnection, partialResult, newPartialResult);
        assert targets.size() == 1;
        return targets.get(0).close();
    }

    public List<RolapMember[]> readTuples(
        Connection jdbcConnection,
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
            FunUtil.hierarchize(tupleList, false);
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
     * @param sql sql statement corresponding to the select used to natively
     * retrieve rows
     */
    private void addTargets(
        int currEnumTargetIdx, int currTargetIdx, int nEnumTargets,
        int[] srcMemberIdxes, ResultSet resultSet, String sql) {

        // loop through the list of members for the current enum target
        Target currTarget = targets.get(currTargetIdx);
        for (int i = 0; i < currTarget.srcMembers.length; i++) {
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
                    srcMemberIdxes, resultSet, sql);
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
                            throw Util.newInternal(
                                e,
                                "while populating member cache with " +
                                    "members for " + targets + "'; sql=[" +
                                    sql +
                                    "]");
                        }
                    } else {
                        RolapMember member =
                            target.srcMembers[srcMemberIdxes[enumTargetIdx++]];
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

    String makeLevelMembersSql(Connection jdbcConnection) {

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
            Set<Map<RolapLevel, RolapStar.Column>> baseCubesLevelToColumnMaps =
                query.getVirtualCubeBaseCubeMaps();
            Map<Map<RolapLevel, RolapStar.Column>, RolapMember> measureMap =
                query.getLevelMapToMeasureMap();

            // generate sub-selects, each one joining with one of
            // underlying fact tables
            int k = -1;
            for (Map<RolapLevel, RolapStar.Column> map :
                baseCubesLevelToColumnMaps)
            {
                boolean finalSelect =
                    (++k == baseCubesLevelToColumnMaps.size() - 1);
                // set the evaluator context so it references a measure
                // associated with the star that we're currently dealing
                // with so the sql generated will reference the appropriate
                // fact table
                RolapMember measure = measureMap.get(map);
                assert measure instanceof RolapStoredMeasure;
                Evaluator evaluator = constraint.getEvaluator();
                evaluator.push();
                evaluator.setContext(measure);
                WhichSelect whichSelect =
                    finalSelect ? WhichSelect.LAST :
                        WhichSelect.NOT_LAST;
                selectString +=
                    generateSelectForLevels(
                        jdbcConnection, map, whichSelect);
                evaluator = evaluator.pop();
                if (!finalSelect) {
                    selectString += " union ";
                }
            }
            return selectString;
        } else {
            Map<RolapLevel, RolapStar.Column> map =
                cube == null ?
                    null :
                    cube.getStar().getLevelToColumnMap(cube);
            return generateSelectForLevels(jdbcConnection, map, WhichSelect.ONLY);
        }
    }

    /**
     * Generates the SQL string corresponding to the levels referenced.
     *
     * @param jdbcConnection jdbc connection that they query will execute
     * against
     * @param levelToColumnMap set only in the case of virtual cubes;
     * provides the appropriate mapping for the base cube being processed
     * @param whichSelect Position of this select statement in a union
     * @return SQL statement string
     */
    private String generateSelectForLevels(
        Connection jdbcConnection,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        WhichSelect whichSelect) {

        String s = "while generating query to retrieve members of level(s) " + targets;
        SqlQuery sqlQuery = newQuery(jdbcConnection, s);

        // add the selects for all levels to fetch
        List<String> ordinalColumns = new ArrayList<String>();
        List<Integer> orderByColnos = new ArrayList<Integer>();
        for (Target target : targets) {
            // if we're going to be enumerating the values for this target,
            // then we don't need to generate sql for it
            if (target.srcMembers == null) {
                addLevelMemberSql(
                    sqlQuery,
                    target.getLevel(),
                    levelToColumnMap,
                    whichSelect,
                    orderByColnos,
                    ordinalColumns);
            }
        }

        // Additional work required for virtual cubes.
        //
        // Add ordinal columns we need to order on to the projection list.
        // We add them to the end of the projection list to avoid shifting
        // the position of the non-ordinal columns.  Subsequent selects
        // depend on this.
        //
        // If this is the final select, add on the orderby column numbers.
        // Adjust any placeholders reflecting ordinal columns with their
        // actual position in the projection list, now that we know how big
        // the total projection list is.
        if (whichSelect != WhichSelect.ONLY) {
            int numNonOrdinalCols = sqlQuery.getCurrentSelectListSize();
            for (String col : ordinalColumns) {
                sqlQuery.addSelect(col);
            }
            if (whichSelect == WhichSelect.LAST) {
                for (int oByColno : orderByColnos) {
                    String ostr;
                    if (oByColno < 0) {
                        ostr = Integer.toString(++numNonOrdinalCols);
                    } else {
                        ostr = Integer.toString(oByColno);
                    }
                    sqlQuery.addOrderBy(ostr, true, false, true);
                }
            }
        }

        // additional constraints
        constraint.addConstraint(sqlQuery, levelToColumnMap);

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
     * @param levelToColumnMap set only in the case of virtual cubes;
     * provides the appropriate mapping for the base cube being processed
     * @param whichSelect describes whether this select belongs to a larger
     * select containing unions or this is a non-union select
     * @param orderByColnos list of order by column numbers; used for virtual
     * cubes
     * @param ordinalColumns list of ordinal columns referenced in the query;
     * used for virtual cubes
     */
    private void addLevelMemberSql(
        SqlQuery sqlQuery,
        RolapLevel level,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        WhichSelect whichSelect,
        List<Integer> orderByColnos,
        List<String> ordinalColumns)
    {
        RolapHierarchy hierarchy = level.getHierarchy();

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

            constraint.addLevelConstraint(
                sqlQuery, null, level2, levelToColumnMap);

            if (level2.hasCaptionColumn()) {
                MondrianDef.Expression captionExp = level2.getCaptionExp();
                hierarchy.addToFrom(sqlQuery, captionExp);
                String captionSql = captionExp.getExpression(sqlQuery);
                sqlQuery.addSelect(captionSql);
                sqlQuery.addGroupBy(captionSql);
            }

            String ordinalSql = level2.getOrdinalExp().getExpression(sqlQuery);
            sqlQuery.addGroupBy(ordinalSql);
            if (whichSelect != WhichSelect.ONLY) {
                // Keep track of ordinal columns so we can add them to the
                // end of the projection list later
                if (!ordinalSql.equals(keySql)) {
                    ordinalColumns.add(ordinalSql);
                }
            }

            // If this is a select on a virtual cube, the query will be
            // a union, so the order by columns need to be numbers,
            // not column name strings.  Don't add these to the order by.
            // Just keep track of the numbers, for now.  Also, if the
            // level contains an ordinal column, the ordinal column will
            // appear at the end of the projection list.  Since we don't
            // yet know how big the total projection list will be,
            // use -1 as a placeholder for those columns.
            switch (whichSelect) {
            case LAST:
                if (ordinalSql.equals(keySql)) {
                    orderByColnos.add(
                        sqlQuery.getCurrentSelectListSize());
                } else {
                    orderByColnos.add(-1);
                }
                break;
            case ONLY:
                sqlQuery.addOrderBy(ordinalSql, true, false, true);
                break;
            }
            RolapProperty[] properties = level2.getProperties();
            for (RolapProperty property : properties) {
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
