/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.TupleList;
import mondrian.calc.impl.ListTupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;
import mondrian.server.Locus;
import mondrian.server.monitor.SqlStatementEvent;
import mondrian.util.Pair;

import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import javax.sql.DataSource;

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
 * {@link TupleConstraint#addLevelConstraint(mondrian.rolap.sql.SqlQuery, RolapCube, mondrian.rolap.aggmatcher.AggStar, RolapLevel)}
 * although it may do so to restrict
 * the result. Also it is permitted to cache the parent/children from all
 * members in MemberCache, so
 * {@link TupleConstraint#getMemberChildrenConstraint(RolapMember)}
 * should not return null.</li>
 *
 * <li>When reading multiple levels (i.e. we are performing a crossjoin),
 * then we can not store the parent/child pairs in the MemberCache and
 * {@link TupleConstraint#getMemberChildrenConstraint(RolapMember)}
 * must return null. Also
 * {@link TupleConstraint#addConstraint(mondrian.rolap.sql.SqlQuery, mondrian.rolap.RolapCube, mondrian.rolap.aggmatcher.AggStar)}
 * is required to join the fact table for the levels table.</li>
 * </ul>
 *
 * @author av
 * @since Nov 11, 2005
 */
public class SqlTupleReader implements TupleReader {
    private static final Logger LOGGER =
        Logger.getLogger(SqlTupleReader.class);
    protected final TupleConstraint constraint;
    List<TargetBase> targets = new ArrayList<TargetBase>();
    int maxRows = 0;

    /**
     * How many members could not be instantiated in this iteration. This
     * phenomenon occurs in a parent-child hierarchy, where a member cannot be
     * created before its parent. Populating the hierarchy will take multiple
     * passes and will terminate in success when missedMemberCount == 0 at the
     * end of a pass, or failure if a pass generates failures but does not
     * manage to load any more members.
     */
    private int missedMemberCount;
    private static final String UNION = "union";

    /**
     * Helper class for SqlTupleReader;
     * keeps track of target levels and constraints for adding to sql query.
     */
    private class Target extends TargetBase {
        final MemberCache cache;

        RolapLevel[] levels;
        int levelDepth;
        boolean parentChild;
        List<RolapMember> members;
        final HashMap<Object, RolapMember> keyToMember =
            new HashMap<Object, RolapMember>();
        List<List<RolapMember>> siblings;
        // if set, the rows for this target come from the array rather
        // than native sql
        // current member within the current result set row
        // for this target

        public Target(
            RolapLevel level,
            MemberBuilder memberBuilder,
            List<RolapMember> srcMembers)
        {
            super(srcMembers, level, memberBuilder);
            this.cache = memberBuilder.getMemberCache();
        }

        public void open() {
            levels = (RolapLevel[]) level.getHierarchy().getLevels();
            setList(new ArrayList<RolapMember>());
            levelDepth = level.getDepth();
            parentChild = level.isParentChild();
            // members[i] is the current member of level#i, and siblings[i]
            // is the current member of level#i plus its siblings
            members =
                new ArrayList<RolapMember>(
                    Collections.<RolapMember>nCopies(levels.length, null));
            siblings = new ArrayList<List<RolapMember>>();
            for (int i = 0; i < levels.length + 1; i++) {
                siblings.add(new ArrayList<RolapMember>());
            }
        }

        int internalAddRow(SqlStatement stmt, int column)
            throws SQLException
        {
            RolapMember member = null;
            if (getCurrMember() != null) {
                setCurrMember(member);
            } else {
                boolean checkCacheStatus = true;
                for (int i = 0; i <= levelDepth; i++) {
                    RolapLevel childLevel = levels[i];
                    if (childLevel.isAll()) {
                        member = memberBuilder.allMember();
                        continue;
                    }
                    RolapMember parentMember = member;
                    final List<SqlStatement.Accessor> accessors =
                        stmt.getAccessors();
                    if (parentChild) {
                        Object parentValue =
                            accessors.get(column++).get();
                        if (parentValue == null
                            || parentValue.toString().equals(
                                childLevel.getNullParentValue()))
                        {
                            // member is at top of hierarchy; its parent is the
                            // 'all' member. Convert null to placeholder value
                            // for uniformity in hashmaps.
                            parentValue = RolapUtil.sqlNullValue;
                        } else {
                            Object parentKey =
                                cache.makeKey(
                                    member,
                                    parentValue);
                            parentMember = cache.getMember(parentKey);
                            if (parentMember == null) {
                                // Maybe it wasn't caching.
                                // We have an intermediate volatile map.
                                parentMember = keyToMember.get(parentValue);
                            }
                            if (parentMember == null) {
                                LOGGER.warn(
                                    MondrianResource.instance()
                                        .LevelTableParentNotFound.str(
                                            childLevel.getUniqueName(),
                                            String.valueOf(parentValue)));
                            }
                        }
                    }
                    Object value = accessors.get(column++).get();
                    if (value == null) {
                        value = RolapUtil.sqlNullValue;
                    }
                    Object captionValue;
                    if (childLevel.hasCaptionColumn()) {
                        captionValue = accessors.get(column++).get();
                    } else {
                        captionValue = null;
                    }
                    Object key;
                    if (parentChild) {
                        key = cache.makeKey(member, value);
                    } else {
                        key = cache.makeKey(parentMember, value);
                    }
                    member = cache.getMember(key, checkCacheStatus);
                    checkCacheStatus = false; // only check the first time
                    if (member == null) {
                        if (constraint instanceof
                            RolapNativeCrossJoin.NonEmptyCrossJoinConstraint
                            && childLevel.isParentChild())
                        {
                            member =
                                castToNonEmptyCJConstraint(constraint)
                                    .findMember(value);
                        }
                        if (member == null) {
                            member = memberBuilder.makeMember(
                                parentMember, childLevel, value, captionValue,
                                parentChild, stmt, key, column);
                        }
                    }

                    // Skip over the columns consumed by makeMember
                    if (!childLevel.getOrdinalExp().equals(
                            childLevel.getKeyExp()))
                    {
                        ++column;
                    }
                    column += childLevel.getProperties().length;

                    // Cache in our intermediate map the key/member pair
                    // for later lookups of children.
                    keyToMember.put(member.getKey(), member);

                    if (member != members.get(i)) {
                        // Flush list we've been building.
                        List<RolapMember> children = siblings.get(i + 1);
                        if (children != null) {
                            MemberChildrenConstraint mcc =
                                constraint.getMemberChildrenConstraint(
                                    members.get(i));
                            if (mcc != null) {
                                cache.putChildren(
                                    members.get(i), mcc, children);
                            }
                        }
                        // Start a new list, if the cache needs one. (We don't
                        // synchronize, so it's possible that the cache will
                        // have one by the time we complete it.)
                        MemberChildrenConstraint mcc =
                            constraint.getMemberChildrenConstraint(member);
                        // we keep a reference to cachedChildren so they don't
                        // get garbage-collected
                        List<RolapMember> cachedChildren =
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
                                siblings.get(i).add(member);
                            }
                        }
                    }
                }
                setCurrMember(member);
            }
            getList().add(member);
            return column;
        }

        public List<Member> close() {
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
        public List<Member> internalClose() {
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
            return Util.cast(getList());
        }

        /**
         * Adds <code>member</code> just before the first element in
         * <code>list</code> which has the same parent.
         */
        private void addAsOldestSibling(
            List<RolapMember> list,
            RolapMember member)
        {
            int i = list.size();
            while (--i >= 0) {
                RolapMember sibling = list.get(i);
                if (sibling.getParentMember() != member.getParentMember()) {
                    break;
                }
            }
            list.add(i + 1, member);
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
        for (TargetBase target : targets) {
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
        for (TargetBase target : targets) {
            if (target.getSrcMembers() != null) {
                enumTargetCount++;
            }
        }
        return enumTargetCount;
    }

    protected void prepareTuples(
        DataSource dataSource,
        TupleList partialResult,
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
                List<TargetBase> partialTargets = new ArrayList<TargetBase>();
                for (TargetBase target : targets) {
                    if (target.srcMembers == null) {
                        partialTargets.add(target);
                    }
                }
                final Pair<String, List<SqlStatement.Type>> pair =
                    makeLevelMembersSql(dataSource);
                String sql = pair.left;
                List<SqlStatement.Type> types = pair.right;
                assert sql != null && !sql.equals("");
                stmt = RolapUtil.executeQuery(
                    dataSource, sql, types, maxRows, 0,
                    new SqlStatement.StatementLocus(
                        Locus.peek().execution,
                        "SqlTupleReader.readTuples " + partialTargets,
                        message,
                        SqlStatementEvent.Purpose.TUPLES, 0),
                    -1, -1);
                resultSet = stmt.getResultSet();
            } else {
                resultSet = null;
            }

            for (TargetBase target : targets) {
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
                    for (TargetBase target : targets) {
                        target.setCurrMember(null);
                        column = target.addRow(stmt, column);
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
                        partialRow =
                            Util.cast(partialResult.get(currPartialResultIdx));
                    }
                    resetCurrMembers(partialRow);
                    addTargets(
                        0, firstEnumTarget, enumTargetCount, srcMemberIdxes,
                        stmt, message);
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
                throw stmt.handle(e);
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public TupleList readMembers(
        DataSource dataSource,
        TupleList partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        int memberCount = countMembers();
        while (true) {
            missedMemberCount = 0;
            int memberCountBefore = memberCount;
            prepareTuples(dataSource, partialResult, newPartialResult);
            memberCount = countMembers();
            if (missedMemberCount == 0) {
                // We have successfully read all members. This is always the
                // case in a regular hierarchy. In a parent-child hierarchy
                // it may take several passes, because we cannot create a member
                // before we create its parent.
                break;
            }
            if (memberCount == memberCountBefore) {
                // This pass made no progress. This must be because of a cycle.
                throw Util.newError(
                    "Parent-child hierarchy contains cyclic data");
            }
        }

        assert targets.size() == 1;

        return new UnaryTupleList(
            bumpNullMember(
                targets.get(0).close()));
    }

    protected List<Member> bumpNullMember(List<Member> members) {
        if (members.size() > 0
            && ((RolapMemberBase)members.get(members.size() - 1)).getKey()
                == RolapUtil.sqlNullValue)
        {
            Member removed = members.remove(members.size() - 1);
            members.add(0, removed);
        }
        return members;
    }

    /**
     * Returns the number of members that have been read from all targets.
     *
     * @return Number of members that have been read from all targets
     */
    private int countMembers() {
        int n = 0;
        for (TargetBase target : targets) {
            if (target.getList() != null) {
                n += target.getList().size();
            }
        }
        return n;
    }

    public TupleList readTuples(
        DataSource jdbcConnection,
        TupleList partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        prepareTuples(jdbcConnection, partialResult, newPartialResult);

        // List of tuples
        final int n = targets.size();
        @SuppressWarnings({"unchecked"})
        final Iterator<Member>[] iter = new Iterator[n];
        for (int i = 0; i < n; i++) {
            TargetBase t = targets.get(i);
            iter[i] = t.close().iterator();
        }
        List<Member> members = new ArrayList<Member>();
        while (iter[0].hasNext()) {
            for (int i = 0; i < n; i++) {
                members.add(iter[i].next());
            }
        }

        TupleList tupleList =
            n == 1
                ? new UnaryTupleList(members)
                : new ListTupleList(n, members);

        // need to hierarchize the columns from the enumerated targets
        // since we didn't necessarily add them in the order in which
        // they originally appeared in the cross product
        int enumTargetCount = getEnumTargetCount();
        if (enumTargetCount > 0) {
            tupleList = FunUtil.hierarchizeTupleList(tupleList, false);
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
        for (TargetBase target : targets) {
            if (target.srcMembers == null) {
                // if we have a previously cached row, use that by picking
                // out the column corresponding to this target; otherwise,
                // we need to retrieve a new column value from the current
                // result set
                if (partialRow != null) {
                    target.setCurrMember(partialRow.get(nativeTarget++));
                } else {
                    target.setCurrMember(null);
                }
            }
        }
    }

    /**
     * Recursively forms the cross product of a row retrieved through sql
     * with each of the targets that contains an enumerated set of members.
     *
     * @param currEnumTargetIdx current enum target that recursion
     *     is being applied on
     * @param currTargetIdx index within the list of a targets that
     *     currEnumTargetIdx corresponds to
     * @param nEnumTargets number of targets that have enumerated members
     * @param srcMemberIdxes for each enumerated target, the current member
     *     to be retrieved to form the current cross product row
     * @param stmt Statement containing the result set corresponding to rows
     *     retrieved through native SQL
     * @param message Message to issue on failure
     */
    private void addTargets(
        int currEnumTargetIdx,
        int currTargetIdx,
        int nEnumTargets,
        int[] srcMemberIdxes,
        SqlStatement stmt,
        String message)
    {
        // loop through the list of members for the current enum target
        TargetBase currTarget = targets.get(currTargetIdx);
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
                    srcMemberIdxes, stmt, message);
            } else {
                // form a cross product using the columns from the current
                // result set row and the current members that recursion
                // has reached for the enum targets
                int column = 0;
                int enumTargetIdx = 0;
                for (TargetBase target : targets) {
                    if (target.srcMembers == null) {
                        try {
                            column = target.addRow(stmt, column);
                        } catch (Throwable e) {
                            throw Util.newError(e, message);
                        }
                    } else {
                        RolapMember member =
                            target.srcMembers.get(
                                srcMemberIdxes[enumTargetIdx++]);
                        target.getList().add(member);
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
        for (TargetBase target : targets) {
            if (target.srcMembers == null) {
                row.add(target.getCurrMember());
            }
        }
        partialResult.add(row);
    }

    Pair<String, List<SqlStatement.Type>> makeLevelMembersSql(
        DataSource dataSource)
    {
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
            Query query = constraint.getEvaluator().getQuery();
            cube = (RolapCube) query.getCube();
            if (sqlConstraint.isJoinRequired()) {
                virtualCube = cube.isVirtual();
            }
        }

        if (virtualCube) {
            Query query = constraint.getEvaluator().getQuery();

            // Make fact table appear in fixed sequence

            final Collection<RolapCube> baseCubes =
                getBaseCubeCollection(query);
            Collection<RolapCube> fullyJoiningBaseCubes =
                getFullyJoiningBaseCubes(baseCubes);
            if (fullyJoiningBaseCubes.size() == 0) {
                return sqlForEmptyTuple(dataSource, baseCubes);
            }
            // generate sub-selects, each one joining with one of
            // the fact table referenced
            String prependString = "";
            final StringBuilder selectString = new StringBuilder();
            List<SqlStatement.Type> types = null;

            final int savepoint =
                getEvaluator(constraint).savepoint();

            SqlQuery unionQuery = SqlQuery.newQuery(dataSource, "");

            try {
                for (RolapCube baseCube : fullyJoiningBaseCubes) {
                    // Use the measure from the corresponding base cube in the
                    // context to find the correct join path to the base fact
                    // table.
                    //
                    // The first non-calculated measure is fine since the
                    // constraint logic only uses it
                    // to find the correct fact table to join to.
                    Member measureInCurrentbaseCube = null;
                    for (Member currMember : baseCube.getMeasures()) {
                        if (!currMember.isCalculated()) {
                            measureInCurrentbaseCube = currMember;
                            break;
                        }
                    }

                    if (measureInCurrentbaseCube == null) {
                        // Couldn't find a non-calculated member in this cube.
                        // Pick any measure and the code will fallback to
                        // the fact table.
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(
                                "No non-calculated member found in cube "
                                + baseCube.getName());
                        }
                        measureInCurrentbaseCube =
                            baseCube.getMeasures().get(0);
                    }

                    // Force the constraint evaluator's measure
                    // to the one in the base cube.
                    getEvaluator(constraint)
                        .setContext(measureInCurrentbaseCube);

                    selectString.append(prependString);

                    // Generate the select statement for the current base cube.
                    // Make sure to pass WhichSelect.NOT_LAST if there are more
                    // than one base cube and it isn't the last one so that
                    // the order by clause is not added to unionized queries
                    // (that would be illegal SQL)
                    final Pair<String, List<SqlStatement.Type>> pair =
                        generateSelectForLevels(
                            dataSource, baseCube,
                            fullyJoiningBaseCubes.size() == 1
                                ? WhichSelect.ONLY
                                : WhichSelect.NOT_LAST);
                    selectString.append(pair.left);
                    types = pair.right;
                    prependString =
                        MondrianProperties.instance().GenerateFormattedSql.get()
                            ? Util.nl + UNION + Util.nl
                            : " " + UNION + " ";
                }
            } finally {
                // Restore the original measure member
                getEvaluator(constraint).restore(savepoint);
            }

            if (fullyJoiningBaseCubes.size() == 1) {
                // Because there is only one virtual cube to
                // join on, we can swap the union query by
                // the original one.
                return Pair.of(selectString.toString(), types);
            } else {
                // Add the subquery to the wrapper query.
                unionQuery.addFromQuery(
                    selectString.toString(), "unionQuery", true);

                // Dont forget to select all columns.
                unionQuery.addSelect("*", null, null);

                // Sort the union of the cubes.
                // The order by columns need to be numbers,
                // not column name strings or expressions.
                if (fullyJoiningBaseCubes.size() > 1) {
                    for (int i = 0; i < types.size(); i++) {
                        unionQuery.addOrderBy(
                            i + 1 + "",
                            true,
                            false,
                            // We can't order the nulls
                            // because column ordinals used as alias
                            // are not supported by functions.
                            // FIXME This dialect call is old and
                            // has lost its meaning in the process.
                            unionQuery.getDialect()
                                .requiresUnionOrderByOrdinal());
                    }
                }
                return Pair.of(unionQuery.toSqlAndTypes().left, types);
            }

        } else {
            // This is the standard code path with regular single-fact table
            // cubes.
            return generateSelectForLevels(
                dataSource, cube, WhichSelect.ONLY);
        }
    }

    private Collection<RolapCube> getFullyJoiningBaseCubes(
        Collection<RolapCube> baseCubes)
    {
        final Collection<RolapCube> fullyJoiningCubes =
            new ArrayList<RolapCube>();
        for (RolapCube baseCube : baseCubes) {
            boolean allTargetsJoin = true;
            for (TargetBase target : targets) {
                if (!targetIsOnBaseCube(target, baseCube)) {
                    allTargetsJoin = false;
                }
            }
            if (allTargetsJoin) {
                fullyJoiningCubes.add(baseCube);
            }
        }
        return fullyJoiningCubes;
    }


    Collection<RolapCube> getBaseCubeCollection(final Query query) {
        RolapCube.CubeComparator cubeComparator =
            new RolapCube.CubeComparator();
        Collection<RolapCube> baseCubes =
            new TreeSet<RolapCube>(cubeComparator);
        baseCubes.addAll(query.getBaseCubes());
        return baseCubes;
    }

    Pair<String, List<SqlStatement.Type>> sqlForEmptyTuple(
        DataSource dataSource,
        final Collection<RolapCube> baseCubes)
    {
        final SqlQuery sqlQuery = SqlQuery.newQuery(dataSource, null);
        sqlQuery.addSelect("0", null);
        sqlQuery.addFrom(baseCubes.iterator().next().getFact(), null, true);
        sqlQuery.addWhere("1 = 0");
        return sqlQuery.toSqlAndTypes();
    }

    /**
     * Generates the SQL string corresponding to the levels referenced.
     *
     * @param dataSource jdbc connection that they query will execute against
     * @param baseCube this is the cube object for regular cubes, and the
     *   underlying base cube for virtual cubes
     * @param whichSelect Position of this select statement in a union
     * @return SQL statement string and types
     */
    Pair<String, List<SqlStatement.Type>> generateSelectForLevels(
        DataSource dataSource,
        RolapCube baseCube,
        WhichSelect whichSelect)
    {
        String s =
            "while generating query to retrieve members of level(s) " + targets;

        // Allow query to use optimization hints from the table definition
        SqlQuery sqlQuery = SqlQuery.newQuery(dataSource, s);
        sqlQuery.setAllowHints(true);


        Evaluator evaluator = getEvaluator(constraint);
        AggStar aggStar = chooseAggStar(constraint, evaluator, baseCube);

        // add the selects for all levels to fetch
        for (TargetBase target : targets) {
            // if we're going to be enumerating the values for this target,
            // then we don't need to generate sql for it
            if (target.getSrcMembers() == null) {
                addLevelMemberSql(
                    sqlQuery,
                    target.getLevel(),
                    baseCube,
                    whichSelect,
                    aggStar);
            }
        }

        constraint.addConstraint(sqlQuery, baseCube, aggStar);

        return sqlQuery.toSqlAndTypes();
    }

    boolean targetIsOnBaseCube(TargetBase target, RolapCube baseCube) {
        return baseCube == null || baseCube.findBaseCubeHierarchy(
            target.getLevel().getHierarchy()) != null;
    }

    /**
     * <p>Determines whether the GROUP BY clause is required, based on the
     * schema definitions of the hierarchy and level properties.</p>
     *
     * <p>The GROUP BY clause may only be eliminated if the level identified by
     * the uniqueKeyLevelName exists, the query is at a depth to include it,
     * and all properties in the included levels are functionally dependent on
     * their level values.</p>
     *
     *
     * @param sqlQuery     The query object being constructed
     * @param hierarchy    Hierarchy of the cube
     * @param levels       Levels in this hierarchy
     * @param levelDepth   Level depth at which the query is occuring
     * @return whether the GROUP BY is needed
     *
     */
    private boolean isGroupByNeeded(
        SqlQuery sqlQuery,
        RolapHierarchy hierarchy,
        RolapLevel[] levels,
        int levelDepth)
    {
        // Figure out if we need to generate GROUP BY at all.  It may only be
        // eliminated if we are at a depth that includes the unique key level,
        // and all properties of included levels depend on the level value.
        boolean needsGroupBy = false;  // figure out if we need GROUP BY at all

        if (hierarchy.getUniqueKeyLevelName() == null) {
            needsGroupBy = true;
        } else {
            boolean foundUniqueKeyLevelName = false;
            for (int i = 0; i <= levelDepth; i++) {
                RolapLevel lvl = levels[i];

                // can ignore the "all" level
                if (!(lvl.isAll())) {
                    if (hierarchy.getUniqueKeyLevelName().equals(
                            lvl.getName()))
                    {
                       foundUniqueKeyLevelName = true;
                    }
                    for (RolapProperty p : lvl.getProperties()) {
                        if (!p.dependsOnLevelValue()) {
                            needsGroupBy = true;
                            // GROUP BY is required, so break out of
                            // properties loop
                            break;
                        }
                    }
                    if (needsGroupBy) {
                        // GROUP BY is required, so break out of levels loop
                        break;
                    }
                }
            }
            if (!foundUniqueKeyLevelName) {
                // if we're not deep enough to be unique,
                // then the GROUP BY is required
                needsGroupBy = true;
            }
        }

        return needsGroupBy;
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
     * @param aggStar aggregate star if available
     */
    protected void addLevelMemberSql(
        SqlQuery sqlQuery,
        RolapLevel level,
        RolapCube baseCube,
        WhichSelect whichSelect,
        AggStar aggStar)
    {
        RolapHierarchy hierarchy = level.getHierarchy();

        // lookup RolapHierarchy of base cube that matches this hierarchy

        if (hierarchy instanceof RolapCubeHierarchy) {
            RolapCubeHierarchy cubeHierarchy = (RolapCubeHierarchy)hierarchy;
            if (baseCube != null
                && !cubeHierarchy.getCube().equals(baseCube))
            {
                // replace the hierarchy with the underlying base cube hierarchy
                // in the case of virtual cubes
                hierarchy = baseCube.findBaseCubeHierarchy(hierarchy);
            }
        }

        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        int levelDepth = level.getDepth();

        boolean needsGroupBy =
            isGroupByNeeded(sqlQuery, hierarchy, levels, levelDepth);

        for (int i = 0; i <= levelDepth; i++) {
            RolapLevel currLevel = levels[i];
            if (currLevel.isAll()) {
                continue;
            }

            // Determine if the aggregate table contains the collapsed level
            boolean levelCollapsed =
                (aggStar != null)
                && SqlMemberSource.isLevelCollapsed(
                    aggStar,
                    (RolapCubeLevel)currLevel);

            boolean multipleCols =
                SqlMemberSource.levelContainsMultipleColumns(currLevel);

            if (levelCollapsed && !multipleCols) {
                // if this is a single column collapsed level, there is
                // no need to join it with dimension tables
                RolapStar.Column starColumn =
                    ((RolapCubeLevel) currLevel).getStarKeyColumn();
                int bitPos = starColumn.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                String q = aggColumn.generateExprString(sqlQuery);
                sqlQuery.addSelectGroupBy(q, starColumn.getInternalType());
                if (whichSelect == WhichSelect.ONLY) {
                    sqlQuery.addOrderBy(q, true, false, true);
                }
                aggColumn.getTable().addToFrom(sqlQuery, false, true);
                continue;
            }

            MondrianDef.Expression keyExp = currLevel.getKeyExp();
            MondrianDef.Expression ordinalExp = currLevel.getOrdinalExp();
            MondrianDef.Expression captionExp = currLevel.getCaptionExp();
            MondrianDef.Expression parentExp = currLevel.getParentExp();

            if (parentExp != null) {
                if (!levelCollapsed) {
                    hierarchy.addToFrom(sqlQuery, parentExp);
                }
                String parentSql = parentExp.getExpression(sqlQuery);
                sqlQuery.addSelectGroupBy(
                    parentSql, currLevel.getInternalType());
                if (whichSelect == WhichSelect.LAST
                    || whichSelect == WhichSelect.ONLY)
                {
                    sqlQuery.addOrderBy(parentSql, true, false, true, false);
                }
            }

            String keySql = keyExp.getExpression(sqlQuery);
            String ordinalSql = ordinalExp.getExpression(sqlQuery);

            if (!levelCollapsed) {
                hierarchy.addToFrom(sqlQuery, keyExp);
                hierarchy.addToFrom(sqlQuery, ordinalExp);
            }
            String captionSql = null;
            if (captionExp != null) {
                captionSql = captionExp.getExpression(sqlQuery);
                if (!levelCollapsed) {
                    hierarchy.addToFrom(sqlQuery, captionExp);
                }
            }

            String alias =
                sqlQuery.addSelect(keySql, currLevel.getInternalType());
            if (needsGroupBy) {
                sqlQuery.addGroupBy(keySql, alias);
            }

            if (captionSql != null) {
                alias = sqlQuery.addSelect(captionSql, null);
                if (needsGroupBy) {
                    sqlQuery.addGroupBy(captionSql, alias);
                }
            }

            if (!ordinalSql.equals(keySql)) {
                alias = sqlQuery.addSelect(ordinalSql, null);
                if (needsGroupBy) {
                    sqlQuery.addGroupBy(ordinalSql, alias);
                }
            }

            constraint.addLevelConstraint(
                sqlQuery, baseCube, aggStar, currLevel);

            if (levelCollapsed) {
                // add join between key and aggstar
                // join to dimension tables starting
                // at the lowest granularity and working
                // towards the fact table
                hierarchy.addToFromInverse(sqlQuery, keyExp);

                RolapStar.Column starColumn =
                    ((RolapCubeLevel) currLevel).getStarKeyColumn();
                int bitPos = starColumn.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                RolapStar.Condition condition =
                    new RolapStar.Condition(keyExp, aggColumn.getExpression());
                sqlQuery.addWhere(condition.toString(sqlQuery));
            }

            if (whichSelect == WhichSelect.ONLY) {
                sqlQuery.addOrderBy(ordinalSql, true, false, true);
            }

            RolapProperty[] properties = currLevel.getProperties();
            for (RolapProperty property : properties) {
                final MondrianDef.Expression propExp = property.getExp();
                final String propSql;
                if (propExp instanceof MondrianDef.Column) {
                    // When dealing with a column, we must use the same table
                    // alias as the one used by the level. We also assume that
                    // the property lives in the same table as the level.
                    propSql =
                        sqlQuery.getDialect().quoteIdentifier(
                            currLevel.getTableAlias(),
                            ((MondrianDef.Column)propExp).name);
                } else {
                    propSql = property.getExp().getExpression(sqlQuery);
                }
                alias = sqlQuery.addSelect(propSql, null);
                if (needsGroupBy) {
                    // Certain dialects allow us to eliminate properties
                    // from the group by that are functionally dependent
                    // on the level value
                    if (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
                        || !property.dependsOnLevelValue())
                    {
                        sqlQuery.addGroupBy(propSql, alias);
                    }
                }
            }
        }
    }

    /**
     * Obtains the evaluator used to find an aggregate table to support
     * the Tuple constraint.
     *
     * @param constraint Constraint
     * @return evaluator for constraint
     */
    protected Evaluator getEvaluator(TupleConstraint constraint) {
        if (constraint instanceof SqlContextConstraint) {
            return constraint.getEvaluator();
        }
        if (constraint instanceof DescendantsConstraint) {
            DescendantsConstraint descConstraint =
                (DescendantsConstraint) constraint;
            MemberChildrenConstraint mcc =
                descConstraint.getMemberChildrenConstraint(null);
            if (mcc instanceof SqlContextConstraint) {
                SqlContextConstraint scc = (SqlContextConstraint) mcc;
                return scc.getEvaluator();
            }
        }
        return null;
    }

    /**
     * Obtains the AggStar instance which corresponds to an aggregate table
     * which can be used to support the member constraint.
     *
     * @param constraint The tuple constraint to apply.
     * @param evaluator the current evaluator to obtain the cube and members to
     *        be queried  @return AggStar for aggregate table
     * @param baseCube The base cube from which to choose an aggregation star.
     *        Can be null, in which case we use the evaluator's cube.
     */
    AggStar chooseAggStar(
        TupleConstraint constraint,
        Evaluator evaluator,
        RolapCube baseCube)
    {
        if (!MondrianProperties.instance().UseAggregates.get()) {
            return null;
        }

        if (evaluator == null) {
            return null;
        }

        if (baseCube == null) {
            baseCube = (RolapCube) evaluator.getCube();
        }

        // Current cannot support aggregate tables for virtual cubes
        if (baseCube.isVirtual()) {
            return null;
        }

        RolapStar star = baseCube.getStar();
        final int starColumnCount = star.getColumnCount();
        BitKey measureBitKey = BitKey.Factory.makeBitKey(starColumnCount);
        BitKey levelBitKey = BitKey.Factory.makeBitKey(starColumnCount);

        // Convert global ordinal to cube based ordinal (the 0th dimension
        // is always [Measures]). In the case of filter constraint this will
        // be the measure on which the filter will be done.

        // Since we support aggregated members as arguments, we'll expand
        // this too.
        // Failing to do so could result in chosing the wrong aggstar, as the
        // level would not be passed to the bitkeys
        final Member[] members =
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              evaluator.getNonAllMembers(), evaluator);

        // if measure is calculated, we can't continue
        if (!(members[0] instanceof RolapBaseCubeMeasure)) {
            return null;
        }

        RolapBaseCubeMeasure measure = (RolapBaseCubeMeasure)members[0];

        int bitPosition =
            ((RolapStar.Measure) measure.getStarMeasure()).getBitPosition();

        // set a bit for each level which is constrained in the context
        final CellRequest request =
            RolapAggregationManager.makeRequest(members);
        if (request == null) {
            // One or more calculated members. Cannot use agg table.
            return null;
        }
        // TODO: RME why is this using the array of constrained columns
        // from the CellRequest rather than just the constrained columns
        // BitKey (method getConstrainedColumnsBitKey)?
        RolapStar.Column[] columns = request.getConstrainedColumns();
        for (RolapStar.Column column1 : columns) {
            levelBitKey.set(column1.getBitPosition());
        }

        // set the masks
        for (TargetBase target : targets) {
            RolapLevel level = target.level;
            if (!level.isAll()) {
                RolapStar.Column column =
                    ((RolapCubeLevel)level).getBaseStarKeyColumn(baseCube);
                if (column != null) {
                    levelBitKey.set(column.getBitPosition());
                }
            }
        }

        // Set the bits for limited rollup members
        RolapUtil.constraintBitkeyForLimitedMembers(
            evaluator, evaluator.getMembers(), baseCube, levelBitKey);

        measureBitKey.set(bitPosition);

        if (constraint
            instanceof RolapNativeCrossJoin.NonEmptyCrossJoinConstraint)
        {
            // Cannot evaluate NonEmptyCrossJoinConstraint using an agg
            // table if one of its args is a DescendantsConstraint.
            RolapNativeCrossJoin.NonEmptyCrossJoinConstraint necj =
                (RolapNativeCrossJoin.NonEmptyCrossJoinConstraint)
                    constraint;
            for (CrossJoinArg arg : necj.args) {
                if (arg instanceof DescendantsCrossJoinArg
                    || arg instanceof MemberListCrossJoinArg)
                {
                    final RolapLevel level = arg.getLevel();
                    if (level != null && !level.isAll()) {
                        RolapStar.Column column =
                            ((RolapCubeLevel)level)
                                .getBaseStarKeyColumn(baseCube);
                        levelBitKey.set(column.getBitPosition());
                    }
                }
            }
        }

        // find the aggstar using the masks
        return AggregationManager.findAgg(
            star, levelBitKey, measureBitKey, new boolean[] {false});
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
