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
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.server.Statement;
import mondrian.server.monitor.SqlStatementEvent;
import mondrian.spi.Dialect;
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
 * {@link TupleConstraint#addLevelConstraint(mondrian.rolap.sql.SqlQuery, RolapStarSet, RolapLevel)}
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
 * {@link TupleConstraint#addConstraint(mondrian.rolap.sql.SqlQuery, RolapStarSet)}
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
    protected final List<Target> targets = new ArrayList<Target>();
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

    /**
     * Helper class for SqlTupleReader;
     * keeps track of target levels and constraints for adding to sql query.
     */
    private class Target {
        final List<RolapMember> srcMembers;
        final RolapLevel level;
        private RolapMember currMember;
        private List<RolapMember> list;
        final Object cacheLock;
        final TupleReader.MemberBuilder memberBuilder;

        final MemberCache cache;
        ColumnLayout columnLayout;

        RolapLevel[] levels;
        int levelDepth;
        boolean parentChild;
        List<RolapMember> members;
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
            this.srcMembers = srcMembers;
            this.level = level;
            cacheLock = memberBuilder.getMemberCacheLock();
            this.memberBuilder = memberBuilder;
            this.cache = memberBuilder.getMemberCache();
        }

        public void setList(final List<RolapMember> list) {
            this.list = list;
        }

        public List<RolapMember> getSrcMembers() {
            return srcMembers;
        }

        public RolapLevel getLevel() {
            return level;
        }

        public RolapMember getCurrMember() {
            return this.currMember;
        }

        public void setCurrMember(final RolapMember m) {
            this.currMember = m;
        }

        public List<RolapMember> getList() {
            return list;
        }

        public String toString() {
            return level.getUniqueName();
        }

        /**
         * Adds a row to the collection.
         *
         * @param stmt Statement
         * @throws SQLException On error
         */
        public final void addRow(SqlStatement stmt) throws SQLException {
            synchronized (cacheLock) {
                internalAddRow(stmt);
            }
        }

        public void add(final RolapMember member) {
            this.getList().add(member);
        }

        public void open() {
            levels = level.getHierarchy().getLevelList().toArray(
                new RolapLevel[
                    level.getHierarchy().getLevelList().size()]);
            setList(new ArrayList<RolapMember>());
            levelDepth = level.getDepth();
            parentChild = level.isParentChild();
            // members[i] is the current member of level#i, and siblings[i]
            // is the current member of level#i plus its siblings
            final int levelCount = levels.length;
            members =
                new ArrayList<RolapMember>(
                    Collections.<RolapMember>nCopies(levelCount, null));
            siblings = new ArrayList<List<RolapMember>>(levelCount + 1);
            for (int i = 0; i < levelCount + 1; i++) {
                siblings.add(new ArrayList<RolapMember>());
            }
        }

        void internalAddRow(
            SqlStatement stmt)
            throws SQLException
        {
            RolapMember member = null;
            if (getCurrMember() != null) {
                setCurrMember(member);
            } else {
                for (int i = 0; i <= levelDepth; i++) {
                    RolapLevel childLevel = levels[i];
                    final LevelColumnLayout layout =
                        columnLayout.levelLayoutMap.get(childLevel);
                    if (childLevel.isAll()) {
                        member = memberBuilder.allMember();
                        continue;
                    }
                    RolapMember parentMember = member;
                    final List<SqlStatement.Accessor> accessors =
                        stmt.getAccessors();
                    pc:
                    if (parentChild) {
                        Comparable[] parentKeys =
                            new Comparable[layout.parentOrdinals.length];
                        for (int j = 0; j < layout.parentOrdinals.length;
                             j++)
                        {
                            int parentOrdinal = layout.parentOrdinals[j];
                            Comparable value =
                                accessors.get(parentOrdinal).get();
                            if (value == null) {
                                // member is at top of hierarchy; its parent is
                                // the 'all' member. Convert null to placeholder
                                // value for uniformity in hashmaps.
                                break pc;
                            } else if (value.toString().equals(
                                    childLevel.getNullParentValue()))
                            {
                                // member is at top of hierarchy; its parent is
                                // the 'all' member
                                break pc;
                            } else {
                                parentKeys[j] = value;
                            }
                        }
                        Object parentKey =
                            parentKeys.length == 1
                                ? parentKeys[0]
                                : Arrays.asList(parentKeys);
                        parentMember = cache.getMember(level, parentKey);
                        if (parentMember == null) {
                            LOGGER.warn(
                                MondrianResource.instance()
                                    .LevelTableParentNotFound.str(
                                        childLevel.getUniqueName(),
                                        parentKey.toString()));
                        }
                    }
                    Comparable[] keyValues =
                        new Comparable[layout.keyOrdinals.length];
                    for (int j = 0; j < layout.keyOrdinals.length; j++) {
                        int keyOrdinal = layout.keyOrdinals[j];
                        Comparable value = accessors.get(keyOrdinal).get();
                        keyValues[j] = SqlMemberSource.toComparable(value);
                    }
                    final Object key = RolapMember.Key.quick(keyValues);
                    member = cache.getMember(childLevel, key);
                    if (member == null) {
                        if (constraint instanceof
                            RolapNativeCrossJoin.NonEmptyCrossJoinConstraint
                            && childLevel.isParentChild())
                        {
                            member =
                                ((RolapNativeCrossJoin
                                    .NonEmptyCrossJoinConstraint) constraint)
                                    .findMember(key);
                        }
                        if (member == null) {
                            final Comparable keyClone =
                                RolapMember.Key.create(keyValues);
                            final Comparable captionValue;
                            if (layout.captionOrdinal >= 0) {
                                captionValue =
                                    accessors.get(layout.captionOrdinal).get();
                            } else {
                                captionValue = null;
                            }
                            final Comparable nameObject;
                            final String nameValue;
                            if (layout.nameOrdinal >= 0) {
                                nameObject =
                                    accessors.get(layout.nameOrdinal).get();
                                nameValue =
                                    nameObject == null
                                        ? RolapUtil.mdxNullLiteral()
                                        : String.valueOf(nameObject);
                            } else {
                                nameObject = null;
                                nameValue = null;
                            }
                            final Comparable orderKey;
                            switch (layout.orderBySource) {
                            case NONE:
                                orderKey = null;
                                break;
                            case KEY:
                                orderKey = keyClone;
                                break;
                            case NAME:
                                orderKey = nameObject;
                                break;
                            case MAPPED:
                                orderKey =
                                    SqlMemberSource.getCompositeKey(
                                        accessors, layout.orderByOrdinals);
                                break;
                            default:
                                throw Util.unexpected(layout.orderBySource);
                            }
                            member = memberBuilder.makeMember(
                                parentMember, childLevel, keyClone,
                                captionValue, nameValue,
                                orderKey, parentChild, stmt, layout);
                        }
                    }

                    final RolapMember prevMember = members.get(i);
                    // TODO: is this block ever entered?
                    if (member != prevMember && prevMember != null) {
                        // Flush list we've been building.
                        List<RolapMember> children = siblings.get(i + 1);
                        if (children != null) {
                            MemberChildrenConstraint mcc =
                                constraint.getMemberChildrenConstraint(
                                    prevMember);
                            if (mcc != null) {
                                cache.putChildren(
                                    prevMember, mcc, children);
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
                            siblings.set(i + 1, null);
                        }
                        // Record new current member of this level.
                        members.set(i, member);
                        // If we're building a list of siblings at this level,
                        // we haven't seen this one before, so add it.
                        if (siblings.get(i) != null) {
                            if (keyValues == null) {
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
        }

        public void setColumnLayout(ColumnLayout columnLayout) {
            this.columnLayout = columnLayout;
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
            if (target.getSrcMembers() != null) {
                enumTargetCount++;
            }
        }
        return enumTargetCount;
    }

    private void prepareTuples(
        Dialect dialect,
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
                List<Target> partialTargets = new ArrayList<Target>();
                for (Target target : targets) {
                    if (target.srcMembers == null) {
                        partialTargets.add(target);
                    }
                }
                final Pair<String, List<SqlStatement.Type>> pair =
                    makeLevelMembersSql(dialect);
                String sql = pair.left;
                List<SqlStatement.Type> types = pair.right;
                assert sql != null && !sql.equals("");

                stmt = RolapUtil.executeQuery(
                    dataSource, sql, types, maxRows, 0,
                    new SqlStatement.StatementLocus(
                        getExecution(),
                        "SqlTupleReader.readTuples " + partialTargets,
                        message,
                        SqlStatementEvent.Purpose.TUPLES, 0),
                    -1, -1, null);
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
                    for (Target target : targets) {
                        target.setCurrMember(null);
                        target.addRow(stmt);
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

    /**
     * Gets an appropriate Execution based on the state of the current
     * locus.  Used for setting StatementLocus.
     */
    private Execution getExecution() {
        assert targets.size() > 0;
        if (Locus.peek().execution.getMondrianStatement()
            .getMondrianConnection().getSchema() != null)
        {
            // the current locus has a statement that's associated with
            // a schema.  Use it.
            return Locus.peek().execution;
        } else {
            // no schema defined in the current locus.  This could
            // happen during schema load.  Construct a new execution associated
            // with the schema.
            Statement statement = targets.get(0)
                .getLevel()
                .getHierarchy()
                .getRolapSchema()
                .getInternalConnection()
                .getInternalStatement();
            return new Execution(statement, 0);
        }
    }

    public TupleList readMembers(
        Dialect dialect,
        DataSource dataSource,
        TupleList partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        int memberCount = countMembers();
        while (true) {
            missedMemberCount = 0;
            int memberCountBefore = memberCount;
            prepareTuples(dialect, dataSource, partialResult, newPartialResult);
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
        for (Target target : targets) {
            if (target.getList() != null) {
                n += target.getList().size();
            }
        }
        return n;
    }

    public TupleList readTuples(
        Dialect dialect,
        DataSource dataSource,
        TupleList partialResult,
        List<List<RolapMember>> newPartialResult)
    {
        prepareTuples(dialect, dataSource, partialResult, newPartialResult);

        // List of tuples
        final int n = targets.size();
        @SuppressWarnings({"unchecked"})
        final Iterator<Member>[] iter = new Iterator[n];
        for (int i = 0; i < n; i++) {
            Target t = targets.get(i);
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
        for (Target target : targets) {
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
                    srcMemberIdxes, stmt, message);
            } else {
                // form a cross product using the columns from the current
                // result set row and the current members that recursion
                // has reached for the enum targets
                int enumTargetIdx = 0;
                for (Target target : targets) {
                    if (target.srcMembers == null) {
                        try {
                            target.addRow(stmt);
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
        for (Target target : targets) {
            if (target.srcMembers == null) {
                row.add(target.getCurrMember());
            }
        }
        partialResult.add(row);
    }

    Pair<String, List<SqlStatement.Type>> makeLevelMembersSql(Dialect dialect) {
        // In the case of a virtual cube, if we need to join to the fact
        // table, we do not necessarily have a single underlying fact table,
        // as the underlying base cubes in the virtual cube may all reference
        // different fact tables.
        //
        // Therefore, we need to gather the underlying fact tables by going
        // through the list of measures referenced in the query.  And then
        // we generate one sub-select per fact table, joining against each
        // underlying fact table, unioning the sub-selects.
        final List<RolapMeasureGroup> measureGroupList;
        if (constraint.isJoinRequired()) {
            measureGroupList = constraint.getMeasureGroupList();
        } else if (constraint.getEvaluator() != null
            && constraint.getEvaluator().isNonEmpty())
        {
            measureGroupList = Collections.singletonList(
                constraint.getEvaluator().getMeasureGroup());
        } else {
            measureGroupList = Collections.emptyList();
        }

        switch (measureGroupList.size()) {
        default:
            // generate sub-selects, each one joining with one of
            // the fact table referenced

            List<RolapMeasureGroup> joiningMeasureGroupList =
                getFullyJoiningMeasureGroups(measureGroupList);
            if (joiningMeasureGroupList.size() == 0) {
                return sqlForEmptyTuple(dialect, measureGroupList);
            }

            // Save the original measure in the context
//          Member originalMeasure = constraint.getEvaluator().getMembers()[0];
            StringBuilder buf = new StringBuilder();
            List<SqlStatement.Type> types = null;
            for (int i = 0; i < joiningMeasureGroupList.size(); i++) {
                final RolapMeasureGroup measureGroup =
                    joiningMeasureGroupList.get(i);
                // Use the measure from the corresponding base cube in the
                // context to find the correct join path to the base fact
                // table.
                //
                // Any measure is fine since the constraint logic only uses it
                // to find the correct fact table to join to.
                Util.deprecated(
                    "todo: push the star into the context somehow, and remove this commented-out logic",
                    false);
//              Member measureInCurrentbaseCube = star.getMeasures().get(0);
//              constraint.getEvaluator().setContext(
//                  measureInCurrentbaseCube);

                if (i > 0) {
                    buf.append(Util.nl)
                        .append("union")
                        .append(Util.nl);
                }
                Pair<String, List<SqlStatement.Type>> pair =
                    generateSelectForLevels(
                        dialect, null,
                        measureGroup, i, joiningMeasureGroupList.size());
                buf.append(pair.left);
                types = pair.right;
            }

            // Restore the original measure member
//            constraint.getEvaluator().setContext(originalMeasure);
            return Pair.of(buf.toString(), types);

        case 1:
            return generateSelectForLevels(
                dialect, null, measureGroupList.get(0), 0, 1);

        case 0:
            return generateSelectForLevels(dialect, null, null, 0, 1);
        }
    }

    private List<RolapMeasureGroup> getFullyJoiningMeasureGroups(
        List<RolapMeasureGroup> measureGroupList)
    {
        final List<RolapMeasureGroup> fullyJoiningCubes =
            new ArrayList<RolapMeasureGroup>();
        for (RolapMeasureGroup measureGroup : measureGroupList) {
            boolean allTargetsJoin = true;
            for (Target target : targets) {
                if (!targetIsOnBaseCube(target, measureGroup)) {
                    allTargetsJoin = false;
                }
            }
            if (allTargetsJoin) {
                fullyJoiningCubes.add(measureGroup);
            }
        }
        return fullyJoiningCubes;
    }

    Pair<String, List<SqlStatement.Type>> sqlForEmptyTuple(
        Dialect dialect,
        final List<RolapMeasureGroup> measureGroupList)
    {
        final SqlQuery sqlQuery = SqlQuery.newQuery(dialect, null);
        sqlQuery.addSelect("0", null);
        sqlQuery.addFrom(
            measureGroupList.get(0).getStar().getFactTable().getRelation(),
            null, true);
        sqlQuery.addWhere("1 = 0");
        return sqlQuery.toSqlAndTypes();
    }

    /**
     * Generates the SQL string corresponding to the levels referenced.
     *
     * @param dialect Database dialect
     * @param measureGroup Measure group whose fact table to join to, or null
     * @param selectOrdinal Ordinal of this SELECT statement in UNION
     * @param selectCount Number of SELECT statements in UNION
     * @return SQL statement string and types
     */
    Pair<String, List<SqlStatement.Type>> generateSelectForLevels(
        Dialect dialect,
        RolapCube baseCube,
        RolapMeasureGroup measureGroup,
        int selectOrdinal,
        int selectCount)
    {
        String s =
            "while generating query to retrieve members of level(s) " + targets;
Util.deprecated("obsolete basecube parameter", false);
        // Allow query to use optimization hints from the table definition
        SqlQuery sqlQuery = SqlQuery.newQuery(dialect, s);
        sqlQuery.setAllowHints(true);

        Evaluator evaluator = getEvaluator(constraint);
        final RolapStarSet starSet;
        if (measureGroup != null) {
            final AggStar aggStar =
                chooseAggStar(constraint, measureGroup, evaluator);
            final RolapMeasureGroup aggMeasureGroup = null; // TODO:
            starSet =
                new RolapStarSet(
                    measureGroup.getStar(), measureGroup, aggMeasureGroup);
        } else {
            starSet = new RolapStarSet(null, null, null);
        }

        // Find targets whose members are not enumerated.
        // if we're going to be enumerating the values for this target,
        // then we don't need to generate sql for it.
        List<Target> unevaluatedTargets = new ArrayList<Target>();

        // Distinct dimensions. (In case two or more levels come from the same
        // dimension, e.g. [Customer].[Gender] and [Customer].[Marital Status].)
        final Set<RolapDimension> dimensions =
            new LinkedHashSet<RolapDimension>();

        for (Target target : targets) {
            if (target.getSrcMembers() == null) {
                unevaluatedTargets.add(target);
                dimensions.add(target.level.getDimension());
            }
        }

        // add the selects for all levels to fetch
        if (!unevaluatedTargets.isEmpty()) {
            ColumnLayoutBuilder columnLayoutBuilder =
                new ColumnLayoutBuilder();
            RolapSchema.SqlQueryBuilder queryBuilder =
                new RolapSchema.SqlQueryBuilder(
                    sqlQuery,
                    columnLayoutBuilder,
                    Collections.<List<RolapSchema.PhysColumn>>emptyList());

            if (measureGroup != null) {
                for (RolapDimension dimension : dimensions) {
                    // Join each dimension to the measure group's fact table.
                    final RolapSchema.PhysPath path =
                        measureGroup.getPath(dimension);
                    for (RolapSchema.PhysHop hop : path.hopList) {
                        if (hop.link != null) {
                            queryBuilder.sqlQuery.addWhere(hop.link.sql);
                        }
                        queryBuilder.addRelation(hop.relation, false);
                    }
                }
            } else if (MondrianProperties.instance()
                    .FilterChildlessSnowflakeMembers.get())
            {
                // start at lowest level of each dimension
                for (RolapDimension dimension : dimensions) {
                    // skip degenerate dimensions, which have no key attribute
                    if (dimension.keyAttribute != null) {
                        queryBuilder.addListToFrom(
                            dimension.keyAttribute.getKeyList());
                    }
                }
            } else {
                // start at target level
                for (Target target : unevaluatedTargets) {
                    queryBuilder.addListToFrom(
                        target.level.attribute.getKeyList());
                }
            }

            for (Target target : unevaluatedTargets) {
                addLevelMemberSql(
                    queryBuilder,
                    target.getLevel(),
                    starSet,
                    selectOrdinal,
                    selectCount);
                target.setColumnLayout(
                    queryBuilder.layoutBuilder.toLayout());
            }
        }

        constraint.addConstraint(sqlQuery, starSet);

        return sqlQuery.toSqlAndTypes();
    }

    boolean targetIsOnBaseCube(
        Target target,
        RolapMeasureGroup measureGroup)
    {
        Util.deprecated("disallow baseCube==null", false);
        return measureGroup == null
            || measureGroup.existsLink(target.getLevel().getDimension());
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
        List<RolapLevel> levels,
        int levelDepth)
    {
        // REVIEW: The functionality of this method in mondrian-3.x depended on
        // the attribute Hierarchy.uniqueKeyLevelName, which does not exist in
        // mondrian-4.0.
        return true;
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
     * @param queryBuilder the query object being constructed
     * @param level level to be added to the sql query
     * @param starSet Star set
     * @param selectOrdinal Ordinal of this SELECT statement in UNION
     * @param selectCount Number of SELECT statements in UNION
     */
    protected void addLevelMemberSql(
        RolapSchema.SqlQueryBuilder queryBuilder,
        RolapLevel level,
        final RolapStarSet starSet,
        int selectOrdinal,
        int selectCount)
    {
        final SqlQuery sqlQuery = queryBuilder.sqlQuery;
        final ColumnLayoutBuilder layoutBuilder = queryBuilder.layoutBuilder;
        RolapHierarchy hierarchy = level.getHierarchy();
        RolapCubeDimension cubeDimension = null;

        // lookup RolapHierarchy of base cube that matches this hierarchy
        if (hierarchy instanceof RolapCubeHierarchy) {
            RolapCubeHierarchy cubeHierarchy = (RolapCubeHierarchy) hierarchy;
            cubeDimension = cubeHierarchy.getDimension();
            if (starSet.cube != null
                && !cubeHierarchy.getCube().equals(starSet.cube))
            {
                Util.deprecated("don't think this is ever the case", true);
                // replace the hierarchy with the underlying base cube hierarchy
                // in the case of virtual cubes
                hierarchy = starSet.cube.findBaseCubeHierarchy(hierarchy);
            }
        }

        final List<RolapLevel> levels = Util.cast(hierarchy.getLevelList());
        int levelDepth = level.getDepth();

        boolean needsGroupBy =
            isGroupByNeeded(sqlQuery, hierarchy, levels, levelDepth);
        boolean isUnion = selectCount > 1;

        final RolapMeasureGroup measureGroup = starSet.getMeasureGroup();

        for (int i = 0; i <= levelDepth; i++) {
            final RolapLevel currLevel = levels.get(i);
            if (currLevel.isAll()) {
                continue;
            }

            final LevelLayoutBuilder levelLayoutBuilder =
                layoutBuilder.createLayoutFor(currLevel);

            // Determine if the aggregate table contains the collapsed level
            boolean levelCollapsed =
                (starSet.getAggStar() != null)
                && SqlMemberSource.isLevelCollapsed(
                    starSet.getAggStar(), (RolapCubeLevel) level, measureGroup);
            if (levelCollapsed) {
                // an earlier check was made in chooseAggStar() to verify
                // that this is a single column level
                RolapCubeLevel currCubeLevel = (RolapCubeLevel) currLevel;
                RolapStar.Column starColumn =
                    currCubeLevel.getBaseStarKeyColumn(
                        measureGroup);
                int bitPos = starColumn.getBitPosition();
                AggStar.Table.Column aggColumn =
                    starSet.getAggStar().lookupColumn(bitPos);
                String q = aggColumn.generateExprString(sqlQuery);
                String alias =
                    sqlQuery.addSelectGroupBy(
                        q,
                        starColumn.getExpression().getInternalType());
                layoutBuilder.register(q, alias);
                sqlQuery.addOrderBy(q, true, false, true);
                aggColumn.getTable().addToFrom(sqlQuery, false, true);
                continue;
            }
            final RolapAttribute attribute = currLevel.getAttribute();

            if (currLevel.getParentAttribute() != null) {
                List<RolapSchema.PhysColumn> parentExps =
                    currLevel.getParentAttribute().getKeyList();
                SqlMemberSource.Sgo sgo =
                    selectOrdinal == selectCount - 1
                        ? SqlMemberSource.Sgo.SELECT_GROUP_ORDER
                        : SqlMemberSource.Sgo.SELECT_GROUP;
                for (RolapSchema.PhysColumn parentExp : parentExps) {
                    levelLayoutBuilder.parentOrdinalList.add(
                        queryBuilder.addColumn(parentExp, sgo));
                    if (starSet.cube != null
                        && !levelCollapsed
                        && measureGroup != null)
                    {
                        parentExp.joinToStarRoot(
                            sqlQuery,
                            measureGroup,
                            cubeDimension);
                    }
                }
            }

            SqlMemberSource.Sgo sgo =
                isUnion
                    ? SqlMemberSource.Sgo.SELECT.maybeGroup(needsGroupBy)
                    : SqlMemberSource.Sgo.SELECT_ORDER.maybeGroup(needsGroupBy);

            for (RolapSchema.PhysColumn column : currLevel.getOrderByList()) {
                levelLayoutBuilder.orderByOrdinalList.add(
                    queryBuilder.addColumn(column, sgo));
            }

            for (RolapSchema.PhysColumn column : attribute.getKeyList()) {
                levelLayoutBuilder.keyOrdinalList.add(
                    queryBuilder.addColumn(column, sgo));
                if (measureGroup != null) {
                    column.joinToStarRoot(
                        sqlQuery, measureGroup, cubeDimension);
                }
            }


            levelLayoutBuilder.nameOrdinal =
                queryBuilder.addColumn(
                    attribute.getNameExp(),
                    SqlMemberSource.Sgo.SELECT.maybeGroup(needsGroupBy));

            levelLayoutBuilder.captionOrdinal =
                queryBuilder.addColumn(
                    attribute.getCaptionExp(),
                    SqlMemberSource.Sgo.SELECT.maybeGroup(needsGroupBy));
            if (attribute.getCaptionExp() != null) {
                if (starSet.cube != null) {
                    Util.deprecated(
                        "join to layoutbuilder key sufficient?",
                        false);
                    if (measureGroup != null) {
                        attribute.getCaptionExp().joinToStarRoot(
                            sqlQuery, measureGroup, cubeDimension);
                    }
                }
            }

            constraint.addLevelConstraint(
                sqlQuery, starSet, currLevel);

            if (levelCollapsed) {
                // add join between key and aggstar
                // join to dimension tables starting
                // at the lowest granularity and working
                // towards the fact table
                for (RolapSchema.PhysColumn column : attribute.getKeyList()) {
                    hierarchy.addToFromInverse(sqlQuery, column);
                }

                RolapCubeLevel currCubeLevel = (RolapCubeLevel) currLevel;
                RolapStar.Column starColumn =
                    currCubeLevel.getBaseStarKeyColumn(
                        measureGroup);
                int bitPos = starColumn.getBitPosition();
                AggStar.Table.Column aggColumn =
                    starSet.getAggStar().lookupColumn(bitPos);
                assert attribute.getKeyList().size() == 1 : "TODO:";
                sqlQuery.addWhere(
                    aggColumn.getExpression().toSql()
                    + " = "
                    + attribute.getKeyList().get(0).toSql());
            }

            // If this is a select on a virtual cube, the query will be
            // a union, so the order by columns need to be numbers,
            // not column name strings or expressions.
            if (isUnion && selectOrdinal == selectCount - 1) {
                addUnionOrderByOrdinal(sqlQuery);
            }

            if (selectOrdinal == 0 && selectCount == 1) {
                for (RolapSchema.PhysColumn column : currLevel.getOrderByList())
                {
                    sqlQuery.addOrderBy(column.toSql(), true, false, true);
                }
            }

            for (RolapProperty property
                : currLevel.attribute.getExplicitProperties())
            {
                // FIXME: For now assume that properties have a single-column
                //    key and name etc. are the same.
                assert property.attribute.getKeyList().size() == 1;
                RolapSchema.PhysColumn column =
                    property.attribute.getKeyList().get(0);
                String propSql = column.toSql();
                int ordinal = layoutBuilder.lookup(propSql);
                if (ordinal < 0) {
                    String alias =
                        sqlQuery.addSelect(propSql, column.getInternalType());
                    ordinal = layoutBuilder.register(propSql, alias);
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
                levelLayoutBuilder.propertyOrdinalList.add(ordinal);
            }
        }

        // Add lower levels' relations to the FROM clause to filter out members
        // that have no children. For backwards compatibility, but less
        // efficient.
        if (measureGroup != null) {
            for (RolapSchema.PhysColumn column : level.attribute.getKeyList()) {
                final RolapSchema.PhysPath keyPath =
                    level.getDimension().getKeyPath(column);
                keyPath.addToFrom(sqlQuery, false);
            }
        }
    }

    private static void addUnionOrderByOrdinal(final SqlQuery sqlQuery)
    {
        // If this is a select on a virtual cube, the query will be
        // a union, so the order by columns need to be numbers,
        // not column name strings or expressions.
        boolean nullable = true;
        final Dialect dialect = sqlQuery.getDialect();
        if (dialect.requiresUnionOrderByExprToBeInSelectClause()
            || dialect.requiresUnionOrderByOrdinal())
        {
            // If the expression is nullable and the dialect
            // sorts NULL values first, the dialect will try to
            // add an expression 'Iif(expr IS NULL, 1, 0)' into
            // the ORDER BY clause, and that is not allowed by this
            // dialect. So, pretend that the expression is not
            // nullable. NULL values, if present, will be sorted
            // wrong, but that's better than generating an invalid
            // query.
            nullable = false;
        }
        sqlQuery.addOrderBy(
            Integer.toString(
                sqlQuery.getCurrentSelectListSize()),
            true, false, nullable, false);
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
     * @param constraint Constraint
     * @param measureGroup1 Measure group
     * @param evaluator the current evaluator to obtain the cube and members to
     *        be queried
     * @return AggStar for aggregate table
     */
    AggStar chooseAggStar(
        TupleConstraint constraint,
        RolapMeasureGroup measureGroup1,
        Evaluator evaluator)
    {
        if (!MondrianProperties.instance().UseAggregates.get()) {
            return null;
        }

        if (evaluator == null) {
            return null;
        }

        // Convert global ordinal to cube based ordinal (the 0th dimension
        // is always [Measures]). In the case of filter constraint this will
        // be the measure on which the filter will be done.
        final Member[] members = evaluator.getNonAllMembers();

        // if measure is calculated, we can't continue
        if (!(members[0] instanceof RolapBaseCubeMeasure)) {
            return null;
        }
        RolapBaseCubeMeasure measure = (RolapBaseCubeMeasure)members[0];
        int bitPosition = measure.getStarMeasure().getBitPosition();

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
        final RolapMeasureGroup measureGroup = measure.getMeasureGroup();
        final RolapStar star = measureGroup.getStar();
        final int starColumnCount = star.getColumnCount();
        BitKey measureBitKey = BitKey.Factory.makeBitKey(starColumnCount);
        BitKey levelBitKey = BitKey.Factory.makeBitKey(starColumnCount);

        RolapStar.Column[] columns = request.getConstrainedColumns();
        for (RolapStar.Column column1 : columns) {
            levelBitKey.set(column1.getBitPosition());
        }

        // set the masks
        for (Target target : targets) {
            RolapCubeLevel level = (RolapCubeLevel) target.level;
            if (!level.isAll()) {
                RolapStar.Column starColumn =
                    level.getBaseStarKeyColumn(measureGroup);
                if (starColumn != null) {
                    levelBitKey.set(starColumn.getBitPosition());
                }
            }
        }

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
                        final RolapCubeLevel cubeLevel = (RolapCubeLevel) level;
                        for (RolapSchema.PhysColumn physColumn
                            : cubeLevel.attribute.getKeyList())
                        {
                            RolapStar.Column column =
                                measureGroup1.getRolapStarColumn(
                                    cubeLevel.cubeDimension,
                                    physColumn,
                                    true);
                            levelBitKey.set(column.getBitPosition());
                        }
                    }
                }
            }
        }

        // find the aggstar using the masks
        return AggregationManager.findAgg(
            star, levelBitKey, measureBitKey, new boolean[]{false});
    }

    int getMaxRows() {
        return maxRows;
    }

    void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    /**
     *
     * @see Util#deprecated(Object) add javadoc and make top level
     */
    static class ColumnLayoutBuilder {
        private final List<String> exprList = new ArrayList<String>();
        private final List<String> aliasList = new ArrayList<String>();
        private final Map<RolapLevel, LevelLayoutBuilder> levelLayoutMap =
            new IdentityHashMap<RolapLevel, LevelLayoutBuilder>();
        LevelLayoutBuilder currentLevelLayout;
        final List<SqlStatement.Type> types =
            new ArrayList<SqlStatement.Type>();

        /**
         * Creates a ColumnLayoutBuilder.
         */
        ColumnLayoutBuilder() {
        }

        /**
         * Returns the ordinal of a given expression in the SELECT clause.
         *
         * @param sql SQL expression
         * @return Ordinal of expression, or -1 if not found
         */
        public int lookup(String sql) {
            return exprList.indexOf(sql);
        }

        /**
         * Registers a given expression in the SELECT clause, or searches for
         * an existing expression, and returns the ordinal.
         *
         * @param sql SQL expression
         * @param alias Alias, or null
         * @return Ordinal of expression
         */
        public int register(String sql, String alias) {
            int ordinal = exprList.size();
            exprList.add(sql);
            aliasList.add(alias);
            return ordinal;
        }

        public ColumnLayout toLayout() {
            return new ColumnLayout(convert(levelLayoutMap.values()));
        }

        private Map<RolapLevel, LevelColumnLayout> convert(
            Collection<LevelLayoutBuilder> builders)
        {
            final Map<RolapLevel, LevelColumnLayout> map =
                new IdentityHashMap<RolapLevel, LevelColumnLayout>();
            for (LevelLayoutBuilder builder : builders) {
                if (builder != null) {
                    map.put(builder.level, convert(builder));
                }
            }
            return map;
        }

        private LevelColumnLayout convert(LevelLayoutBuilder builder) {
            return builder == null ? null : builder.toLayout();
        }

        public LevelLayoutBuilder createLayoutFor(RolapLevel level) {
            LevelLayoutBuilder builder = levelLayoutMap.get(level);
            if (builder == null) {
                builder = new LevelLayoutBuilder(level);
                levelLayoutMap.put(level, builder);
            }
            currentLevelLayout = builder;
            return builder;
        }
    }

    /**
     * Builder for {@link LevelColumnLayout}.
     *
     * @see Util#deprecated(Object) make top level
     */
    static class LevelLayoutBuilder {
        public List<Integer> keyOrdinalList = new ArrayList<Integer>();
        public int nameOrdinal = -1;
        public List<Integer> orderByOrdinalList = new ArrayList<Integer>();
        public int captionOrdinal = -1;
        final List<Integer> propertyOrdinalList = new ArrayList<Integer>();
        private final List<Integer> parentOrdinalList =
            new ArrayList<Integer>();
        public final RolapLevel level;

        public LevelLayoutBuilder(RolapLevel level) {
            this.level = level;
        }

        public LevelColumnLayout toLayout() {
            boolean assignOrderKeys =
                MondrianProperties.instance().CompareSiblingsByOrderKey.get()
                || Util.deprecated(true, false); // TODO: remove property

            OrderKeySource orderBySource = OrderKeySource.NONE;
            if (assignOrderKeys) {
                if (orderByOrdinalList.equals(keyOrdinalList)) {
                    orderBySource = OrderKeySource.KEY;
                } else if (orderByOrdinalList.equals(
                        Collections.singletonList(nameOrdinal)))
                {
                    orderBySource = OrderKeySource.NAME;
                } else {
                    orderBySource = OrderKeySource.MAPPED;
                }
            }
            return new LevelColumnLayout(
                toArray(keyOrdinalList),
                nameOrdinal,
                captionOrdinal,
                orderBySource,
                orderBySource == OrderKeySource.MAPPED
                    ? toArray(orderByOrdinalList)
                    : null,
                toArray(propertyOrdinalList),
                toArray(parentOrdinalList));
        }

        private static int[] toArray(List<Integer> list) {
            final int[] ints = new int[list.size()];
            for (int i = 0; i < ints.length; i++) {
                ints[i] = list.get(i);
            }
            return ints;
        }
    }

    /**
     * Description of where to find attributes within each row.
     */
    static class ColumnLayout {
        final Map<RolapLevel, LevelColumnLayout> levelLayoutMap;

        public ColumnLayout(
            final Map<RolapLevel, LevelColumnLayout> levelLayoutMap)
        {
            this.levelLayoutMap = levelLayoutMap;
        }
    }

    static class LevelColumnLayout {
        // column ordinals where the values of the level's key (possibly
        // compound) are found
        public final int[] keyOrdinals;
        // column ordinal where the value of the level's name is found
        public final int nameOrdinal;
        // column ordinals where the value of the level's caption is found,
        // or -1 if no caption
        public final int captionOrdinal;
        public final OrderKeySource orderBySource;
        // column ordinals where the ordinal expression is found
        public final int[] orderByOrdinals;
        // column ordinals where the values of the level's properties are found
        public final int[] propertyOrdinals;
        // column ordinals of the fields that contain this member's parent
        // member (in a parent child hierarchy)
        public final int[] parentOrdinals;

        LevelColumnLayout(
            int[] keyOrdinals,
            int nameOrdinal,
            int captionOrdinal,
            OrderKeySource orderBySource,
            int[] orderByOrdinals,
            int[] propertyOrdinals,
            int[] parentOrdinals)
        {
            this.keyOrdinals = keyOrdinals;
            this.nameOrdinal = nameOrdinal;
            this.captionOrdinal = captionOrdinal;
            this.orderBySource = orderBySource;
            this.orderByOrdinals = orderByOrdinals;
            this.propertyOrdinals = propertyOrdinals;
            this.parentOrdinals = parentOrdinals;
        }
    }

    enum OrderKeySource {
        NONE,
        KEY,
        NAME,
        MAPPED
    }
}

// End SqlTupleReader.java
