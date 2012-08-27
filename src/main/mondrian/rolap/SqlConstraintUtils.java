/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
 */
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.MondrianDef.RelationOrJoin;
import mondrian.olap.Role.RollupPolicy;
import mondrian.rolap.agg.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;
import mondrian.util.FilteredIterableList;

import java.util.*;

/**
 * Utility class used by implementations of {@link mondrian.rolap.sql.SqlConstraint},
 * used to generate constraints into {@link mondrian.rolap.sql.SqlQuery}.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class SqlConstraintUtils {

    /** Utility class */
    private SqlConstraintUtils() {
    }

    /**
     * For every restricting member in the current context, generates
     * a WHERE condition and a join to the fact table.
     *
     * @param sqlQuery the query to modify
     * @param aggStar Aggregate table, or null if query is against fact table
     * @param restrictMemberTypes defines the behavior if the current context
     *   contains calculated members. If true, thows an exception.
     * @param evaluator Evaluator
     */
    public static void addContextConstraint(
        SqlQuery sqlQuery,
        AggStar aggStar,
        Evaluator evaluator,
        boolean restrictMemberTypes)
    {
        // Add constraint using the current evaluator context
        Member[] members = evaluator.getNonAllMembers();
        RolapCube baseCube = null;
        if (evaluator instanceof RolapEvaluator) {
            baseCube = ((RolapEvaluator)evaluator).getCube();
        }

        Map<RelationOrJoin, Set<RolapMember>> mapOfSlicerMembers =
                new HashMap<RelationOrJoin, Set<RolapMember>>();
        Map<RelationOrJoin, Boolean> done =
                new HashMap<RelationOrJoin, Boolean>();
        if (restrictMemberTypes) {
            if (containsCalculatedMember(members)) {
                throw Util.newInternal(
                    "can not restrict SQL to calculated Members");
            }
            if (hasMultiPositionSlicer(evaluator)) {
                List<Member> slicerMembers =
                        ((RolapEvaluator)evaluator).getSlicerMembers();

                for (Member slicerMember : slicerMembers) {
                    RelationOrJoin rel =
                            ((RolapCubeHierarchy)slicerMember.getHierarchy())
                            .getRelation();
                    if (!mapOfSlicerMembers.containsKey(rel)) {
                        mapOfSlicerMembers.put(rel, new HashSet<RolapMember>());
                    }
                    mapOfSlicerMembers.get(rel).add((RolapMember)slicerMember);
                }
            }

        } else {
            members = removeCalculatedAndDefaultMembers(members);
            members = removeMultiPositionSlicerMembers(members, evaluator);
        }
        final CellRequest request =
            RolapAggregationManager.makeRequest(members);
        if (request == null) {
            if (restrictMemberTypes) {
                throw Util.newInternal("CellRequest is null - why?");
            }
            // One or more of the members was null or calculated, so the
            // request is impossible to satisfy.
            return;
        }
        RolapStar.Column[] columns = request.getConstrainedColumns();
        Object[] values = request.getSingleValues();
        int arity = columns.length;
        // following code is similar to
        // AbstractQuerySpec#nonDistinctGenerateSQL()
        for (int i = 0; i < arity; i++) {
            RolapStar.Column column = columns[i];

            String expr;
            if (aggStar != null) {
                int bitPos = column.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                AggStar.Table table = aggColumn.getTable();
                table.addToFrom(sqlQuery, false, true);
                expr = aggColumn.generateExprString(sqlQuery);
            } else {
                RolapStar.Table table = column.getTable();
                table.addToFrom(sqlQuery, false, true);
                expr = column.generateExprString(sqlQuery);
            }

            final String value = String.valueOf(values[i]);
            if ((RolapUtil.mdxNullLiteral().equalsIgnoreCase(value))
                || (value.equalsIgnoreCase(RolapUtil.sqlNullValue.toString())))
            {
                sqlQuery.addWhere(
                    expr,
                    " is ",
                    RolapUtil.sqlNullLiteral);
            } else {
                if (column.getDatatype().isNumeric()) {
                    // make sure it can be parsed
                    Double.valueOf(value);
                }

                RelationOrJoin keyForSlicerMap =
                        column.getTable().getRelation();
                if (mapOfSlicerMembers.containsKey(keyForSlicerMap)) {
                    if (!done.containsKey(keyForSlicerMap)) {
                        Set<RolapMember> slicerMembersArray =
                                mapOfSlicerMembers.get(keyForSlicerMap);
                        List<RolapMember> slicerMembers =
                                new ArrayList<RolapMember>(slicerMembersArray);

                        RolapMember allMember = null;
                        for (RolapMember slicerMember : slicerMembers) {
                            if (slicerMember.isAll()) {
                                allMember = slicerMember;
                                break;
                            }
                        }

                        if (allMember != null) {
                            slicerMembers.remove(allMember);
                        }

                        if (slicerMembers.size() > 0) {
                            int levelIndex = slicerMembers.get(0)
                                    .getHierarchy()
                                    .getLevels().length;
                            RolapLevel levelForWhere =
                                    (RolapLevel) slicerMembers.get(0)
                                    .getHierarchy()
                                    .getLevels()[levelIndex - 1];
                            final String where =
                                    generateMultiValueInExpr(
                                        sqlQuery, baseCube,
                                        aggStar, slicerMembers,
                                        levelForWhere,
                                        restrictMemberTypes, null);
                            sqlQuery.addWhere(where);
                        } else {
                            //No extra slicers.... just use the = method
                            final StringBuilder buf = new StringBuilder();
                            sqlQuery.getDialect().quote(
                                buf, value,
                                column.getDatatype());
                            sqlQuery.addWhere(
                                expr,
                                " = ",
                                buf.toString());
                        }
                        done.put(keyForSlicerMap, Boolean.TRUE);
                    }

                } else {
                    final StringBuilder buf = new StringBuilder();
                    sqlQuery.getDialect().quote(
                        buf, value,
                        column.getDatatype());
                    sqlQuery.addWhere(
                        expr,
                        " = ",
                        buf.toString());
                }
            }
        }

        // force Role based Access filtering
        Map<RelationOrJoin, String> whereClausesForRoleConstraints =
            new HashMap<RelationOrJoin, String>();
        SchemaReader schemaReader = evaluator.getSchemaReader();
        Member[] mm = evaluator.getMembers();
        for (int mIndex = 0; mIndex < mm.length; mIndex++) {
            if (mm[mIndex] instanceof RolapHierarchy.LimitedRollupMember
                || mm[mIndex] instanceof
                   RestrictedMemberReader.MultiCardinalityDefaultMember)
            {
                List<Level> hierarchyLevels = schemaReader
                        .getHierarchyLevels(mm[mIndex].getHierarchy());
                for (Level affectedLevel : hierarchyLevels) {
                    List<RolapMember> slicerMembers =
                        new ArrayList<RolapMember>();

                    List<Member> availableMembers = schemaReader
                            .getLevelMembers(affectedLevel, false);
                    for (Member member : availableMembers) {
                        if (!member.isAll()) {
                            slicerMembers.add((RolapMember) member);
                        }
                    }

                    if (slicerMembers.size() > 0) {
                        int levelIndex =
                            slicerMembers.get(0).getHierarchy()
                                .getLevels().length;
                        RolapLevel levelForWhere =
                            (RolapLevel) slicerMembers
                                .get(0).getHierarchy()
                                .getLevels()[levelIndex - 1];
                        final String where =
                                generateMultiValueInExpr(
                                    sqlQuery, baseCube,
                                    aggStar, slicerMembers,
                                    levelForWhere,
                                    restrictMemberTypes, null);

                        RelationOrJoin rel =
                            ((RolapCubeHierarchy) slicerMembers
                                .get(0).getHierarchy()).getRelation();
                        whereClausesForRoleConstraints.put(rel, where);
                    }
                }
            }
        }
        for (RelationOrJoin key : whereClausesForRoleConstraints.keySet()) {
            sqlQuery.addWhere(whereClausesForRoleConstraints.get(key));
        }
    }

    /**
     * Looks at the given <code>evaluator</code> to determine if it has more
     * than one slicer member from any particular hierarchy.
     * @param evaluator the evaluator to look at
     * @return <code>true</code> if the evaluator's slicer has more than one
     *  member from any particular hierarchy
     */
    public static boolean hasMultiPositionSlicer(Evaluator evaluator) {
        if (evaluator instanceof RolapEvaluator) {
            Map<Hierarchy, Member> mapOfSlicerMembers =
                new HashMap<Hierarchy, Member>();
            for (
                Member slicerMember
                : ((RolapEvaluator)evaluator).getSlicerMembers())
            {
                Hierarchy hierarchy = slicerMember.getHierarchy();
                if (mapOfSlicerMembers.containsKey(hierarchy)) {
                    // We have found a second member in this hierarchy
                    return true;
                }
                mapOfSlicerMembers.put(hierarchy, slicerMember);
            }
        }
        return false;
    }

    protected static Member[] removeMultiPositionSlicerMembers(
        Member[] members,
        Evaluator evaluator)
    {
        List<Member> slicerMembers = null;
        if (evaluator instanceof RolapEvaluator) {
            // get the slicer members from the evaluator
            slicerMembers =
                ((RolapEvaluator)evaluator).getSlicerMembers();
        }
        if (slicerMembers != null) {
            // Iterate the list of slicer members, grouping them by hierarchy
            Map<Hierarchy, Set<Member>> mapOfSlicerMembers =
                new HashMap<Hierarchy, Set<Member>>();
            for (Member slicerMember : slicerMembers) {
                Hierarchy hierarchy = slicerMember.getHierarchy();
                if (!mapOfSlicerMembers.containsKey(hierarchy)) {
                    mapOfSlicerMembers.put(hierarchy, new HashSet<Member>());
                }
                mapOfSlicerMembers.get(hierarchy).add(slicerMember);
            }
            List<Member> listOfMembers = new ArrayList<Member>();
            // Iterate the given list of members, removing any whose hierarchy
            // has multiple members on the slicer axis
            for (Member member : members) {
                Hierarchy hierarchy = member.getHierarchy();
                if (!mapOfSlicerMembers.containsKey(hierarchy)
                        || mapOfSlicerMembers.get(hierarchy).size() < 2)
                {
                    listOfMembers.add(member);
                }
            }
            members = listOfMembers.toArray(new Member[listOfMembers.size()]);
        }
        return members;
    }

    /**
     * Removes calculated and default members from an array.
     *
     * <p>This is required only if the default member is
     * not the ALL member. The time dimension for example, has 1997 as default
     * member. When we evaluate the query
     * <pre>
     *   select NON EMPTY crossjoin(
     *     {[Time].[1998]}, [Customer].[All].children
     *  ) on columns
     *   from [sales]
     * </pre>
     * the <code>[Customer].[All].children</code> is evaluated with the default
     * member <code>[Time].[1997]</code> in the evaluator context. This is wrong
     * because the NON EMPTY must filter out Customers with no rows in the fact
     * table for 1998 not 1997. So we do not restrict the time dimension and
     * fetch all children.
     *
     * <p>For calculated members, effect is the same as
     * {@link #removeCalculatedMembers(java.util.List)}.
     *
     * @param members Array of members
     * @return Members with calculated members removed (except those that are
     *    leaves in a parent-child hierarchy) and with members that are the
     *    default member of their hierarchy
     */
    private static Member[] removeCalculatedAndDefaultMembers(
        Member[] members)
    {
        List<Member> memberList = new ArrayList<Member>(members.length);
        for (int i = 0; i < members.length; ++i) {
            Member member = members[i];
            // Skip calculated members (except if leaf of parent-child hier)
            if (member.isCalculated() && !member.isParentChildLeaf()) {
                continue;
            }

            // Remove members that are the default for their hierarchy,
            // except for the measures hierarchy.
            if (i > 0
                && member.getHierarchy().getDefaultMember().equals(member))
            {
                continue;
            }
            memberList.add(member);
        }
        return memberList.toArray(new Member[memberList.size()]);
    }

    static List<Member> removeCalculatedMembers(List<Member> members) {
        return new FilteredIterableList<Member>(
            members,
            new FilteredIterableList.Filter<Member>() {
                public boolean accept(final Member m) {
                    return !m.isCalculated() || m.isParentChildLeaf();
                }
            });
    }

    public static boolean containsCalculatedMember(Member[] members) {
        for (Member member : members) {
            if (member.isCalculated()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Ensures that the table of <code>level</code> is joined to the fact
     * table
     *
     * @param sqlQuery sql query under construction
     * @param aggStar
     * @param e evaluator corresponding to query
     * @param level level to be added to query
     */
    public static void joinLevelTableToFactTable(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        Evaluator e,
        RolapCubeLevel level)
    {
        RolapStar.Column starColumn = level.getBaseStarKeyColumn(baseCube);
        if (aggStar != null) {
            int bitPos = starColumn.getBitPosition();
            AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
            AggStar.Table table = aggColumn.getTable();
            table.addToFrom(sqlQuery, false, true);
        } else {
            RolapStar.Table table = starColumn.getTable();
            assert table != null;
            table.addToFrom(sqlQuery, false, true);
        }
    }

    /**
     * Creates a "WHERE parent = value" constraint.
     *
     * @param sqlQuery the query to modify
     * @param baseCube base cube if virtual
     * @param aggStar Definition of the aggregate table, or null
     * @param parent the list of parent members
     * @param restrictMemberTypes defines the behavior if <code>parent</code>
     * is a calculated member. If true, an exception is thrown
     */
    public static void addMemberConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapMember parent,
        boolean restrictMemberTypes)
    {
        List<RolapMember> list = Collections.singletonList(parent);
        boolean exclude = false;
        addMemberConstraint(
            sqlQuery, baseCube, aggStar, list, restrictMemberTypes, false,
            exclude);
    }

    /**
     * Creates a "WHERE exp IN (...)" condition containing the values
     * of all parents.  All parents must belong to the same level.
     *
     * <p>If this constraint is part of a native cross join, there are
     * multiple constraining members, and the members comprise the cross
     * product of all unique member keys referenced at each level, then
     * generating IN expressions would result in incorrect results.  In that
     * case, "WHERE ((level1 = val1a AND level2 = val2a AND ...)
     * OR (level1 = val1b AND level2 = val2b AND ...) OR ..." is generated
     * instead.
     *
     * @param sqlQuery the query to modify
     * @param baseCube base cube if virtual
     * @param aggStar (not used)
     * @param members the list of members for this constraint
     * @param restrictMemberTypes defines the behavior if <code>parents</code>
     *   contains calculated members.
     *   If true, and one of the members is calculated, an exception is thrown.
     * @param crossJoin true if constraint is being generated as part of
     *   a native crossjoin
     * @param exclude whether to exclude the members in the SQL predicate.
     *  e.g. not in { member list}.
     */
    public static void addMemberConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        List<RolapMember> members,
        boolean restrictMemberTypes,
        boolean crossJoin,
        boolean exclude)
    {
        if (members.size() == 0) {
            // Generate a predicate which is always false in order to produce
            // the empty set.  It would be smarter to avoid executing SQL at
            // all in this case, but doing it this way avoid special-case
            // evaluation code.
            String predicate = "(1 = 0)";
            if (exclude) {
                predicate = "(1 = 1)";
            }
            sqlQuery.addWhere(predicate);
            return;
        }

        // Find out the first(lowest) unique parent level.
        // Only need to compare members up to that level.
        RolapMember member = members.get(0);
        RolapLevel memberLevel = member.getLevel();
        RolapMember firstUniqueParent = member;
        RolapLevel firstUniqueParentLevel = null;
        for (; firstUniqueParent != null
                 && !firstUniqueParent.getLevel().isUnique();
             firstUniqueParent = firstUniqueParent.getParentMember())
        {
        }

        if (firstUniqueParent != null) {
            // There's a unique parent along the hierarchy
            firstUniqueParentLevel = firstUniqueParent.getLevel();
        }

        String condition = "(";

        // If this constraint is part of a native cross join and there
        // are multiple values for the parent members, then we can't
        // use single value IN clauses
        if (crossJoin
            && !memberLevel.isUnique()
            && !membersAreCrossProduct(members))
        {
            assert (member.getParentMember() != null);
            condition +=
                constrainMultiLevelMembers(
                    sqlQuery,
                    baseCube,
                    aggStar,
                    members,
                    firstUniqueParentLevel,
                    restrictMemberTypes,
                    exclude);
        } else {
            condition +=
                generateSingleValueInExpr(
                    sqlQuery,
                    baseCube,
                    aggStar,
                    members,
                    firstUniqueParentLevel,
                    restrictMemberTypes,
                    exclude);
        }

        if (condition.length() > 1) {
            // condition is not empty
            condition += ")";
            sqlQuery.addWhere(condition);
        }
    }

    private static StarColumnPredicate getColumnPredicates(
        RolapStar.Column column,
        Collection<RolapMember> members)
    {
        switch (members.size()) {
        case 0:
            return new LiteralStarPredicate(column, false);
        case 1:
            return new MemberColumnPredicate(column, members.iterator().next());
        default:
            List<StarColumnPredicate> predicateList =
                new ArrayList<StarColumnPredicate>();
            for (RolapMember member : members) {
                predicateList.add(new MemberColumnPredicate(column, member));
            }
            return new ListColumnPredicate(column, predicateList);
        }
    }

    private static LinkedHashSet<RolapMember> getUniqueParentMembers(
        Collection<RolapMember> members)
    {
        LinkedHashSet<RolapMember> set = new LinkedHashSet<RolapMember>();
        for (RolapMember m : members) {
            m = m.getParentMember();
            if (m != null) {
                set.add(m);
            }
        }
        return set;
    }

    /**
     * Adds to the where clause of a query expression matching a specified
     * list of members
     *
     * @param sqlQuery query containing the where clause
     * @param baseCube base cube if virtual
     * @param aggStar aggregate star if available
     * @param members list of constraining members
     * @param fromLevel lowest parent level that is unique
     * @param restrictMemberTypes defines the behavior when calculated members
     * are present
     * @param exclude whether to exclude the members. Default is false.
     *
     * @return a non-empty String if SQL is generated for the multi-level
     * member list.
     */
    private static String constrainMultiLevelMembers(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        List<RolapMember> members,
        RolapLevel fromLevel,
        boolean restrictMemberTypes,
        boolean exclude)
    {
        // Use LinkedHashMap so that keySet() is deterministic.
        Map<RolapMember, List<RolapMember>> parentChildrenMap =
            new LinkedHashMap<RolapMember, List<RolapMember>>();
        StringBuilder condition = new StringBuilder();
        StringBuilder condition1 = new StringBuilder();
        if (exclude) {
            condition.append("not (");
        }

        // First try to generate IN list for all members
        if (sqlQuery.getDialect().supportsMultiValueInExpr()) {
            condition1.append(
                generateMultiValueInExpr(
                    sqlQuery,
                    baseCube,
                    aggStar,
                    members,
                    fromLevel,
                    restrictMemberTypes,
                    parentChildrenMap));

            // The members list might contain NULL values in the member levels.
            //
            // e.g.
            //   [USA].[CA].[San Jose]
            //   [null].[null].[San Francisco]
            //   [null].[null].[Los Angeles]
            //   [null].[CA].[San Diego]
            //   [null].[CA].[Sacramento]
            //
            // Pick out such members to generate SQL later.
            // These members are organized in a map that maps the parant levels
            // containing NULL to all its children members in the list. e.g.
            // the member list above becomes the following map, after SQL is
            // generated for [USA].[CA].[San Jose] in the call above.
            //
            //   [null].[null]->([San Francisco], [Los Angeles])
            //   [null]->([CA].[San Diego], [CA].[Sacramento])
            //
            if (parentChildrenMap.isEmpty()) {
                condition.append(condition1.toString());
                if (exclude) {
                    // If there are no NULL values in the member levels, then
                    // we're done except we need to also explicitly include
                    // members containing nulls across all levels.
                    condition.append(")");
                    condition.append(" or ");
                    condition.append(
                        generateMultiValueIsNullExprs(
                            sqlQuery,
                            baseCube,
                            members.get(0),
                            fromLevel));
                }
                return condition.toString();
            }
        } else {
            // Multi-value IN list not supported
            // Classify members into List that share the same parent.
            //
            // Using the same example as above, the resulting map will be
            //   [USA].[CA]->[San Jose]
            //   [null].[null]->([San Francisco], [Los Angesles])
            //   [null].[CA]->([San Diego],[Sacramento])
            //
            // The idea is to be able to "compress" the original member list
            // into groups that can use single value IN list for part of the
            // comparison that does not involve NULLs
            //
            for (RolapMember m : members) {
                if (m.isCalculated()) {
                    if (restrictMemberTypes) {
                        throw Util.newInternal(
                            "addMemberConstraint: cannot "
                            + "restrict SQL to calculated member :" + m);
                    }
                    continue;
                }
                RolapMember p = m.getParentMember();
                List<RolapMember> childrenList = parentChildrenMap.get(p);
                if (childrenList == null) {
                    childrenList = new ArrayList<RolapMember>();
                    parentChildrenMap.put(p, childrenList);
                }
                childrenList.add(m);
            }
        }

        // Now we try to generate predicates for the remaining
        // parent-children group.

        // Note that NULLs are not used to enforce uniqueness
        // so we ignore the fromLevel here.
        boolean firstParent = true;
        StringBuilder condition2 = new StringBuilder();

        if (condition1.length() > 0) {
            // Some members have already been translated into IN list.
            firstParent = false;
            condition.append(condition1.toString());
            condition.append(" or ");
        }

        RolapLevel memberLevel = members.get(0).getLevel();

        // The children for each parent are turned into IN list so they
        // should not contain null.
        for (RolapMember p : parentChildrenMap.keySet()) {
            assert p != null;
            if (condition2.toString().length() > 0) {
                condition2.append(" or ");
            }

            condition2.append("(");

            // First generate ANDs for all members in the parent lineage of
            // this parent-children group
            int levelCount = 0;
            for (RolapMember gp = p; gp != null; gp = gp.getParentMember()) {
                if (gp.isAll()) {
                    // Ignore All member
                    // Get the next parent
                    continue;
                }

                RolapLevel level = gp.getLevel();

                // add the level to the FROM clause if this is the
                // first parent-children group we're generating sql for
                if (firstParent) {
                    RolapHierarchy hierarchy = level.getHierarchy();

                    // this method can be called within the context of shared
                    // members, outside of the normal rolap star, therefore
                    // we need to check the level to see if it is a shared or
                    // cube level.

                    RolapStar.Column column = null;
                    if (level instanceof RolapCubeLevel) {
                        column = ((RolapCubeLevel)level).getBaseStarKeyColumn(
                            baseCube);
                    }
                    if (column != null) {
                        if (aggStar != null) {
                            int bitPos = column.getBitPosition();
                            AggStar.Table.Column aggColumn =
                                aggStar.lookupColumn(bitPos);
                            AggStar.Table table = aggColumn.getTable();
                            table.addToFrom(sqlQuery, false, true);
                        } else {
                            RolapStar.Table targetTable = column.getTable();
                            hierarchy.addToFrom(sqlQuery, targetTable);
                        }
                    } else {
                        assert (aggStar == null);
                        hierarchy.addToFrom(sqlQuery, level.getKeyExp());
                    }
                }

                if (levelCount > 0) {
                    condition2.append(" and ");
                }
                ++levelCount;

                condition2.append(
                    constrainLevel(
                        level,
                        sqlQuery,
                        baseCube,
                        aggStar,
                        getColumnValue(
                            level.nameExp != null
                                ? gp.getName()
                                : gp.getKey(),
                            sqlQuery.getDialect(),
                            level.getDatatype()),
                        false));
                if (gp.getLevel() == fromLevel) {
                    // SQL is completely generated for this parent
                    break;
                }
            }
            firstParent = false;

            // Next, generate children for this parent-children group
            List<RolapMember> children = parentChildrenMap.get(p);

            // If no children to be generated for this parent then we are done
            if (!children.isEmpty()) {
                Map<RolapMember, List<RolapMember>> tmpParentChildrenMap =
                    new HashMap<RolapMember, List<RolapMember>>();

                if (levelCount > 0) {
                    condition2.append(" and ");
                }
                RolapLevel childrenLevel =
                    (RolapLevel)(p.getLevel().getChildLevel());

                if (sqlQuery.getDialect().supportsMultiValueInExpr()
                    && childrenLevel != memberLevel)
                {
                    // Multi-level children and multi-value IN list supported
                    condition2.append(
                        generateMultiValueInExpr(
                            sqlQuery,
                            baseCube,
                            aggStar,
                            children,
                            childrenLevel,
                            restrictMemberTypes,
                            tmpParentChildrenMap));
                    assert tmpParentChildrenMap.isEmpty();
                } else {
                    // Can only be single level children
                    // If multi-value IN list not supported, children will be on
                    // the same level as members list. Only single value IN list
                    // needs to be generated for this case.
                    assert childrenLevel == memberLevel;
                    condition2.append(
                        generateSingleValueInExpr(
                            sqlQuery,
                            baseCube,
                            aggStar,
                            children,
                            childrenLevel,
                            restrictMemberTypes,
                            false));
                }
            }
            // SQL is complete for this parent-children group.
            condition2.append(")");
        }

        // In the case where multi-value IN expressions are not generated,
        // condition2 contains the entire filter condition.  In the
        // case of excludes, we also need to explicitly include null values,
        // minus the ones that are referenced in condition2.  Therefore,
        // we OR on a condition that corresponds to an OR'ing of IS NULL
        // filters on each level PLUS an exclusion of condition2.
        //
        // Note that the expression generated is non-optimal in the case where
        // multi-value IN's cannot be used because we end up excluding
        // non-null values as well as the null ones.  Ideally, we only need to
        // exclude the expressions corresponding to nulls, which is possible
        // in the multi-value IN case, since we have a map of the null values.
        condition.append(condition2.toString());
        if (exclude) {
            condition.append(") or (");
            condition.append(
                generateMultiValueIsNullExprs(
                    sqlQuery,
                    baseCube,
                    members.get(0),
                    fromLevel));
            condition.append(" and not(");
            condition.append(condition2.toString());
            condition.append("))");
        }

        return condition.toString();
    }

    /**
     * @param members list of members
     *
     * @return true if the members comprise the cross product of all unique
     * member keys referenced at each level
     */
    private static boolean membersAreCrossProduct(List<RolapMember> members)
    {
        int crossProdSize = getNumUniqueMemberKeys(members);
        for (Collection<RolapMember> parents = getUniqueParentMembers(members);
            !parents.isEmpty(); parents = getUniqueParentMembers(parents))
        {
            crossProdSize *= parents.size();
        }
        return (crossProdSize == members.size());
    }

    /**
     * @param members list of members
     *
     * @return number of unique member keys in a list of members
     */
    private static int getNumUniqueMemberKeys(List<RolapMember> members)
    {
        final HashSet<Object> set = new HashSet<Object>();
        for (RolapMember m : members) {
            set.add(m.getKey());
        }
        return set.size();
    }

    /**
     * @param key key corresponding to a member
     * @param dialect sql dialect being used
     * @param datatype data type of the member
     *
     * @return string value corresponding to the member
     */
    private static String getColumnValue(
        Object key,
        Dialect dialect,
        Dialect.Datatype datatype)
    {
        if (key != RolapUtil.sqlNullValue) {
            return key.toString();
        } else {
            return RolapUtil.mdxNullLiteral();
        }
    }

    /**
     * Generates a sql expression constraining a level by some value
     *
     * @param level the level
     * @param query the query that the sql expression will be added to
     * @param baseCube base cube for virtual levels
     * @param aggStar aggregate star if available
     * @param columnValue value constraining the level
     * @param caseSensitive if true, need to handle case sensitivity of the
     * member value
     *
     * @return generated string corresponding to the expression
     */
    public static String constrainLevel(
        RolapLevel level,
        SqlQuery query,
        RolapCube baseCube,
        AggStar aggStar,
        String columnValue,
        boolean caseSensitive)
    {
        // this method can be called within the context of shared members,
        // outside of the normal rolap star, therefore we need to
        // check the level to see if it is a shared or cube level.

        RolapStar.Column column = null;
        if (level instanceof RolapCubeLevel) {
            column = ((RolapCubeLevel)level).getBaseStarKeyColumn(baseCube);
        }

        String columnString;
        Dialect.Datatype datatype;
        if (column != null) {
            if (column.getNameColumn() == null) {
                datatype = level.getDatatype();
            } else {
                column = column.getNameColumn();
                // The schema doesn't specify the datatype of the name column,
                // but we presume that it is a string.
                datatype = Dialect.Datatype.String;
            }
            if (aggStar != null) {
                // this makes the assumption that the name column is the same
                // as the key column
                int bitPos = column.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                columnString = aggColumn.generateExprString(query);
            } else {
                columnString = column.generateExprString(query);
            }
        } else {
            assert (aggStar == null);
            MondrianDef.Expression exp = level.getNameExp();
            if (exp == null) {
                exp = level.getKeyExp();
                datatype = level.getDatatype();
            } else {
                // The schema doesn't specify the datatype of the name column,
                // but we presume that it is a string.
                datatype = Dialect.Datatype.String;
            }
            columnString = exp.getExpression(query);
        }

        String constraint;

        if (RolapUtil.mdxNullLiteral().equalsIgnoreCase(columnValue)) {
            constraint = columnString + " is " + RolapUtil.sqlNullLiteral;
        } else {
            if (datatype.isNumeric()) {
                // make sure it can be parsed
                Double.valueOf(columnValue);
            }
            final StringBuilder buf = new StringBuilder();
            query.getDialect().quote(buf, columnValue, datatype);
            String value = buf.toString();
            if (caseSensitive && datatype == Dialect.Datatype.String) {
                // Some databases (like DB2) compare case-sensitive. We convert
                // the value to upper-case in the DBMS (e.g. UPPER('Foo'))
                // rather than in Java (e.g. 'FOO') in case the DBMS is running
                // a different locale.
                if (!MondrianProperties.instance().CaseSensitive.get()) {
                    columnString = query.getDialect().toUpper(columnString);
                    value = query.getDialect().toUpper(value);
                }
            }

            constraint = columnString + " = " + value;
        }

        return constraint;
    }

    /**
     * Generates a sql expression constraining a level by some value
     *
     * @param exp Key expression
     * @param datatype Key datatype
     * @param query the query that the sql expression will be added to
     * @param columnValue value constraining the level
     *
     * @return generated string corresponding to the expression
     */
    public static String constrainLevel2(
        SqlQuery query,
        MondrianDef.Expression exp,
        Dialect.Datatype datatype,
        Comparable columnValue)
    {
        String columnString = exp.getExpression(query);
        if (columnValue == RolapUtil.sqlNullValue) {
            return columnString + " is " + RolapUtil.sqlNullLiteral;
        } else {
            final StringBuilder buf = new StringBuilder();
            buf.append(columnString);
            buf.append(" = ");
            query.getDialect().quote(buf, columnValue, datatype);
            return buf.toString();
        }
    }

    /**
     * Generates a multi-value IN expression corresponding to a list of
     * member expressions, and adds the expression to the WHERE clause
     * of a query, provided the member values are all non-null
     *
     * @param sqlQuery query containing the where clause
     * @param baseCube base cube if virtual
     * @param aggStar aggregate star if available
     * @param members list of constraining members
     * @param fromLevel lowest parent level that is unique
     * @param restrictMemberTypes defines the behavior when calculated members
     *        are present
     * @param parentWithNullToChildrenMap upon return this map contains members
     *        that have Null values in its (parent) levels
     * @return a non-empty String if multi-value IN list was generated for some
     *        members
     */
    private static String generateMultiValueInExpr(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        List<RolapMember> members,
        RolapLevel fromLevel,
        boolean restrictMemberTypes,
        Map<RolapMember, List<RolapMember>> parentWithNullToChildrenMap)
    {
        final StringBuilder columnBuf = new StringBuilder();
        final StringBuilder valueBuf = new StringBuilder();
        final StringBuilder memberBuf = new StringBuilder();

        columnBuf.append("(");

        // generate the left-hand side of the IN expression
        int ordinalInMultiple = 0;
        for (RolapMember m = members.get(0); m != null; m = m.getParentMember())
        {
            if (m.isAll()) {
                continue;
            }
            RolapLevel level = m.getLevel();
            RolapHierarchy hierarchy = level.getHierarchy();

            // this method can be called within the context of shared members,
            // outside of the normal rolap star, therefore we need to
            // check the level to see if it is a shared or cube level.

            RolapStar.Column column = null;
            if (level instanceof RolapCubeLevel) {
                column = ((RolapCubeLevel)level).getBaseStarKeyColumn(baseCube);
            }

            // REVIEW: The following code mostly uses the name column (or name
            // expression) of the level. Shouldn't it use the key column (or key
            // expression)?
            String columnString;
            if (column != null) {
                if (aggStar != null) {
                    // this assumes that the name column is identical to the
                    // id column
                    int bitPos = column.getBitPosition();
                    AggStar.Table.Column aggColumn =
                        aggStar.lookupColumn(bitPos);
                    AggStar.Table table = aggColumn.getTable();
                    table.addToFrom(sqlQuery, false, true);
                    columnString = aggColumn.generateExprString(sqlQuery);
                } else {
                    RolapStar.Table targetTable = column.getTable();
                    hierarchy.addToFrom(sqlQuery, targetTable);
                    columnString = column.generateExprString(sqlQuery);
                }
            } else {
                assert (aggStar == null);
                hierarchy.addToFrom(sqlQuery, level.getKeyExp());

                MondrianDef.Expression nameExp = level.getNameExp();
                if (nameExp == null) {
                    nameExp = level.getKeyExp();
                }
                columnString = nameExp.getExpression(sqlQuery);
            }

            if (ordinalInMultiple++ > 0) {
                columnBuf.append(", ");
            }

            columnBuf.append(columnString);

            // Only needs to compare up to the first(lowest) unique level.
            if (m.getLevel() == fromLevel) {
                break;
            }
        }

        columnBuf.append(")");

        // generate the RHS of the IN predicate
        valueBuf.append("(");
        int memberOrdinal = 0;
        for (RolapMember m : members) {
            if (m.isCalculated()) {
                if (restrictMemberTypes) {
                    throw Util.newInternal(
                        "addMemberConstraint: cannot "
                        + "restrict SQL to calculated member :" + m);
                }
                continue;
            }

            ordinalInMultiple = 0;
            memberBuf.setLength(0);
            memberBuf.append("(");

            boolean containsNull = false;
            for (RolapMember p = m; p != null; p = p.getParentMember()) {
                if (p.isAll()) {
                    // Ignore the ALL level.
                    // Generate SQL condition for the next level
                    continue;
                }
                RolapLevel level = p.getLevel();

                String value = getColumnValue(
                    p.getKey(),
                    sqlQuery.getDialect(),
                    level.getDatatype());

                // If parent at a level is NULL, record this parent and all
                // its children(if there's any)
                if (RolapUtil.mdxNullLiteral().equalsIgnoreCase(value)) {
                    // Add to the nullParent map
                    List<RolapMember> childrenList =
                        parentWithNullToChildrenMap.get(p);
                    if (childrenList == null) {
                        childrenList = new ArrayList<RolapMember>();
                        parentWithNullToChildrenMap.put(p, childrenList);
                    }

                    // If p has children
                    if (m != p) {
                        childrenList.add(m);
                    }

                    // Skip generating condition for this parent
                    containsNull = true;
                    break;
                }

                if (ordinalInMultiple++ > 0) {
                    memberBuf.append(", ");
                }

                sqlQuery.getDialect().quote(
                    memberBuf, value, level.getDatatype());

                // Only needs to compare up to the first(lowest) unique level.
                if (p.getLevel() == fromLevel) {
                    break;
                }
            }

            // Now check if sql string is sucessfully generated for this member.
            // If parent levels do not contain NULL then SQL must have been
            // generated successfully.
            if (!containsNull) {
                memberBuf.append(")");
                if (memberOrdinal++ > 0) {
                    valueBuf.append(", ");
                }
                valueBuf.append(memberBuf);
            }
        }

        StringBuilder condition = new StringBuilder();
        if (memberOrdinal > 0) {
            // SQLs are generated for some members.
            condition.append(columnBuf);
            condition.append(" in ");
            condition.append(valueBuf);
            condition.append(")");
        }

        return condition.toString();
    }

    /**
     * Generates an expression that is an OR of IS NULL expressions, one
     * per level in a RolapMember.
     *
     * @param sqlQuery query corresponding to the expression
     * @param baseCube base cube if virtual
     * @param member the RolapMember
     * @param fromLevel lowest parent level that is unique
     *
     * @return the text of the expression
     */
    private static String generateMultiValueIsNullExprs(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        RolapMember member,
        RolapLevel fromLevel)
    {
        final StringBuilder conditionBuf = new StringBuilder();

        conditionBuf.append("(");

        // generate the left-hand side of the IN expression
        boolean isFirstLevelInMultiple = true;
        for (RolapMember m = member; m != null; m = m.getParentMember()) {
            if (m.isAll()) {
                continue;
            }
            RolapLevel level = m.getLevel();

            // this method can be called within the context of shared members,
            // outside of the normal rolap star, therefore we need to
            // check the level to see if it is a shared or cube level.

            RolapStar.Column column = null;
            if (level instanceof RolapCubeLevel) {
                column = ((RolapCubeLevel)level).getBaseStarKeyColumn(baseCube);
            }

            String columnString;
            if (column != null) {
                RolapStar.Column nameColumn = column.getNameColumn();
                if (nameColumn == null) {
                    nameColumn = column;
                }
                columnString = nameColumn.generateExprString(sqlQuery);
            } else {
                MondrianDef.Expression nameExp = level.getNameExp();
                if (nameExp == null) {
                    nameExp = level.getKeyExp();
                }
                columnString = nameExp.getExpression(sqlQuery);
            }

            if (!isFirstLevelInMultiple) {
                conditionBuf.append(" or ");
            } else {
                isFirstLevelInMultiple = false;
            }

            conditionBuf.append(columnString);
            conditionBuf.append(" is null");

            // Only needs to compare up to the first(lowest) unique level.
            if (m.getLevel() == fromLevel) {
                break;
            }
        }

        conditionBuf.append(")");
        return conditionBuf.toString();
    }

    /**
     * Generates a multi-value IN expression corresponding to a list of
     * member expressions, and adds the expression to the WHERE clause
     * of a query, provided the member values are all non-null
     *
     * @param sqlQuery query containing the where clause
     * @param baseCube base cube if virtual
     * @param aggStar aggregate star if available
     * @param members list of constraining members
     * @param fromLevel lowest parent level that is unique
     * @param restrictMemberTypes defines the behavior when calculated members
     *        are present
     * @param exclude whether to exclude the members. Default is false.
     * @return a non-empty String if IN list was generated for the members.
     */
    private static String generateSingleValueInExpr(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        List<RolapMember> members,
        RolapLevel fromLevel,
        boolean restrictMemberTypes,
        boolean exclude)
    {
        int maxConstraints =
            MondrianProperties.instance().MaxConstraints.get();
        Dialect dialect = sqlQuery.getDialect();

        String condition = "";
        boolean firstLevel = true;
        for (Collection<RolapMember> c = members;
            !c.isEmpty();
            c = getUniqueParentMembers(c))
        {
            RolapMember m = c.iterator().next();
            if (m.isAll()) {
                continue;
            }
            if (m.isNull()) {
                return "1 = 0";
            }
            if (m.isCalculated() && !m.isParentChildLeaf()) {
                if (restrictMemberTypes) {
                    throw Util.newInternal(
                        "addMemberConstraint: cannot "
                        + "restrict SQL to calculated member :" + m);
                }
                continue;
            }

            boolean containsNullKey = false;
            Iterator<RolapMember> it = c.iterator();
            while (it.hasNext()) {
                m = it.next();
                if (m.getKey() == RolapUtil.sqlNullValue) {
                    containsNullKey = true;
                }
            }

            RolapLevel level = m.getLevel();
            RolapHierarchy hierarchy = level.getHierarchy();

            // this method can be called within the context of shared members,
            // outside of the normal rolap star, therefore we need to
            // check the level to see if it is a shared or cube level.

            RolapStar.Column column = null;
            if (level instanceof RolapCubeLevel) {
                column = ((RolapCubeLevel)level).getBaseStarKeyColumn(baseCube);
            }

            String q;
            if (column != null) {
                if (aggStar != null) {
                    int bitPos = column.getBitPosition();
                    AggStar.Table.Column aggColumn =
                        aggStar.lookupColumn(bitPos);
                    if (aggColumn == null) {
                        throw Util.newInternal(
                            "AggStar " + aggStar + " has no column for "
                            + column + " (bitPos " + bitPos + ")");
                    }
                    AggStar.Table table = aggColumn.getTable();
                    table.addToFrom(sqlQuery, false, true);
                    q = aggColumn.generateExprString(sqlQuery);
                } else {
                    RolapStar.Table targetTable = column.getTable();
                    hierarchy.addToFrom(sqlQuery, targetTable);
                    q = column.generateExprString(sqlQuery);
                }
            } else {
                assert (aggStar == null);
                hierarchy.addToFrom(sqlQuery, level.getKeyExp());
                q = level.getKeyExp().getExpression(sqlQuery);
            }

            StarColumnPredicate cc = getColumnPredicates(column, c);

            if (!dialect.supportsUnlimitedValueList()
                && cc instanceof ListColumnPredicate
                && ((ListColumnPredicate) cc).getPredicates().size()
                > maxConstraints)
            {
                // Simply get them all, do not create where-clause.
                // Below are two alternative approaches (and code). They
                // both have problems.
            } else {
                String where =
                    RolapStar.Column.createInExpr(
                        q, cc, level.getDatatype(), sqlQuery);
                if (!where.equals("true")) {
                    if (!firstLevel) {
                        if (exclude) {
                            condition += " or ";
                        } else {
                            condition += " and ";
                        }
                    } else {
                        firstLevel = false;
                    }
                    if (exclude) {
                        where = "not (" + where + ")";
                        if (!containsNullKey) {
                            // Null key fails all filters so should add it here
                            // if not already excluded.  E.g., if the original
                            // exclusion filter is :
                            //
                            // not(year = '1997' and quarter in ('Q1','Q3'))
                            //
                            // then with IS NULL checks added, the filter
                            // becomes:
                            //
                            // (not(year = '1997') or year is null) or
                            // (not(quarter in ('Q1','Q3')) or quarter is null)
                            where = "(" + where + " or " + "(" + q
                                + " is null))";
                        }
                    }
                    condition += where;
                }
            }

            if (m.getLevel().isUnique() || m.getLevel() == fromLevel) {
                break; // no further qualification needed
            }
        }

        return condition;
    }
}

// End SqlConstraintUtils.java
