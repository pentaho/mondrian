/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.Clause;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.SqlQueryBuilder;
import mondrian.spi.Dialect;
import mondrian.util.Pair;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Utility class used by implementations of {@link mondrian.rolap.sql.SqlConstraint},
 * used to generate constraints into {@link mondrian.rolap.sql.SqlQuery}.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class SqlConstraintUtils {

    private static final Pattern MULTIPLE_WHITESPACE_PATTERN =
        Pattern.compile("[\n ]+");

    /** Utility class */
    private SqlConstraintUtils() {
    }

    /**
     * For every restricting member in the current context, generates
     * a WHERE condition and a join to the fact table.
     *
     * @param queryBuilder Query builder
     * @param starSet Star set
     * @param restrictMemberTypes defines the behavior if the current context
     *   contains calculated members. If true, throws an exception.
     * @param evaluator Evaluator
     */
    public static void addContextConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet,
        RolapEvaluator evaluator,
        boolean restrictMemberTypes)
    {
        // Add constraint using the current evaluator context
        List<RolapMember> members = Arrays.asList(evaluator.getNonAllMembers());
        if (queryBuilder.fact != null) {
            setMeasureForCellRequest(members, queryBuilder.fact);
        }

        if (restrictMemberTypes) {
            if (containsCalculatedMember(members)) {
                throw Util.newInternal(
                    "can not restrict SQL to calculated Members");
            }
            if (hasMultiPositionSlicer(evaluator)) {
                throw Util.newInternal(
                    "can not restrict SQL to context with multi-position slicer");
            }
        } else {
            members = new ArrayList<RolapMember>(members);
            removeCalculatedAndDefaultMembers(members);
            removeMultiPositionSlicerMembers(members, evaluator);
        }

        // get the constrained columns and their values
        final CellRequest request =
            RolapAggregationManager.makeRequest(
                members.toArray(new RolapMember[members.size()]));
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

        if (starSet.getAggMeasureGroup() != null) {
            RolapGalaxy galaxy = evaluator.getCube().galaxy;
            @SuppressWarnings("unchecked")
            final AggregationManager.StarConverter starConverter =
                new BatchLoader.StarConverterImpl(
                    galaxy,
                    starSet.getStar(),
                    starSet.getAggMeasureGroup().getStar(),
                    Collections.EMPTY_MAP);
            columns = starConverter.convertColumnArray(columns);
        }

        // add constraints to where
        addColumnValueConstraints(queryBuilder, Pair.zip(columns, values));

        // add role constraints to where
        addRoleAccessConstraints(
            queryBuilder,
            starSet,
            restrictMemberTypes,
            evaluator);
    }

    /**
     * Replaces the measure in the members list (if present) with
     * the factCountMeasure associated with the RolapMeasureGroup.
     */
    private static void setMeasureForCellRequest(
        List<RolapMember> members, RolapMeasureGroup fact)
    {
        assert fact.factCountMeasure != null;
        for (int i = 0; i < members.size(); i++) {
            if (members.get(i).isMeasure()) {
                members.set(i, fact.factCountMeasure);
                break;
            }
        }
    }

    public static Map<Level, List<RolapMember>> getRoleConstraintMembers(
        SchemaReader schemaReader,
        Member[] members)
    {
        // LinkedHashMap keeps insert-order
        Map<Level, List<RolapMember>> roleMembers =
            new LinkedHashMap<Level, List<RolapMember>>();
        for (Member member : members) {
            if (member instanceof RolapHierarchy.LimitedRollupMember
                || member instanceof
                   RestrictedMemberReader.MultiCardinalityDefaultMember)
            {
                List<Level> hierarchyLevels = schemaReader
                        .getHierarchyLevels(member.getHierarchy());
                for (Level affectedLevel : hierarchyLevels) {
                    List<RolapMember> slicerMembers =
                        new ArrayList<RolapMember>();
                    boolean hasCustom = false;
                    List<Member> availableMembers =
                        schemaReader
                            .getLevelMembers(affectedLevel, false);
                    Role role = schemaReader.getRole();
                    for (Member availableMember : availableMembers) {
                        if (!availableMember.isAll()) {
                            slicerMembers.add((RolapMember) availableMember);
                        }
                        hasCustom |=
                            role.getAccess(availableMember) == Access.CUSTOM;
                    } // accessible members
                    if (!slicerMembers.isEmpty()) {
                        roleMembers.put(affectedLevel, slicerMembers);
                    }
                    if (!hasCustom) {
                        // we don't have partial access, no need going deeper
                        break;
                    }
                } // levels
            } // members
        }
        return roleMembers;
    }

    private static void addRoleAccessConstraints(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet,
        boolean restrictMemberTypes,
        Evaluator evaluator)
    {
        Map<Level, List<RolapMember>> roleMembers =
            getRoleConstraintMembers(
                evaluator.getSchemaReader(),
                evaluator.getMembers());
        for (Map.Entry<Level, List<RolapMember>> entry
            : roleMembers.entrySet())
        {
            StringBuilder where = new StringBuilder("(");
            generateSingleValueInExpr(
                where,
                queryBuilder,
                evaluator.getMeasureGroup(),
                starSet.getAggStar(),
                entry.getValue(),
                (RolapCubeLevel) entry.getKey(),
                restrictMemberTypes,
                false);

            if (where.length() > 1) {
                where.append(")");
                // The where clause might be null because if the
                // list of members is greater than the limit
                // permitted, we won't constrain.
                // add constraints
                queryBuilder.sqlQuery.addWhere(where.toString());
            }
        }
    }

    private static void addColumnValueConstraints(
        final SqlQueryBuilder queryBuilder,
        List<Pair<RolapStar.Column, Object>> columnValues)
    {
        // following code is similar to
        // AbstractQuerySpec#nonDistinctGenerateSQL()
        final StringBuilder buf = new StringBuilder();
        for (Pair<RolapStar.Column, Object> columnValue : columnValues) {
            final RolapStar.Column column = columnValue.left;
            String expr = column.getExpression().toSql();
            final String value = String.valueOf(columnValue.right);
            buf.setLength(0);
            if ((RolapUtil.mdxNullLiteral().equalsIgnoreCase(value))
                || (value.equalsIgnoreCase(RolapUtil.sqlNullValue.toString())))
            {
                buf.append(expr).append(" is null");
            } else {
                final Dialect dialect = queryBuilder.getDialect();
                try {
                    buf.append(expr)
                        .append(" = ");
                    dialect.quote(buf, value, column.getDatatype());
                } catch (NumberFormatException e) {
                    buf.setLength(0);
                    dialect.quoteBooleanLiteral(buf, false);
                }
            }
            queryBuilder.addColumn(
                queryBuilder.column(column.getExpression(), column.getTable()),
                Clause.FROM);
            queryBuilder.sqlQuery.addWhere(buf.toString());
        }
    }

    /**
     * Looks at the given <code>evaluator</code> to determine if it has more
     * than one slicer member from any particular hierarchy.
     *
     * @param evaluator the evaluator to look at
     * @return <code>true</code> if the evaluator's slicer has more than one
     *  member from any particular hierarchy
     */
    public static boolean hasMultiPositionSlicer(RolapEvaluator evaluator) {
        Map<Hierarchy, Member> mapOfSlicerMembers =
            new HashMap<Hierarchy, Member>();
        for (RolapMember slicerMember : evaluator.getSlicerMembers()) {
            Hierarchy hierarchy = slicerMember.getHierarchy();
            if (mapOfSlicerMembers.containsKey(hierarchy)) {
                // We have found a second member in this hierarchy
                return true;
            }
            mapOfSlicerMembers.put(hierarchy, slicerMember);
        }
        return false;
    }

    protected static void removeMultiPositionSlicerMembers(
        List<RolapMember> memberList,
        Evaluator evaluator)
    {
        List<RolapMember> slicerMembers = null;
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
            // Iterate the given list of members, removing any whose hierarchy
            // has multiple members on the slicer axis
            for (int i = 0; i < memberList.size(); i++) {
                RolapMember member = memberList.get(i);
                Hierarchy hierarchy = member.getHierarchy();
                if (mapOfSlicerMembers.containsKey(hierarchy)
                    && mapOfSlicerMembers.get(hierarchy).size() >= 2)
                {
                    memberList.remove(i--);
                }
            }
        }
    }

    /**
     * Removes calculated and default members from a list (except those that are
     * leaves in a parent-child hierarchy) and with members that are the
     * default member of their hierarchy.
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
     * @param memberList Array of members
     */
    private static void removeCalculatedAndDefaultMembers(
        List<RolapMember> memberList)
    {
        for (int i = 0; i < memberList.size(); i++) {
            RolapMember member = memberList.get(i);
            // Skip calculated members (except if leaf of parent-child hier)
            if (member.isCalculated() && !member.isParentChildLeaf()) {
                memberList.remove(i--);

            // Remove members that are the default for their hierarchy,
            // except for the measures hierarchy.
            } else if (i > 0
                && member.getHierarchy().getDefaultMember().equals(member))
            {
                memberList.remove(i--);
            }
        }
    }

    static List<Member> removeCalculatedMembers(List<Member> members) {
        return Util.copyWhere(
            members,
            new Util.Predicate1<Member>() {
                public boolean test(Member member) {
                    return !member.isCalculated() || member.isParentChildLeaf();
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

    public static boolean containsCalculatedMember(
        Iterable<? extends Member> members)
    {
        for (Member member : members) {
            if (member.isCalculated()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Ensures that the table of <code>level</code> is joined to the fact
     * table.
     *
     * @param sqlQuery sql query under construction
     * @param starSet Star set
     * @param e evaluator corresponding to query
     * @param level level to be added to query
     */
    public static void joinLevelTableToFactTable(
        SqlQuery sqlQuery,
        RolapStarSet starSet,
        Evaluator e,
        RolapCubeLevel level)
    {
        Util.deprecated(false, false); // REMOVE method
/*
        AggStar aggStar = starSet.getAggStar();
        TODO: fix this code later. In the attribute-oriented world, we would
        go about joining to fact table differently.

        for (RolapStar star : starSet.getStars()) {
            if (aggStar != null) {
                RolapStar.Column starColumn = level.getBaseStarKeyColumn(star);
                int bitPos = starColumn.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                AggStar.Table table = aggColumn.getTable();
                table.addToFrom(sqlQuery, false, true);
            } else {
                level.getRolapLevel().getAttribute().keyList.addToFrom(
                    sqlQuery, star);
//                RolapStar.Table table = starColumn.getTable();
//                assert table != null;
//                table.addToFrom(sqlQuery, false, true);
            }
        }
*/
    }

    /**
     * Creates a "WHERE parent = value" constraint.
     *
     * @param queryBuilder Query builder
     * @param starSet Star set
     * @param parent the list of parent members
     * @param restrictMemberTypes defines the behavior if {@code parent}
     *                            contains calculated members. If true, and one
     *                            of the members is calculated, throws.
     */
    public static void addMemberConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet,
        RolapMember parent,
        boolean restrictMemberTypes)
    {
        List<RolapMember> list = Collections.singletonList(parent);
        boolean exclude = false;
        addMemberConstraint(
            queryBuilder, starSet, list, restrictMemberTypes, false, exclude);
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
     * @param queryBuilder Query builder
     * @param starSet Star set
     * @param members the list of members for this constraint
     * @param restrictMemberTypes defines the behavior if {@code parent}
     *                            contains calculated members. If true, and one
     *                            of the members is calculated, throws.
     * @param crossJoin true if constraint is being generated as part of
     * @param exclude whether to exclude the members in the SQL predicate.
     */
    public static void addMemberConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet,
        List<RolapMember> members,
        boolean restrictMemberTypes,
        boolean crossJoin,
        boolean exclude)
    {
        assert starSet != null;
        if (members.size() == 0) {
            // Generate a predicate which is always false
            // (or, if exclude, always true) in order to produce
            // the empty set.  It would be smarter to avoid executing SQL at
            // all in this case, but doing it this way avoid special-case
            // evaluation code.
            final StringBuilder buf = new StringBuilder();
            queryBuilder.sqlQuery.getDialect()
                .quoteBooleanLiteral(buf, exclude);
            queryBuilder.sqlQuery.addWhere(buf.toString());
            return;
        }

        // Find out the first(lowest) unique parent level.
        // Only need to compare members up to that level.
        RolapMember member = members.get(0);
        RolapMember firstUniqueParent = member;
        RolapCubeLevel firstUniqueParentLevel = null;
        for (;
            firstUniqueParent != null
            && true /* TODO: !firstUniqueParent.getLevel().isUnique() */;
            firstUniqueParent = firstUniqueParent.getParentMember())
        {
        }

        if (firstUniqueParent != null) {
            // There's a unique parent along the hierarchy
            firstUniqueParentLevel = firstUniqueParent.getLevel();
        }

        StringBuilder condition = new StringBuilder("(");

        // If this constraint is part of a native cross join and there
        // are multiple values for the parent members, then we can't
        // use single value IN clauses
        boolean added = false;
        if (crossJoin
            && true /* TODO: !memberLevel.isUnique() */
            && !membersAreCrossProduct(members))
        {
            assert (member.getParentMember() != null);
            added =
                constrainMultiLevelMembers(
                    queryBuilder,
                    condition,
                    starSet.getMeasureGroup(),
                    starSet.getAggStar(),
                    members,
                    firstUniqueParentLevel,
                    restrictMemberTypes,
                    exclude);
        } else {
            generateSingleValueInExpr(
                condition,
                queryBuilder,
                starSet.getMeasureGroup(),
                starSet.getAggStar(),
                members,
                firstUniqueParentLevel,
                restrictMemberTypes,
                exclude);
            added = condition.length() > 1;
        }

        if (added) {
            // condition is not empty
            condition.append(")");
            queryBuilder.sqlQuery.addWhere(condition.toString());
        }
    }

    private static StarPredicate getColumnPredicates(
        RolapMeasureGroup measureGroup,
        RolapSchema.PhysSchema physSchema,
        Collection<RolapMember> members)
    {
        int i = members.size();
        if (i == 0) {
            return LiteralStarPredicate.FALSE;
        }
        RolapMember member1 = members.iterator().next();
        final RolapCubeLevel level = member1.getLevel();
        RolapSchema.PhysRouter router =
            new RolapSchema.CubeRouter(measureGroup, level.getDimension());
        if (i == 1) {
            return Predicates.memberPredicate(
                router,
                member1);
        }
        return Predicates.list(
            physSchema,
            router,
            level,
            new ArrayList<RolapMember>(members));
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
     * list of members.
     *
     *
     * @param queryBuilder query containing the where clause
     * @param measureGroup Measure group
     * @param aggStar aggregate star if available
     * @param members list of constraining members
     * @param fromLevel lowest parent level that is unique
     * @param restrictMemberTypes defines the behavior when calculated members
     * are present
     * @param exclude whether to exclude the members. Default is false.
     * @return Whether anything was appended to {@code condition}.
     */
    private static boolean constrainMultiLevelMembers(
        SqlQueryBuilder queryBuilder,
        StringBuilder condition,
        RolapMeasureGroup measureGroup,
        AggStar aggStar,
        List<RolapMember> members,
        RolapCubeLevel fromLevel,
        boolean restrictMemberTypes,
        boolean exclude)
    {
        final int initial = condition.length();

        // TODO: use fromLevel.

        // Use LinkedHashMap so that keySet() is deterministic.
        Map<RolapMember, List<RolapMember>> parentChildrenMap =
            new LinkedHashMap<RolapMember, List<RolapMember>>();
        StringBuilder condition1 = new StringBuilder();
        if (exclude) {
            condition.append("not (");
        }

        // First try to generate IN list for all members
        if (queryBuilder.getDialect().supportsMultiValueInExpr()) {
            condition1.append(
                generateMultiValueInExpr(
                    queryBuilder,
                    measureGroup,
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
            // These members are organized in a map that maps the parent levels
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
                    condition.append(strip(")\n    or "));
                    generateMultiValueIsNullExprs(
                        condition,
                        members.get(0),
                        fromLevel);
                }
                return condition.length() > initial;
            }
        } else {
            // Multi-value IN list not supported
            // Classify members into List that share the same parent.
            //
            // Using the same example as above, the resulting map will be
            //   [USA].[CA] -> [San Jose]
            //   [null].[null] -> ([San Francisco], [Los Angeles])
            //   [null].[CA] -> ([San Diego],[Sacramento])
            //
            // The idea is to be able to "compress" the original member list
            // into groups that can use single value IN list for part of the
            // comparison that does not involve NULLs
            //
            for (RolapMember m : members) {
                if (m.isCalculated()) {
                    if (restrictMemberTypes) {
                        throw Util.newInternal(
                            "addMemberConstraint: cannot restrict SQL to "
                            + "calculated member :" + m);
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
        StringBuilder condition2 = new StringBuilder();

        if (condition1.length() > 0) {
            // Some members have already been translated into IN list.
            condition.append(condition1.toString());
            condition.append(strip("\n    or "));
        }

        RolapCubeLevel memberLevel = members.get(0).getLevel();

        // The children for each parent are turned into IN list so they
        // should not contain null.
        for (RolapMember p : parentChildrenMap.keySet()) {
            assert p != null;
            if (condition2.length() > 0) {
                condition2.append(strip("\n    or "));
            }

            condition2.append("(");

            // First generate ANDs for all members in the parent lineage of
            // this parent-children group
            int levelCount = 0;

            // this method can be called within the context of shared
            // members, outside of the normal rolap star, therefore
            // we need to check the level to see if it is a shared or
            // cube level.
            if (Util.deprecated(true, false)) {
                final RolapAttribute attribute = p.getLevel().getAttribute();
                for (Pair<RolapSchema.PhysColumn, Comparable> pair
                    : Pair.iterate(
                        attribute.getKeyList(), p.getKeyAsList()))
                {
                    final RolapSchema.PhysColumn physColumn = pair.left;
                    final Comparable o = pair.right;
                    final RolapStar.Column column =
                        measureGroup.getRolapStarColumn(
                            p.getDimension(), physColumn);
                    if (column != null) {
                        if (aggStar != null) {
                            int bitPos = column.getBitPosition();
                            AggStar.Table.Column aggColumn =
                                aggStar.lookupColumn(bitPos);
                            AggStar.Table table = aggColumn.getTable();
                            table.addToFrom(
                                queryBuilder.sqlQuery,
                                false,
                                true);
                        } else {
                            queryBuilder.addColumn(
                                queryBuilder.column(
                                    physColumn, p.getDimension()),
                                Clause.FROM);
                        }
                    } else {
                        assert aggStar == null;
                        Util.deprecated("todo", false);
                        // level.getKeyExp().addToFrom(sqlQuery, star);
                    }

                    if (levelCount++ > 0) {
                        condition2.append(strip("\n    and "));
                    }

                    Util.deprecated("obsolete", false);
                    condition2.append(
                        constrainLevel(
                            physColumn,
                            queryBuilder.getDialect(),
                            getColumnValue(
                                o,
                                queryBuilder.getDialect(),
                                physColumn.getDatatype()),
                            false));

                    // TODO:
//                    if (gp.getLevel() == fromLevel) {
//                        SQL is completely generated for this parent
//                        break;
//                    }
                }
            }

            // Next, generate children for this parent-children group
            List<RolapMember> children = parentChildrenMap.get(p);

            // If no children to be generated for this parent then we are done
            if (!children.isEmpty()) {
                Map<RolapMember, List<RolapMember>> tmpParentChildrenMap =
                    new HashMap<RolapMember, List<RolapMember>>();

                if (levelCount > 0) {
                    condition2.append(strip("\n    and "));
                }
                RolapCubeLevel childrenLevel = p.getLevel().getChildLevel();

                if (queryBuilder.getDialect().supportsMultiValueInExpr()
                    && childrenLevel != memberLevel)
                {
                    // Multi-level children and multi-value IN list supported
                    condition2.append(
                        generateMultiValueInExpr(
                            queryBuilder,
                            measureGroup,
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
                    generateSingleValueInExpr(
                        condition2,
                        queryBuilder,
                        measureGroup,
                        aggStar,
                        children,
                        childrenLevel,
                        restrictMemberTypes,
                        false);
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
            condition.append(strip(")\n       or ("));
            generateMultiValueIsNullExprs(
                condition,
                members.get(0),
                fromLevel);
            condition.append(" and not(");
            condition.append(condition2.toString());
            condition.append("))");
        }

        return condition.length() > initial;
    }

    /**
     * @param members list of members
     *
     * @return true if the members comprise the cross product of all unique
     * member keys referenced at each level
     */
    public static boolean membersAreCrossProduct(List<RolapMember> members)
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
        Comparable key,
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
     * Generates a SQL expression constraining a level's key by some value.
     *
     *
     * @param exp Column to constrain
     * @param dialect SQL dialect
     * @param columnValue value constraining the level
     * @param caseSensitive if true, need to handle case sensitivity of the
     *                      member value
     * @return generated string corresponding to the expression
     */
    public static String constrainLevel(
        RolapSchema.PhysColumn exp,
        Dialect dialect,
        String columnValue,
        boolean caseSensitive)
    {
        // this method can be called within the context of shared members,
        // outside of the normal rolap star, therefore we need to
        // check the level to see if it is a shared or cube level.

        Util.deprecated(
            "TODO unify inside-star and outside-star code paths",
            false);
        Dialect.Datatype datatype = exp.getDatatype();
        String columnString = exp.toSql();

        String constraint;
        if (RolapUtil.mdxNullLiteral().equalsIgnoreCase(columnValue)) {
            constraint = columnString + " is null";
        } else {
            if (datatype.isNumeric()) {
                // A numeric data type deserves a numeric value.
                try {
                    Double.valueOf(columnValue);
                } catch (NumberFormatException e) {
                    // Trying to equate a numeric column to a non-numeric value,
                    // for example
                    //
                    //    WHERE int_column = 'Foo Bar'
                    //
                    // It's clearly impossible to match, so convert condition
                    // to FALSE. We used to play games like
                    //
                    //   WHERE Upper(int_column) = Upper('Foo Bar')
                    //
                    // but Postgres in particular didn't like that. And who can
                    // blame it.
                    return RolapUtil.SQL_FALSE_LITERAL;
                }
            }
            final StringBuilder buf = new StringBuilder();
            dialect.quote(buf, columnValue, datatype);
            String value = buf.toString();
            if (caseSensitive && datatype == Dialect.Datatype.String) {
                // Some databases (like DB2) compare case-sensitive. We convert
                // the value to upper-case in the DBMS (e.g. UPPER('Foo'))
                // rather than in Java (e.g. 'FOO') in case the DBMS is running
                // a different locale.
                if (!MondrianProperties.instance().CaseSensitive.get()) {
                    columnString = dialect.toUpper(columnString);
                    value = dialect.toUpper(value);
                }
            }

            constraint = columnString + " = " + value;
        }

        return constraint;
    }

    /**
     * Generates a SQL expression constraining a level by some value,
     * and appends it to the WHERE clause. If the value is invalid for its
     * data type, appends 'WHERE FALSE'.
     *
     * @param exp Key expression
     * @param dimension Dimension
     * @param queryBuilder Query that the sql expression will be added to
     * @param columnValue value constraining the level
     */
    public static void constrainLevel2(
        SqlQueryBuilder queryBuilder,
        RolapSchema.PhysColumn exp,
        RolapCubeDimension dimension,
        Comparable columnValue)
    {
        final StringBuilder buf = new StringBuilder(exp.toSql());
        if (columnValue == RolapUtil.sqlNullValue) {
            buf.append(" is null");
        } else {
            try {
                buf.append(" = ");
                queryBuilder.getDialect()
                    .quote(buf, columnValue, exp.getDatatype());
            } catch (NumberFormatException e) {
                buf.setLength(0);
                queryBuilder.getDialect().quoteBooleanLiteral(buf, false);
            }
        }
        queryBuilder.addColumn(
            queryBuilder.column(exp, dimension), Clause.FROM);
        queryBuilder.sqlQuery.addWhere(buf.toString());
    }

    /**
     * Generates a multi-value IN expression corresponding to a list of
     * member expressions, and adds the expression to the WHERE clause
     * of a query, provided the member values are all non-null.
     *
     * @param queryBuilder query containing the where clause
     * @param measureGroup Measure group
     * @param aggStar aggregate star if available
     * @param members list of constraining members
     * @param fromLevel lowest parent level that is unique
     * @param restrictMemberTypes defines the behavior when calculated members
     *        are present
     * @param parentWithNullToChildrenMap upon return this map contains members
     *        that have Null values in its (parent) levels
     *  @return a non-empty String if multi-value IN list was generated for some
     *        members
     */
    private static String generateMultiValueInExpr(
        SqlQueryBuilder queryBuilder,
        RolapMeasureGroup measureGroup,
        AggStar aggStar,
        List<RolapMember> members,
        RolapCubeLevel fromLevel,
        boolean restrictMemberTypes,
        Map<RolapMember, List<RolapMember>> parentWithNullToChildrenMap)
    {
        final StringBuilder columnBuf = new StringBuilder();
        final StringBuilder valueBuf = new StringBuilder();
        final StringBuilder memberBuf = new StringBuilder();

        columnBuf.append("(");

        // generate the left-hand side of the IN expression
        final RolapMember member = members.get(0);
        final RolapCubeLevel level = member.getLevel();

        // this method can be called within the context of shared members,
        // outside of the normal rolap star, therefore we need to
        // check the level to see if it is a shared or cube level.

        final List<RolapStar.Column> columns =
            new ArrayList<RolapStar.Column>();
        for (RolapSchema.PhysColumn key : level.attribute.getKeyList()) {
            columns.add(
                measureGroup.getRolapStarColumn(
                    level.cubeDimension,
                    key));
        }

        // REVIEW: The following code mostly uses the name column (or name
        // expression) of the level. Shouldn't it use the key column (or key
        // expression)?
        final List<String> columnList = new ArrayList<String>();
        if (columns != null) {
            if (aggStar != null) {
                // this assumes that the name column is identical to the
                // id column
                for (RolapStar.Column column : columns) {
                    int bitPos = column.getBitPosition();
                    AggStar.Table.Column aggColumn =
                        aggStar.lookupColumn(bitPos);
                    AggStar.Table table = aggColumn.getTable();
                    table.addToFrom(queryBuilder.sqlQuery, false, true);
                    columnList.add(aggColumn.getExpression().toSql());
                }
            } else {
                for (RolapStar.Column column : columns) {
                    queryBuilder.addColumn(
                        queryBuilder.column(
                            column.getExpression(), column.getTable()),
                        Clause.FROM);
                    columnList.add(column.getExpression().toSql());
                }
            }
        } else {
            assert aggStar == null;
            RolapSchema.PhysExpr nameExp = level.getAttribute().getNameExp();
            columnList.add(nameExp.toSql());
        }

        columnBuf.append(Util.commaList(columnList));
        columnBuf.append(")");

        // generate the RHS of the IN predicate
        valueBuf.append("(");
        int memberOrdinal = 0;
        for (RolapMember m : members) {
            if (m.isCalculated()) {
                if (restrictMemberTypes) {
                    throw Util.newInternal(
                        "addMemberConstraint: cannot restrict SQL to "
                        + "calculated member :" + m);
                }
                continue;
            }

            memberBuf.setLength(0);
            memberBuf.append("(");

            boolean containsNull = false;
            int ordinalInMultiple = 0;
            for (Pair<RolapSchema.PhysColumn, Comparable> pair
                : Pair.iterate(level.attribute.getKeyList(), m.getKeyAsList()))
            {
                final Comparable o = pair.right;
                final Dialect.Datatype datatype = pair.left.getDatatype();
                String value =
                    getColumnValue(
                        o,
                        queryBuilder.getDialect(),
                        datatype);

                // If parent at a level is NULL, record this parent and all
                // its children (if there are any)
                if (RolapUtil.mdxNullLiteral().equalsIgnoreCase(value)) {
                    // Add to the nullParent map
                    List<RolapMember> childrenList =
                        parentWithNullToChildrenMap.get(m);
                    if (childrenList == null) {
                        childrenList = new ArrayList<RolapMember>();
                        parentWithNullToChildrenMap.put(m, childrenList);
                    }

                    // Skip generating condition for this parent
                    containsNull = true;
                    break;
                }

                if (ordinalInMultiple++ > 0) {
                    memberBuf.append(", ");
                }

                queryBuilder.getDialect().quote(
                    memberBuf, value, datatype);
            }

            // Now check if SQL string is successfully generated for this
            // member.  If parent levels do not contain NULL then SQL must
            // have been generated successfully.
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
     * @param buf Buffer to which to append condition
     * @param member the RolapMember
     * @param fromLevel lowest parent level that is unique
     */
    private static void generateMultiValueIsNullExprs(
        StringBuilder buf,
        RolapMember member,
        RolapLevel fromLevel)
    {
        buf.append("(");

        // generate the left-hand side of the IN expression
        int levelInMultiple = 0;
        for (RolapMember m = member; m != null; m = m.getParentMember()) {
            if (m.isAll()) {
                continue;
            }

            if (levelInMultiple++ > 0) {
                buf.append(strip("\n        or "));
            }
            // REVIEW.  Will this mishandle aggregate table queries?
            // SqlTupleReader does not generate agg table queries
            // (MONDRIAN-1372), but will once that is fixed and this
            // may generate incorrect sql.
            // 1a62586962a854bfec36e8c2d47b5f1fce98d4d6 addressed an
            // agg table issue with this method in 3x
            buf.append(m.getLevel().getAttribute().getNameExp().toSql())
                .append(" is null");

            // Only needs to compare up to the first(lowest) unique level.
            if (m.getLevel() == fromLevel) {
                break;
            }
        }

        buf.append(")");
    }

    /**
     * Generates a multi-value IN expression corresponding to a list of
     * member expressions, and adds the expression to the WHERE clause
     * of a query, provided that the member values are all non-null.
     *
     * <p>{@link Util#deprecated(Object, boolean)} different versions of
     * this method for RolapMember and RolapCubeMember? The latter with
     * a RolapMeasureGroup, former without.
     *
     * @param buf Buffer into which to generate condition
     * @param queryBuilder query containing the where clause
     * @param measureGroup Measure group to semijoin to; or null
     * @param aggStar aggregate star if available
     * @param members list of constraining members
     * @param fromLevel lowest parent level that is unique
     * @param restrictMemberTypes defines the behavior when calculated members
     *        are present
     * @param exclude whether to exclude the members. Default is false.
     */
    private static void generateSingleValueInExpr(
        StringBuilder buf,
        SqlQueryBuilder queryBuilder,
        RolapMeasureGroup measureGroup,
        AggStar aggStar,
        List<RolapMember> members,
        RolapCubeLevel fromLevel,
        boolean restrictMemberTypes,
        boolean exclude)
    {
        final int maxConstraints =
            MondrianProperties.instance().MaxConstraints.get();
        final Dialect dialect = queryBuilder.getDialect();

        int levelCount = 0;
        Collection<RolapMember> members2 = members;
        RolapMember m = null;
        for (;
            !members2.isEmpty();
            members2 = getUniqueParentMembers(members2))
        {
            m = members2.iterator().next();
            if (m.isAll()) {
                continue;
            }
            if (m.isNull()) {
                dialect.quoteBooleanLiteral(buf, false);
                return;
            }
            if (m.isCalculated() && !m.isParentChildLeaf()) {
                if (restrictMemberTypes) {
                    throw Util.newInternal(
                        "addMemberConstraint: cannot restrict SQL to "
                        + "calculated member :" + m);
                }
                continue;
            }
            break;
        }

        boolean containsNullKey = false;
        for (RolapMember member : members2) {
            m = member;
            if (m.getKey() == RolapUtil.sqlNullValue) {
                containsNullKey = true;
            }
        }

        final RolapCubeLevel level = m.getLevel();
        for (RolapSchema.PhysColumn key : level.getAttribute().getKeyList()) {
            // this method can be called within the context of shared
            // members, outside of the normal rolap star, therefore we need
            // to check the level to see if it is a shared or cube level.

            String q;
            if (measureGroup != null) {
                final RolapStar.Column column =
                    measureGroup.getRolapStarColumn(
                        level.cubeDimension, key, true);

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
                    table.addToFrom(queryBuilder.sqlQuery, false, true);
                    q = aggColumn.getExpression().toSql();
                } else {
                    queryBuilder.addColumn(
                        queryBuilder.column(
                            column.getExpression(), column.getTable()),
                        Clause.FROM);
                    q = column.getExpression().toSql();
                }
            } else {
                assert aggStar == null;
                queryBuilder.addRelation(
                    queryBuilder.table(key.relation, level.cubeDimension),
                    SqlQueryBuilder.NullJoiner.INSTANCE);
                q = key.toSql();
            }

            StarPredicate cc =
                getColumnPredicates(
                    measureGroup,
                    level.getAttribute().getKeyList().get(0).relation
                        .getSchema(),
                    members2);

            if (!dialect.supportsUnlimitedValueList()
                && cc instanceof ListColumnPredicate
                && ((ListColumnPredicate) cc).getPredicates().size()
                > maxConstraints)
            {
                // Simply get them all, do not create where-clause.
                // Below are two alternative approaches (and code). They
                // both have problems.
            } else {
                Util.deprecated("obsolete", false);
                String where = Predicates.toSql(cc, dialect);
                if (!where.equals("true")) {
                    if (levelCount++ > 0) {
                        buf.append(
                            strip(exclude ? "\n    or " : "\n    and "));
                    }
                    if (exclude) {
                        where = "not (" + where + ")";
                        if (!containsNullKey) {
                            // Null key fails all filters so should add it
                            // here if not already excluded. For example, if
                            // the original exclusion filter is
                            //
                            // not(year = '1997' and quarter in ('Q1','Q3'))
                            //
                            // then with IS NULL checks added, the filter
                            // becomes:
                            //
                            // (not (year = '1997')
                            //    or year is null)
                            // or
                            // (not (quarter in ('Q1','Q3'))
                            //    or quarter is null)
                            where = "(" + where + strip("\n        or (")
                                + q + " is null))";
                        }
                    }
                    buf.append(where);
                }
            }
            if (!exclude) {
                break;
            }
        }
    }

    /**
     * Converts multiple whitespace and linefeeds to a single space, if
     * {@link MondrianProperties#GenerateFormattedSql} is false. This method
     * is a convenient way to generate the right amount of formatting with one
     * call.
     */
    private static String strip(String s) {
        if (!MondrianProperties.instance().GenerateFormattedSql.get()) {
            s = MULTIPLE_WHITESPACE_PATTERN.matcher(s).replaceAll(" ");
        }
        return s;
    }
}

// End SqlConstraintUtils.java
