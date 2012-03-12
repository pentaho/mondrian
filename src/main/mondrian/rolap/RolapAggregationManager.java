/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2012 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.VisualTotalsFunDef.VisualTotalMember;
import mondrian.rolap.agg.*;
import mondrian.util.Pair;

import java.util.*;

/**
 * <code>RolapAggregationManager</code> manages all
 * {@link mondrian.rolap.agg.Segment}s in the system.
 *
 * <p> The bits of the implementation which depend upon dimensional concepts
 * <code>RolapMember</code>, etc.) live in this class, and the other bits live
 * in the derived class, {@link mondrian.rolap.agg.AggregationManager}.
 *
 * @author jhyde
 * @since 30 August, 2001
 * @version $Id$
 */
public abstract class RolapAggregationManager {

    /**
     * Creates the RolapAggregationManager.
     */
    protected RolapAggregationManager() {
    }

    /**
     * Creates a request to evaluate the cell identified by
     * <code>members</code>.
     *
     * <p>If any of the members is the null member, returns
     * null, since there is no cell. If the measure is calculated, returns
     * null.
     *
     * @param members Set of members which constrain the cell
     * @return Cell request, or null if the requst is unsatisfiable
     */
    public static CellRequest makeRequest(final Member[] members)
    {
        return makeCellRequest(members, false, false, null, null);
    }

    /**
     * Creates a request for the fact-table rows underlying the cell identified
     * by <code>members</code>.
     *
     * <p>If any of the members is the null member, returns null, since there
     * is no cell. If the measure is calculated, returns null.
     *
     * @param members           Set of members which constrain the cell
     *
     * @param extendedContext   If true, add non-constraining columns to the
     *                          query for levels below each current member.
     *                          This additional context makes the drill-through
     *                          queries easier for humans to understand.
     *
     * @param cube              Cube
     * @return Cell request, or null if the requst is unsatisfiable
     */
    public static DrillThroughCellRequest makeDrillThroughRequest(
        final Member[] members,
        final boolean extendedContext,
        RolapCube cube,
        List<Exp> fieldsList)
    {
        assert cube != null;
        return (DrillThroughCellRequest) makeCellRequest(
            members, true, extendedContext, cube, fieldsList);
    }

    /**
     * Creates a request to evaluate the cell identified by the context
     * specified in <code>evaluator</code>.
     *
     * <p>If any of the members from the context is the null member, returns
     * null, since there is no cell. If the measure is calculated, returns
     * null.
     *
     * @param evaluator the cell specified by the evaluator context
     * @return Cell request, or null if the requst is unsatisfiable
     */
    public static CellRequest makeRequest(
        RolapEvaluator evaluator)
    {
        final Member[] currentMembers = evaluator.getNonAllMembers();
        final List<List<List<Member>>> aggregationLists =
            evaluator.getAggregationLists();

        final RolapStoredMeasure measure =
            (RolapStoredMeasure) currentMembers[0];
        final RolapStar.Measure starMeasure = measure.getStarMeasure();
        assert starMeasure != null;
        final RolapStar star = starMeasure.getStar();
        int starColumnCount = star.getColumnCount();

        CellRequest request =
            makeCellRequest(currentMembers, false, false, null, null);

        // Now setting the compound keys.
        // First find out the columns referenced in the aggregateMemberList.
        // Each list defines a compound member.
        if (aggregationLists == null) {
            return request;
        }

        // For each aggregationList, generate the optimal form of
        // compoundPredicate. These compoundPredicates are AND'ed together when
        // sql is generated for them.
        for (List<List<Member>> aggregationList : aggregationLists) {
            BitKey compoundBitKey = BitKey.Factory.makeBitKey(starColumnCount);
            Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap =
                new LinkedHashMap<BitKey, List<RolapCubeMember[]>>();

            // Go through the compound members/tuples once and separate them
            // into groups.
            List<List<RolapMember>> rolapAggregationList =
                new ArrayList<List<RolapMember>>();
            for (List<Member> members : aggregationList) {
                // REVIEW: do we need to copy?
                List<RolapMember> rolapMembers = Util.cast(members);
                rolapAggregationList.add(rolapMembers);
            }

            final RolapMeasureGroup measureGroup = measure.getMeasureGroup();
            boolean unsatisfiable =
                makeCompoundGroup(
                    starColumnCount,
                    measureGroup,
                    rolapAggregationList,
                    compoundGroupMap);

            if (unsatisfiable) {
                return null;
            }
            StarPredicate compoundPredicate =
                makeCompoundPredicate(
                    compoundGroupMap, measureGroup);

            if (compoundPredicate != null) {
                // Only add the compound constraint when it is not empty.
                for (BitKey bitKey : compoundGroupMap.keySet()) {
                    compoundBitKey = compoundBitKey.or(bitKey);
                }
                request.addAggregateList(compoundBitKey, compoundPredicate);
            }
        }

        return request;
    }

    private static CellRequest makeCellRequest(
        final Member[] members,
        boolean drillThrough,
        final boolean extendedContext,
        RolapCube cube,
        List<Exp> fieldsList)
    {
        // Need cube for drill-through requests
        assert drillThrough == (cube != null);

        if (extendedContext) {
            assert drillThrough;
        }

        final RolapStoredMeasure measure;
        if (drillThrough) {
            cube = RolapCell.chooseDrillThroughCube(members, cube);
            if (cube == null) {
                return null;
            }
            if (members.length > 0
                && members[0] instanceof RolapStoredMeasure)
            {
                measure = (RolapStoredMeasure) members[0];
            } else {
                measure = (RolapStoredMeasure) cube.getMeasures().get(0);
            }
        } else {
            if (members.length > 0
                && members[0] instanceof RolapStoredMeasure)
            {
                measure = (RolapStoredMeasure) members[0];
            } else {
                return null;
            }
        }

        final RolapMeasureGroup measureGroup = measure.getMeasureGroup();
        final RolapStar.Measure starMeasure = measure.getStarMeasure();
        assert starMeasure != null;
        final CellRequest request;
        if (drillThrough) {
            request =
                new DrillThroughCellRequest(starMeasure, extendedContext);
        } else {
            request =
                new CellRequest(starMeasure, extendedContext, drillThrough);
        }

        // Since 'if (extendedContext)' is a well-worn code path,
        // we have moved the test outside the loop.
        if (extendedContext) {
            if (fieldsList != null) {
                // If a field list was specified, there will be some columns
                // to include in the result set, other that we don't. This
                // happens when the MDX is a DRILLTHROUGH operation and
                // includes a RETURN clause.
                final SchemaReader reader = cube.getSchemaReader().withLocus();
                for (Exp exp : fieldsList) {
                    final RolapCubeMember member = (RolapCubeMember)
                        reader.lookupCompound(
                            cube,
                            Util.parseIdentifier(exp.toString()),
                            true,
                            Category.Unknown);
                    if (member.getHierarchy().getRolapHierarchy().closureFor
                        != null)
                    {
                        continue;
                    }
                    addNonConstrainingColumns(member, measureGroup, request);
                }
            }
            for (int i = 1; i < members.length; i++) {
                final RolapCubeMember member = (RolapCubeMember) members[i];
                if (member.getHierarchy().getRolapHierarchy().closureFor
                    != null)
                {
                    continue;
                }
                addNonConstrainingColumns(member, measureGroup, request);

                final RolapCubeLevel level = member.getLevel();
                final boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, measureGroup, request);
                if (needToReturnNull) {
                    return null;
                }
            }
        } else {
            for (int i = 1; i < members.length; i++) {
                if (!(members[i] instanceof RolapCubeMember)) {
                    Util.deprecated("no longer occurs?", true);
                    continue;
                }
                RolapCubeMember member = (RolapCubeMember) members[i];
                final RolapCubeLevel level = member.getLevel();
                final boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, measureGroup, request);
                if (needToReturnNull) {
                    return null;
                }
            }
        }
        return request;
    }

    /**
     * Adds the key columns as non-constraining columns. For
     * example, if they asked for [Gender].[M], [Store].[USA].[CA]
     * then the following levels are in play:<ul>
     *   <li>Gender = 'M'
     *   <li>Marital Status not constraining
     *   <li>Nation = 'USA'
     *   <li>State = 'CA'
     *   <li>City not constraining
     * </ul>
     *
     * <p>Note that [Marital Status] column is present by virtue of
     * the implicit [Marital Status].[All] member. Hence the SQL
     *
     *   <blockquote><pre>
     *   select [Marital Status], [City]
     *   from [Star]
     *   where [Gender] = 'M'
     *   and [Nation] = 'USA'
     *   and [State] = 'CA'
     *   </pre></blockquote>
     *
     * @param member Member to constraint
     * @param measureGroup Measure group
     * @param request Cell request
     */
    private static void addNonConstrainingColumns(
        final RolapCubeMember member,
        final RolapMeasureGroup measureGroup,
        final CellRequest request)
    {
        final RolapAttribute level1 = member.getLevel().getAttribute();
        for (RolapSchema.PhysColumn column : level1.keyList) {
            RolapStar.Column starColumn =
                measureGroup.getRolapStarColumn(
                    member.getDimension(),
                    column);
            if (starColumn != null) {
                request.addConstrainedColumn(starColumn, null);
            }
        }
        if (request.extendedContext
            && level1.nameExp != null)
        {
            RolapStar.Column starColumn =
                measureGroup.getRolapStarColumn(
                    member.getDimension(),
                    level1.nameExp);
            // REVIEW: Is it valid to assume that the level has a join path to
            //     the measure group? If it has no join path, will we notice
            //     that getRolapStarColumn returns null for the key cols?
            //     Should we curry the map: mGroup.map1(dimension).map2(expr)?
            //     map1 could be empty or null if dimension is not linked.
            assert starColumn != null;
            request.addConstrainedColumn(starColumn, null);
        }
    }

    /*
    private static void addNonConstrainingColumns(
        final OlapElement member,
        final RolapCube baseCube,
        final CellRequest request)
    {
        RolapCubeLevel level;
        if (member instanceof RolapCubeLevel) {
            level = (RolapCubeLevel) member;
        } else if (member instanceof RolapCubeHierarchy
            || member instanceof RolapCubeDimension)
        {
            level = (RolapCubeLevel) member.getHierarchy().getLevels()[0];
            if (level.isAll()) {
                level = level.getChildLevel();
            }
        } else if (member instanceof RolapStar.Measure) {
            ((DrillThroughCellRequest)request)
                .addDrillThroughMeasure((RolapStar.Measure)member);
            return;
        } else if (member instanceof RolapBaseCubeMeasure) {
            ((DrillThroughCellRequest)request)
                .addDrillThroughMeasure(
                    (RolapStar.Measure)
                        ((RolapBaseCubeMeasure)member).getStarMeasure());
            return;
        } else {
            // FIXME make this better.
            throw new MondrianException();
        }
        RolapStar.Column column = level.getBaseStarKeyColumn(mGbaseCube);
        if (column != null) {
            request.addConstrainedColumn(column, null);
            ((DrillThroughCellRequest)request).addDrillThroughColumn(column);
            if (request.extendedContext
                && level.getNameExp() != null)
            {
                final RolapStar.Column nameColumn = column.getNameColumn();
                Util.assertTrue(nameColumn != null);
                request.addConstrainedColumn(nameColumn, null);
            }
        }
    }
    */

    /**
     * Groups members (or tuples) from the same compound (i.e. hierarchy) into
     * groups that are constrained by the same set of columns.
     *
     * <p>For example:</p>
     *
     * <pre>
     * Members
     *     [USA].[CA],
     *     [Canada].[BC],
     *     [USA].[CA].[San Francisco],
     *     [USA].[OR].[Portland]
     * </pre>
     *
     * will be grouped into
     *
     * <pre>
     * Group 1:
     *     {[USA].[CA], [Canada].[BC]}
     * Group 2:
     *     {[USA].[CA].[San Francisco], [USA].[OR].[Portland]}
     * </pre>
     *
     * <p>This helps with generating optimal form of sql.
     *
     * <p>In case of aggregating over a list of tuples, similar logic also
     * applies.
     *
     * <p>For example:</p>
     *
     * <pre>
     * Tuples:
     *     ([Gender].[M], [Store].[USA].[CA])
     *     ([Gender].[F], [Store].[USA].[CA])
     *     ([Gender].[M], [Store].[USA])
     *     ([Gender].[F], [Store].[Canada])
     * </pre>
     *
     * <p>will be grouped into</p>
     *
     * <pre>
     * Group 1:
     *     {([Gender].[M], [Store].[USA].[CA]),
     *      ([Gender].[F], [Store].[USA].[CA])}
     * Group 2:
     *     {([Gender].[M], [Store].[USA]),
     *      ([Gender].[F], [Store].[Canada])}
     * </pre>
     *
     * <p>This function returns a boolean value indicating if any constraint
     * can be created from the aggregationList. It is possible that only part
     * of the aggregationList can be applied, which still leads to a (partial)
     * constraint that is represented by the {@code compoundGroupMap}
     * parameter.</p>
     */
    private static boolean makeCompoundGroup(
        int starColumnCount,
        RolapMeasureGroup measureGroup,
        List<List<RolapMember>> aggregationList,
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap)
    {
        // The more generalized aggregation as aggregating over tuples.
        // The special case is a tuple defined by only one member.
        int unsatisfiableTupleCount = 0;
        for (List<RolapMember> aggregation : aggregationList) {
            if (aggregation.size() == 0
                || !(aggregation.get(0) instanceof RolapCubeMember
                    || aggregation.get(0) instanceof VisualTotalMember))
            {
                // not a tuple
                ++unsatisfiableTupleCount;
                continue;
            }

            BitKey bitKey = BitKey.Factory.makeBitKey(starColumnCount);
            RolapCubeMember[] tuple = new RolapCubeMember[aggregation.size()];
            int i = 0;
            for (Member member : aggregation) {
                if (member instanceof VisualTotalMember) {
                    tuple[i] = (RolapCubeMember)
                        ((VisualTotalMember) member).getMember();
                } else {
                    tuple[i] = (RolapCubeMember)member;
                }
                i++;
            }

            boolean tupleUnsatisfiable = false;
            for (RolapCubeMember member : tuple) {
                // Tuple cannot be constrained if any of the member cannot be.
                tupleUnsatisfiable =
                    makeCompoundGroupForMember(
                        member,
                        measureGroup,
                        bitKey);
                if (tupleUnsatisfiable) {
                    // If this tuple is unsatisfiable, skip it and try to
                    // constrain the next tuple.
                    ++unsatisfiableTupleCount;
                    break;
                }
            }

            if (!tupleUnsatisfiable && !bitKey.isEmpty()) {
                // Found tuple(columns) to constrain,
                // now add it to the compoundGroupMap
                addTupleToCompoundGroupMap(tuple, bitKey, compoundGroupMap);
            }
        }

        return unsatisfiableTupleCount == aggregationList.size();
    }

    private static void addTupleToCompoundGroupMap(
        RolapCubeMember[] tuple,
        BitKey bitKey,
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap)
    {
        List<RolapCubeMember[]> compoundGroup = compoundGroupMap.get(bitKey);
        if (compoundGroup == null) {
            compoundGroup = new ArrayList<RolapCubeMember[]>();
            compoundGroupMap.put(bitKey, compoundGroup);
        }
        compoundGroup.add(tuple);
    }

    private static boolean makeCompoundGroupForMember(
        RolapCubeMember member,
        RolapMeasureGroup measureGroup,
        BitKey bitKey)
    {
        assert measureGroup != null;
        final RolapCubeLevel level = member.getLevel();
        for (RolapSchema.PhysColumn key : level.getAttribute().keyList) {
            final RolapStar.Column column =
                measureGroup.getRolapStarColumn(
                    level.getDimension(),
                    key);
            if (column == null) {
                // request is unsatisfiable
                return true;
            }
            bitKey.set(column.getBitPosition());
        }
        return false;
    }

    /**
     * Translates a Map&lt;BitKey, List&lt;RolapMember&gt;&gt; of the same
     * compound member into {@link ListPredicate} by traversing a list of
     * members or tuples.
     *
     * <p>1. The example below is for list of tuples
     *
     * <blockquote>
     * group 1: [Gender].[M], [Store].[USA].[CA]<br/>
     * group 2: [Gender].[F], [Store].[USA].[CA]
     * </blockquote>
     *
     * is translated into
     *
     * <blockquote>
     * (Gender = 'M' AND Store_State = 'CA' AND Store_Country = 'USA')<br/>
     * OR<br/>
     * (Gender = 'F' AND Store_State = 'CA' AND Store_Country = 'USA')
     * </blockquote>
     *
     * <p>The caller of this method will translate this representation into
     * appropriate SQL form as
     *
     * <blockquote>
     * WHERE (gender = 'M'<br/>
     *        AND Store_State = 'CA'<br/>
     *        AND Store_Country = 'USA')<br/>
     *     OR (Gender = 'F'<br/>
     *         AND Store_State = 'CA'<br/>
     *         AND Store_Country = 'USA')
     * </blockquote>
     *
     * <p>2. The example below for a list of members
     *
     * <blockquote>
     * group 1: [USA].[CA], [Canada].[BC]<br/>
     * group 2: [USA].[CA].[San Francisco], [USA].[OR].[Portland]
     * </blockquote>
     *
     * is translated into:
     *
     * <blockquote>
     * (Country = 'USA' AND State = 'CA')<br/>
     * OR (Country = 'Canada' AND State = 'BC')<br/>
     * OR (Country = 'USA' AND State = 'CA' AND City = 'San Francisco')<br/>
     * OR (Country = 'USA' AND State = 'OR' AND City = 'Portland')
     * </pre>
     * <p>The caller of this method will translate this representation into
     * appropriate SQL form. For exmaple, if the underlying DB supports multi
     * value IN-list, the second group will turn into this predicate:
     * <pre>
     * where (country, state, city) IN (('USA', 'CA', 'San Francisco'),
     *                                      ('USA', 'OR', 'Portland'))
     * </blockquote>
     *
     * or, if the DB does not support multi-value IN list:
     *
     * <blockquote>
     * WHERE country = 'USA' AND<br/>
     *           ((state = 'CA' AND city = 'San Francisco') OR<br/>
     *            (state = 'OR' AND city = 'Portland'))
     * </blockquote>
     *
     * @param compoundGroupMap Map from dimensionality to groups
     * @param measureGroup Measure group
     * @return compound predicate for a tuple or a member
     */
    private static StarPredicate makeCompoundPredicate(
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap,
        RolapMeasureGroup measureGroup)
    {
        List<StarPredicate> compoundPredicateList =
            new ArrayList<StarPredicate>();
        final List<RolapSchema.PhysRouter> routers =
            new ArrayList<RolapSchema.PhysRouter>();
        int count = -1;
        for (List<RolapCubeMember[]> group : compoundGroupMap.values()) {
            // e.g.
            // {[USA].[CA], [Canada].[BC]}
            // or
            // {
            ++count;
            StarPredicate compoundGroupPredicate = null;
            for (RolapCubeMember[] tuple : group) {
                // [USA].[CA]
                StarPredicate tuplePredicate = null;

                for (int i = 0; i < tuple.length; i++) {
                    RolapCubeMember member = tuple[i];
                    final RolapSchema.PhysRouter router;
                    if (count == 0) {
                        router = new RolapSchema.CubeRouter(
                            measureGroup, member.getDimension());
                        routers.add(router);
                    } else {
                        router = routers.get(i);
                    }
                    tuplePredicate = makeCompoundPredicateForMember(
                        router, member, tuplePredicate);
                }
                if (tuplePredicate != null) {
                    if (compoundGroupPredicate == null) {
                        compoundGroupPredicate = tuplePredicate;
                    } else {
                        compoundGroupPredicate =
                            compoundGroupPredicate.or(tuplePredicate);
                    }
                }
            }

            if (compoundGroupPredicate != null) {
                // Sometimes the compound member list does not constrain any
                // columns; for example, if only AllLevel is present.
                compoundPredicateList.add(compoundGroupPredicate);
            }
        }

        return Predicates.or(compoundPredicateList);
    }

    private static StarPredicate makeCompoundPredicateForMember(
        RolapSchema.PhysRouter router,
        RolapCubeMember member,
        StarPredicate memberPredicate)
    {
        final RolapCubeLevel level = member.getLevel();
        final List<RolapSchema.PhysColumn> keyList = level.attribute.keyList;
        final List<Object> valueList = member.getKeyAsList();
        for (Pair<RolapSchema.PhysColumn, Object> pair
            : Pair.iterate(keyList, valueList))
        {
            final ValueColumnPredicate predicate =
                new ValueColumnPredicate(
                    new PredicateColumn(
                        router,
                        pair.left),
                    pair.right);
            if (memberPredicate == null) {
                memberPredicate = predicate;
            } else {
                memberPredicate = memberPredicate.and(predicate);
            }
        }
        return memberPredicate;
    }

    /**
     * Retrieves the value of a cell from the cache.
     *
     * @param request Cell request
     * @pre request != null && !request.isUnsatisfiable()
     * @return Cell value, or null if cell is not in any aggregation in cache,
     *   or {@link Util#nullValue} if cell's value is null
     */
    public abstract Object getCellFromCache(CellRequest request);

    public abstract Object getCellFromCache(
        CellRequest request,
        PinSet pinSet);

    /**
     * Generates a SQL statement which will return the rows which contribute to
     * this request.
     *
     * @param request Cell request
     * @param countOnly If true, return a statment which returns only the count
     * @param starPredicateSlicer A StarPredicate representing slicer positions
     * that could not be represented by the CellRequest, or
     * <code>null</code> if no additional predicate is necessary.
     * @return SQL statement
     */
    public abstract String getDrillThroughSql(
        DrillThroughCellRequest request,
        StarPredicate starPredicateSlicer,
        List<Exp> fields,
        boolean countOnly);

    public static RolapCacheRegion makeCacheRegion(
        final RolapMeasureGroup measureGroup,
        final CacheControl.CellRegion region)
    {
        final List<Member> measureList = CacheControlImpl.findMeasures(region);
        final List<RolapStar.Measure> starMeasureList =
            new ArrayList<RolapStar.Measure>();
        final RolapStar star = measureGroup.getStar();
        for (Member measure : measureList) {
            if (!(measure instanceof RolapStoredMeasure)) {
                continue;
            }
            final RolapStoredMeasure storedMeasure =
                (RolapStoredMeasure) measure;
            final RolapStar.Measure starMeasure =
                storedMeasure.getStarMeasure();
            assert starMeasure != null;
            if (star != starMeasure.getStar()) {
                continue;
            }
            // TODO: each time this code executes, baseCube is set.
            // Should there be a 'break' here? Are all of the
            // storedMeasure cubes the same cube? Is the measureList always
            // non-empty so that baseCube is always set?
            assert measureGroup == storedMeasure.getMeasureGroup();
            starMeasureList.add(starMeasure);
        }
        final RolapCacheRegion cacheRegion =
            new RolapCacheRegion(star, starMeasureList);
        if (region instanceof CacheControlImpl.CrossjoinCellRegion) {
            final CacheControlImpl.CrossjoinCellRegion crossjoin =
                (CacheControlImpl.CrossjoinCellRegion) region;
            for (CacheControl.CellRegion component
                : crossjoin.getComponents())
            {
                constrainCacheRegion(cacheRegion, measureGroup, component);
            }
        } else {
            constrainCacheRegion(cacheRegion, measureGroup, region);
        }
        return cacheRegion;
    }

    private static void constrainCacheRegion(
        final RolapCacheRegion cacheRegion,
        final RolapMeasureGroup measureGroup,
        final CacheControl.CellRegion region)
    {
        if (region instanceof CacheControlImpl.MemberCellRegion) {
            final CacheControlImpl.MemberCellRegion memberCellRegion =
                (CacheControlImpl.MemberCellRegion) region;
            final List<Member> memberList = memberCellRegion.getMemberList();
            for (Member member : memberList) {
                if (member.isMeasure()) {
                    continue;
                }
                final RolapCubeMember rolapMember;
                if (member instanceof RolapCubeMember) {
                    rolapMember = (RolapCubeMember) member;
                } else {
                    rolapMember = (RolapCubeMember)
                        measureGroup.getCube().getSchemaReader()
                            .getMemberByUniqueName(
                                Util.parseIdentifier(member.getUniqueName()),
                                true);
                }
                rolapMember.getLevel().getLevelReader().constrainRegion(
                    Predicates.memberPredicate(
                        new RolapSchema.CubeRouter(
                            measureGroup,
                            rolapMember.getDimension()),
                        rolapMember),
                    measureGroup,
                    cacheRegion);
            }
        } else if (region instanceof CacheControlImpl.MemberRangeCellRegion) {
            final CacheControlImpl.MemberRangeCellRegion rangeRegion =
                (CacheControlImpl.MemberRangeCellRegion) region;
            final RolapCubeLevel level = (RolapCubeLevel)rangeRegion.getLevel();

            level.getLevelReader().constrainRegion(
                Predicates.rangePredicate(
                    new RolapSchema.CubeRouter(
                        measureGroup,
                        level.cubeDimension),
                    rangeRegion.getLowerInclusive(),
                    rangeRegion.getLowerBound(),
                    rangeRegion.getUpperInclusive(),
                    rangeRegion.getUpperBound()),
                measureGroup,
                cacheRegion);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns a {@link mondrian.rolap.CellReader} which reads cells from cache.
     */
    public CellReader getCacheCellReader() {
        return new CellReader() {
            // implement CellReader
            public Object get(RolapEvaluator evaluator) {
                CellRequest request = makeRequest(evaluator);
                if (request == null || request.isUnsatisfiable()) {
                    // request out of bounds
                    return Util.nullValue;
                }
                return getCellFromCache(request);
            }

            public int getMissCount() {
                return 0; // RolapAggregationManager never lies
            }

            public boolean isDirty() {
                return false;
            }
        };
    }

    /**
     * Creates a {@link PinSet}.
     *
     * @return a new PinSet
     */
    public abstract PinSet createPinSet();

    /**
     * A set of segments which are pinned (prevented from garbage collection)
     * for a short duration as a result of a cache inquiry.
     */
    public interface PinSet {
    }
}

// End RolapAggregationManager.java
