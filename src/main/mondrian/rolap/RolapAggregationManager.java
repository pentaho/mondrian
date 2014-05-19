/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
//
// jhyde, 30 August, 2001
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.VisualTotalsFunDef.VisualTotalMember;
import mondrian.resource.MondrianResource;
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
     * @return Cell request, or null if the request is unsatisfiable
     */
    public static CellRequest makeRequest(final Member[] members)
    {
        return makeCellRequest(members, false, false, null, null, null);
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
     * @return Cell request, or null if the request is unsatisfiable
     */
    public static DrillThroughCellRequest makeDrillThroughRequest(
        final List<RolapMember> members,
        final boolean extendedContext,
        RolapCube cube,
        List<Exp> fieldsList)
    {
        assert cube != null;
        return (DrillThroughCellRequest) makeCellRequest(
            members, true, extendedContext, cube, fieldsList, null);
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
     * @return Cell request, or null if the request is unsatisfiable
     */
    public static CellRequest makeRequest(
        RolapEvaluator evaluator)
    {
        final RolapMember[] currentMembers = evaluator.getNonAllMembers();
        final List<List<List<Member>>> aggregationLists =
            evaluator.getAggregationLists();

        final RolapStoredMeasure measure =
            (RolapStoredMeasure) currentMembers[0];
        final RolapStar.Measure starMeasure = measure.getStarMeasure();
        assert starMeasure != null;
        final RolapStar star = starMeasure.getStar();
        int starColumnCount = star.getColumnCount();

        CellRequest request =
            makeCellRequest(
                currentMembers,
                false,
                false,
                null,
                null,
                evaluator);

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
            Map<BitKey, List<RolapMember[]>> compoundGroupMap =
                new LinkedHashMap<BitKey, List<RolapMember[]>>();

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
        List<Exp> fieldsList,
        Evaluator evaluator)
    {
        final List<RolapMember> rolapMembers =
            new ArrayList<RolapMember>((List) Arrays.asList(members));
        return makeCellRequest(
            rolapMembers,
            drillThrough,
            extendedContext,
            cube,
            fieldsList,
            evaluator);
    }

    private static CellRequest makeCellRequest(
        final List<RolapMember> memberList,
        boolean drillThrough,
        final boolean extendedContext,
        RolapCube cube,
        List<Exp> fieldsList,
        Evaluator evaluator)
    {
        // Need cube for drill-through requests
        assert drillThrough == (cube != null);

        if (extendedContext) {
            assert drillThrough;
        }

        final RolapStoredMeasure measure;
        if (drillThrough) {
            cube = RolapCell.chooseDrillThroughCube(memberList, cube);
            if (cube == null) {
                return null;
            }
            if (memberList.size() > 0
                && memberList.get(0) instanceof RolapStoredMeasure)
            {
                measure = (RolapStoredMeasure) memberList.get(0);
            } else {
                measure = (RolapStoredMeasure) cube.getMeasures().get(0);
            }
        } else {
            if (memberList.size() > 0
                && memberList.get(0) instanceof RolapStoredMeasure)
            {
                measure = (RolapStoredMeasure) memberList.get(0);
            } else {
                return null;
            }
        }

        final RolapMeasureGroup measureGroup = measure.getMeasureGroup();
        final RolapStar.Measure starMeasure = measure.getStarMeasure();
        assert starMeasure != null;

        if (drillThrough) {
            // This is a drillthrough request.
            DrillThroughCellRequest request =
                new DrillThroughCellRequest(starMeasure, extendedContext);
            if (fieldsList != null
                && fieldsList.size() > 0)
            {
                // Since a field list was specified, there will be some columns
                // to include in the result set & others that we don't. This
                // happens when the MDX is a DRILLTHROUGH operation and
                // includes a RETURN clause which specified exactly which
                // fields to return.
                final SchemaReader reader = cube.getSchemaReader().withLocus();
                for (Exp exp : fieldsList) {
                    final OlapElement member =
                        reader.lookupCompound(
                            cube,
                            Util.parseIdentifier(exp.toString()),
                            false,
                            Category.Unknown);
                    if (member == null) {
                        throw MondrianResource.instance()
                            .DrillthroughUnknownMemberInReturnClause
                                .ex(exp.toString());
                    }
                    addDrillthroughColumn(member, measureGroup, request);
                }
            }

            // Sort the members.  Columns will be added to
            // DrillThroughCellRequest which will preserve the order
            // they are added.
            Collections.sort(
                memberList,
                new CubeOrderedMemberLevelComparator(cube.getDimensionList()));

            // Iterate over members.
            for (RolapMember member : memberList) {
                if (member.getHierarchy().getRolapHierarchy().closureFor
                    != null)
                {
                    // If this gets called for an internal "closure" level,
                    // we skip this level.
                    // REVIEW: Why should this ever happen??
                    continue;
                }
                // Start by constraining the request on the current member.
                // This will result in a SQL with a WHERE clause which will
                // limit the rows to those covered by the current cell.
                RolapCubeLevel level = member.getLevel();
                final boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, measureGroup, request);
                if (needToReturnNull) {
                    return null;
                }
                if (fieldsList == null
                    || fieldsList.size() == 0)
                {
                    // There was no RETURN clause in the query.
                    // We add the key columns of the non-all members
                    // which are part of the evaluator.
                    // This is the default behavior.
                    if (member.getDimension().isMeasures()) {
                        // Measures are a bit different.
                        if (!member.isCalculated()) {
                            request.addDrillThroughMeasure(
                                ((RolapStoredMeasure)member).getStarMeasure(),
                                member.getName());
                        }
                    } else {
                        // We can't add 'all' levels, since they don't
                        // map to a DB column.
                        if (!level.isAll()) {
                            addNonConstrainingColumns(
                                level, measureGroup, request);
                            if (extendedContext) {
                                while (level.getChildLevel() != null) {
                                    level = level.getChildLevel();
                                    addNonConstrainingColumns(
                                        level, measureGroup, request);
                                }
                            }
                        }
                    }
                }
            }
            return request;
        } else {
            // This is the code path for regular cell requests.
            // For each member in the evaluator, we constrain the request.
            CellRequest request =
                new CellRequest(starMeasure, extendedContext, drillThrough);
            for (RolapMember member : Util.subList(memberList, 1)) {
                if (
                    member instanceof
                        RestrictedMemberReader.MultiCardinalityDefaultMember)
                {
                    continue;
                }
                final RolapCubeLevel level = member.getLevel();
                final boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, measureGroup, request);
                if (needToReturnNull) {
                    // check to see if the current member is part of an ignored
                    // unrelated dimension
                    if (evaluator == null
                        || !evaluator.mightReturnNullForUnrelatedDimension()
                        || evaluator.needToReturnNullForUnrelatedDimension(
                            new Member[] {member})) {
                        return null;
                    }
                }
            }
            return request;
        }
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
     * @param level Level to constrain
     * @param measureGroup Measure group
     * @param request Cell request
     */
    private static void addNonConstrainingColumns(
        RolapCubeLevel level,
        final RolapMeasureGroup measureGroup,
        final CellRequest request)
    {
        List<RolapCubeLevel> levels = new ArrayList<RolapCubeLevel>();
        if (request.extendedContext) {
            // As per the API, if extendedContext is set to
            // true, we must also add columns for the sub
            // levels of the members which are part of the
            // evaluator. This will ventilate the results
            // and make it more human friendly.
            do {
                levels.add(level);
                level = level.getChildLevel();
            } while (level != null);
        } else {
            levels.add(level);
        }
        for (RolapCubeLevel currentLevel : levels) {
            for (RolapSchema.PhysColumn column
                : currentLevel.attribute.getKeyList())
            {
                RolapStar.Column starColumn =
                    measureGroup.getRolapStarColumn(
                        currentLevel.cubeDimension,
                        column);
                if (starColumn != null) {
                    request.addConstrainedColumn(starColumn, null);
                }
                if (request instanceof DrillThroughCellRequest) {
                    ((DrillThroughCellRequest) request)
                        .addDrillThroughColumn(
                            starColumn,
                            currentLevel.getName());
                }
            }
        }
    }

    private static void addDrillthroughColumn(
        final OlapElement element,
        final RolapMeasureGroup measureGroup,
        final DrillThroughCellRequest request)
    {
        final RolapCubeLevel level;
        if (element.getDimension().isMeasures()) {
            // Measures are a bit different.
            request.addDrillThroughMeasure(
                ((RolapStoredMeasure)element).getStarMeasure(),
                element.getName());
            return;
        } else if (element instanceof RolapCubeLevel) {
            level = (RolapCubeLevel) element;
        } else if (element instanceof RolapCubeDimension) {
            RolapCubeHierarchy hierarchy =
                (RolapCubeHierarchy) element.getHierarchy();
            if (hierarchy.getLevelList().get(0).isAll()) {
                level = hierarchy.getLevelList().get(1);
            } else {
                level = hierarchy.getLevelList().get(0);
            }
        } else if (element instanceof RolapCubeHierarchy) {
            RolapCubeHierarchy hierarchy = (RolapCubeHierarchy)element;
            if (hierarchy.getLevelList().get(0).isAll()) {
                level = hierarchy.getLevelList().get(1);
            } else {
                level = hierarchy.getLevelList().get(0);
            }
        } else if (element instanceof RolapMember) {
            level = ((RolapMember)element).getLevel();
        } else {
            throw MondrianResource.instance()
                .DrillthroughInvalidMemberInReturnClause
                    .ex(element.getUniqueName(), element.getClass().getName());
        }
        if (level.getHierarchy().closureFor != null) {
            return;
        }

        for (RolapSchema.PhysColumn column : level.attribute.getKeyList()) {
            RolapStar.Column starColumn =
                measureGroup.getRolapStarColumn(
                    level.cubeDimension,
                    column);
            if (starColumn != null) {
                request.addConstrainedColumn(starColumn, null);
                request.addDrillThroughColumn(
                    starColumn,
                    level.getName());
            }
        }
    }

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
        Map<BitKey, List<RolapMember[]>> compoundGroupMap)
    {
        // The more generalized aggregation as aggregating over tuples.
        // The special case is a tuple defined by only one member.
        int unsatisfiableTupleCount = 0;
        for (List<RolapMember> aggregation : aggregationList) {
            if (aggregation.size() == 0) {
                // not a tuple
                ++unsatisfiableTupleCount;
                continue;
            }

            BitKey bitKey = BitKey.Factory.makeBitKey(starColumnCount);
            RolapMember[] tuple = new RolapMember[aggregation.size()];
            int i = 0;
            for (RolapMember member : aggregation) {
                if (member instanceof VisualTotalMember) {
                    tuple[i] = ((VisualTotalMember) member).getMember();
                } else {
                    tuple[i] = member;
                }
                i++;
            }

            boolean tupleUnsatisfiable = false;
            for (RolapMember member : tuple) {
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
        RolapMember[] tuple,
        BitKey bitKey,
        Map<BitKey, List<RolapMember[]>> compoundGroupMap)
    {
        List<RolapMember[]> compoundGroup = compoundGroupMap.get(bitKey);
        if (compoundGroup == null) {
            compoundGroup = new ArrayList<RolapMember[]>();
            compoundGroupMap.put(bitKey, compoundGroup);
        }
        compoundGroup.add(tuple);
    }

    private static boolean makeCompoundGroupForMember(
        RolapMember member,
        RolapMeasureGroup measureGroup,
        BitKey bitKey)
    {
        assert measureGroup != null;
        final RolapCubeLevel level = member.getLevel();
        for (RolapSchema.PhysColumn key : level.getAttribute().getKeyList()) {
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
    static StarPredicate makeCompoundPredicate(
        Map<BitKey, List<RolapMember[]>> compoundGroupMap,
        RolapMeasureGroup measureGroup)
    {
        List<StarPredicate> compoundPredicateList =
            new ArrayList<StarPredicate>();
        final List<RolapSchema.PhysRouter> routers =
            new ArrayList<RolapSchema.PhysRouter>();
        int count = -1;
        for (List<RolapMember[]> group : compoundGroupMap.values()) {
            // e.g.
            // {[USA].[CA], [Canada].[BC]}
            // or
            // {
            ++count;
            StarPredicate compoundGroupPredicate = null;
            for (RolapMember[] tuple : group) {
                // [USA].[CA]
                StarPredicate tuplePredicate = null;

                for (int i = 0; i < tuple.length; i++) {
                    RolapMember member = tuple[i];
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
        RolapMember member,
        StarPredicate memberPredicate)
    {
        final RolapCubeLevel level = member.getLevel();
        final List<RolapSchema.PhysColumn> keyList =
            level.attribute.getKeyList();
        final List<Comparable> valueList = member.getKeyAsList();
        for (Pair<RolapSchema.PhysColumn, Comparable> pair
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
        Util.deprecated("not used -- remove", true);
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
                final RolapMember rolapMember = (RolapMember) member;
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

    /**
     * Compares members based on the position of their respective
     * levels wrt the list of RolapCubeDimension objects.
     */
    private static class CubeOrderedMemberLevelComparator
        implements Comparator<Member>
    {
        private final List<RolapCubeLevel> orderedLevels =
            new ArrayList<RolapCubeLevel>();

        public CubeOrderedMemberLevelComparator(
            List<? extends RolapCubeDimension> dimList)
        {
            for (RolapCubeDimension dim : dimList) {
                for (RolapCubeHierarchy hier : dim.getHierarchyList()) {
                    orderedLevels.addAll(hier.getLevelList());
                }
            }
        }

        public int compare(Member o1, Member o2) {
            return orderedLevels.indexOf(o1.getLevel())
                - orderedLevels.indexOf(o2.getLevel());
        }
    }
}

// End RolapAggregationManager.java
