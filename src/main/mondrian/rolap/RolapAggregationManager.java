/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.Util;
import mondrian.rolap.agg.*;
import mondrian.olap.CacheControl;

import java.util.*;
import java.io.PrintWriter;

/**
 * <code>RolapAggregationManager</code> manages all {@link
 * mondrian.rolap.agg.Aggregation}s in the system. It is a singleton class.
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
        return makeCellRequest(members, false, false);
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
     * @return Cell request, or null if the requst is unsatisfiable
     */
    public static CellRequest makeDrillThroughRequest(
        final Member[] members,
        final boolean extendedContext)
    {
        return makeCellRequest(members, true, extendedContext);
    }

    /**
     * Creates a request to evaluate the cell identified by the context specified
     * in <code>evaluator</code>.
     *
     * <p>If any of the members from the context is the null member, returns
     * null, since there is no cell. If the measure is calculated, returns
     * null.
     *
     * @param evaluator the cell specified by the evaluator context
     * @return Cell request, or null if the requst is unsatisfiable
     */
    public static CellRequest makeRequest(
        RolapEvaluator evaluator) {
        final Member[] currentMembers = evaluator.getMembers();
        final List<List<RolapMember>> aggregationLists =
            evaluator.getAggregationLists();

        final RolapStoredMeasure measure = 
            (RolapStoredMeasure) currentMembers[0];
        final RolapStar.Measure starMeasure =
            (RolapStar.Measure) measure.getStarMeasure();
        assert starMeasure != null;
        int starColumnCount = starMeasure.getStar().getColumnCount();

        CellRequest request = makeCellRequest(currentMembers, false, false);

        /*
         * Now setting the compound keys.
         * First find out the columns referenced in the aggregateMemberList.
         * Each list defines a compound member.
         */
        if (aggregationLists == null) {
            return request;
        }

        BitKey compoundBitKey;
        StarPredicate compoundPredicate;
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap;
        boolean unsatisfiable;

        for (List<RolapMember> aggregationList : aggregationLists) {
            compoundBitKey = BitKey.Factory.makeBitKey(starColumnCount);
            compoundBitKey.clear();
            compoundGroupMap = new LinkedHashMap<BitKey, List<RolapCubeMember>>();
            unsatisfiable =
                makeCompoundGroup(
                    starColumnCount, 
                    measure.getCube(), 
                    aggregationList, 
                    compoundGroupMap);
            if (unsatisfiable) {
                return null;
            }
            compoundPredicate =
                makeCompoundPredicate(compoundGroupMap, measure.getCube());
            if (compoundPredicate != null) {
                /*
                 * Only add the compound constraint when it is not empty.
                 */
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
        final boolean extendedContext)
    {
        if (extendedContext) {
            assert (drillThrough);
        }

        if (!(members[0] instanceof RolapStoredMeasure)) {
            return null;
        }

        final RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
        final RolapStar.Measure starMeasure =
            (RolapStar.Measure) measure.getStarMeasure();
        assert starMeasure != null;
        final CellRequest request =
            new CellRequest(starMeasure, extendedContext, drillThrough);
        
        // Since 'request.extendedContext == false' is a well-worn code path,
        // we have moved the test outside the loop.
        if (extendedContext) {
            for (int i = 1; i < members.length; i++) {
                final RolapCubeMember member = (RolapCubeMember) members[i];
                addNonConstrainingColumns(member, measure.getCube(), request);

                final RolapCubeLevel level = member.getLevel();
                final boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, measure.getCube(), request);
                if (needToReturnNull) {
                    return null;
                }
            }
        } else {
            for (int i = 1; i < members.length; i++) {
                RolapCubeMember member = (RolapCubeMember) members[i];
                final RolapCubeLevel level = member.getLevel();
                final boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, measure.getCube(), request);
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
     * @param baseCube base cube if virtual
     * @param request Cell request
     */
    private static void addNonConstrainingColumns(
        final RolapCubeMember member,
        final RolapCube baseCube,
        final CellRequest request)
    {
        final RolapCubeHierarchy hierarchy = member.getHierarchy();
        final Level[] levels = hierarchy.getLevels();
        for (int j = levels.length - 1, depth = member.getLevel().getDepth();
             j > depth; j--) {
            final RolapCubeLevel level = (RolapCubeLevel)levels[j];
            RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
            if (column != null) {
                request.addConstrainedColumn(column, null);
                if (request.extendedContext &&
                        level.getNameExp() != null) {
                    final RolapStar.Column nameColumn = column.getNameColumn();
                    Util.assertTrue(nameColumn != null);
                    request.addConstrainedColumn(nameColumn, null);
                }
            }
        }
    }

    /*
     * Group members from the same compound(i.e. hierarchy) into groups that
     * are constrained by the same set of columns by traversing list of tuples
     * or members.
     * E.g.
     *
     * Members
     * [USA].[CA],
     * [Canada].[BC],
     * [USA].[CA].[San Francisco],
     * [USA].[OR].[Portland]
     *
     * will be grouped into
     * group 1: [USA].[CA], [Canada].[BC]
     * group 2: [USA].[CA].[San Francisco], [USA].[OR].[Portland]
     *
     * This helps with generating optimal form of sql.
     *
     * <p>Incase of aggregating over a list of tuples, each tuple forms a
     * seperate group
     * E.g.
     *
     * Tuples:
     * ([Gender].[M], [Store].[All Stores].[USA].[CA])
     * ([Gender].[F], [Store].[All Stores].[USA].[CA])
     *
     * will be grouped into
     * group 1: [Gender].[M], [Store].[All Stores].[USA].[CA]
     * group 2: [Gender].[F], [Store].[All Stores].[USA].[CA]
     *
     */
    private static boolean makeCompoundGroup(
        int starColumnCount,
        RolapCube baseCube,
        List memberList,
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap)
    {
        /*
         * TODO: this should also return a boolean unsatisfiable
         * if not all members can be constrained.
         * However, the presense of All member will exonerate the whole list.
         */

        int unsatisfiableMemberCount;

        if (containsTuple(memberList)) {
            unsatisfiableMemberCount =
                makeTuplesCompoundGroup(
                    starColumnCount,
                    baseCube,
                    memberList,
                    compoundGroupMap);
        }
        else {
            unsatisfiableMemberCount =
                makeMembersCompoundGroup(
                    starColumnCount,
                    baseCube,
                    memberList,
                    compoundGroupMap);
        }
        return (unsatisfiableMemberCount == memberList.size());
    }

    private static boolean containsTuple(List memberList) {
        return memberList.get(0) instanceof Member[];
    }

    private static int makeTuplesCompoundGroup(
        int starColumnCount,
        RolapCube baseCube,
        List memberList,
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap)
    {
        int unsatisfiableMemberCount = 0;
        for (Object object : memberList) {
            Member[] tuple = (Member[]) object;

            BitKey bitKey = BitKey.Factory.makeBitKey(starColumnCount);
            for (Member member : tuple) {
                unsatisfiableMemberCount +=
                    makeCompoundGroupForAMember((RolapCubeMember)member, baseCube, bitKey);
            }
            if (bitKey.isEmpty()) {
                // Did not find columns to constrain
                continue;
            }
            createCompoundGroupBasedOnBitKey(tuple, bitKey, compoundGroupMap);
        }
        return unsatisfiableMemberCount;
    }

    private static int makeMembersCompoundGroup(
        int starColumnCount,
        RolapCube baseCube,
        List<RolapCubeMember> memberList,
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap)
    {
        int unsatisfiableMemberCount=0;
        for (RolapCubeMember member : memberList) {
            BitKey bitKey = BitKey.Factory.makeBitKey(starColumnCount);
            unsatisfiableMemberCount +=
                makeCompoundGroupForAMember(member, baseCube, bitKey);

            if (bitKey.isEmpty()) {
                // Did not find columns to constrain
                continue;
            }

            createCompoundGroupBasedOnBitKey(member, bitKey, compoundGroupMap);
        }
        return unsatisfiableMemberCount;
    }

    private static void createCompoundGroupBasedOnBitKey(
        Object member,
        BitKey bitKey,
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap)
    {
        List compoundGroup = compoundGroupMap.get(bitKey);
        if (compoundGroup == null) {
            compoundGroup = new ArrayList<RolapCubeMember>();
            compoundGroupMap.put(bitKey, compoundGroup);
        }
        compoundGroup.add(member);

    }

    private static int makeCompoundGroupForAMember(
        RolapCubeMember member,
        RolapCube baseCube,
        BitKey bitKey)
    {
        RolapCubeMember levelMember = member;
        int unsatisfiableMemberCount=0;
        while (levelMember != null) {
            RolapCubeLevel level = levelMember.getLevel();
            // Only need to constrain the nonAll levels
            if (!level.isAll()) {
                RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
                if (column != null) {
                    bitKey.set(column.getBitPosition());
                } else {
                    // Failed to process compound member constraint.
                    unsatisfiableMemberCount++;
                    break;
                }
            }

            levelMember = levelMember.getParentMember();
        }
        return unsatisfiableMemberCount;
    }

    /**
     * Translate Map<BitKey, List<RolapMember>> of the same compound member into
     * ListPredicate by traversing list of members or tuples.
     * <p>1. The example below is for list of tuples
     *
     * <blockquote>
     * <p>group 1: [Gender].[M], [Store].[All Stores].[USA].[CA]
     * group 2: [Gender].[F], [Store].[All Stores].[USA].[CA]
     * </blockquote>
     * is translated into
     * <blockquote>
     * <p>(Gender=M AND Store_State=CA AND Store_Country=USA)
     * OR
     * (Gender=F AND Store_State=CA AND Store_Country=USA)
     * </blockquote>
     * <p>The caller of this method will translate this representation into
     * appropriate SQL form as
     * <blockquote>
     *  <p>where (gender = 'M' and Store_State = 'CA' AND Store_Country = 'USA')
     *     OR (Gender = 'F' and Store_State = 'CA' AND Store_Country = 'USA')
     * </blockquote>
     * <p>2. The example below for a list of members
     * <blockquote>
     * <p>group 1: [USA].[CA], [Canada].[BC]
     * group 2: [USA].[CA].[San Francisco], [USA].[OR].[Portland]
     * </blockquote>
     * is translated into:
     * <blockquote>
     * <p>(Country=USA AND State=CA)
     *     OR (Country=Canada AND State=BC)
     * OR
     * (Country=USA AND State=CA AND City=San Francisco)
     *     OR (Country=USA AND State=OR AND City=Portland)
     * </blockquote>
     * <p>The caller of this method will translate this representation into
     * appropriate SQL form. For exmaple, if the underlying DB supports multi value
     * IN-list, the second group will turn into this predicate:
     * <blockquote>
     * <p>    where (country, state, city) IN ((USA, CA, San Francisco),
     *                                      (USA, OR, Portland))
     * </blockquote>
     * or, if the DB does not support multi-value IN list:
     * <blockquote>
     * <p>    where country=USA AND
     *           ((state=CA AND city = San Francisco) OR
     *            (state=OR AND city=Portland))
     * </blockquote>
     *
     * @param compoundGroupMap
     * @param baseCube base cube if virtual 
     * @return compound predicate for a tuple or a member
     */
    public static StarPredicate makeCompoundPredicate(
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap,
        RolapCube baseCube)
    {
        List<StarPredicate> compoundPredicateList;
        if(containsTuple(compoundGroupMap)){
            compoundPredicateList =
                makeCompoundPredicateForTuple(compoundGroupMap, baseCube);
        } else {
            compoundPredicateList =
                makeCompoundPredicateForMembers(compoundGroupMap, baseCube);
        }
        StarPredicate compoundPredicate = null;

        if (compoundPredicateList.size() > 1) {
            compoundPredicate = new OrPredicate(compoundPredicateList);
        } else if (compoundPredicateList.size() == 1) {
            compoundPredicate = compoundPredicateList.get(0);
        }

        return compoundPredicate;
    }

    private static List<StarPredicate> makeCompoundPredicateForMembers(
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap,
        RolapCube baseCube)
    {
        List<StarPredicate> compoundPredicateList =
            new ArrayList<StarPredicate> ();
        for (List<RolapCubeMember> groups : compoundGroupMap.values()) {
            /*
             * e.g. [USA].[CA], [Canada].[BC]
             */
            StarPredicate compoundGroupPredicate = null;
            for (RolapCubeMember member : groups) {
               /*
                * [USA].[CA]
                */
                StarPredicate memberPredicate = null;
                memberPredicate = makeCompoundPredicateForAMember(
                        member, baseCube, memberPredicate);
                if (memberPredicate != null) {
                    if (compoundGroupPredicate == null) {
                        compoundGroupPredicate = memberPredicate;
                    } else {
                        compoundGroupPredicate =
                            compoundGroupPredicate.or(memberPredicate);
                    }
                }
            }
            addToCompoundPredicateList(
                compoundGroupPredicate, compoundPredicateList);
        }
        return compoundPredicateList;
    }

    private static void addToCompoundPredicateList(
        StarPredicate startPredicate,
        List<StarPredicate> compoundPredicateList)
    {
        if (startPredicate != null) {
            /*
             * Sometimes the compound member list does not constrain any
             * columns; for example, if only AllLevel is present.
             */
            compoundPredicateList.add(startPredicate);
        }
    }

    private static List<StarPredicate> makeCompoundPredicateForTuple(
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap,
        RolapCube baseCube)
    {
        List<StarPredicate> compoundPredicateList =
            new ArrayList<StarPredicate>();
        for (List groups : compoundGroupMap.values()) {
            /*
             * e.g. ([USA].[CA], [Gender].[M])
             */
            for (Object membersArray : groups) {
                Member[] tuple = (Member[]) membersArray;
                StarPredicate tuplePredicate = null;
                for (Member m1 : tuple) {
                    RolapCubeMember member = (RolapCubeMember) m1;
                    /*
                    * [USA].[CA]
                    */
                    tuplePredicate = makeCompoundPredicateForAMember(
                                member, baseCube, tuplePredicate);
                }
                addToCompoundPredicateList(
                        tuplePredicate, compoundPredicateList);
            }
        }
        return compoundPredicateList;
    }

    private static boolean containsTuple(
        Map<BitKey, List<RolapCubeMember>> compoundGroupMap)
    {
        Collection<List<RolapCubeMember>> lists = compoundGroupMap.values();
        Iterator<List<RolapCubeMember>> iterator = lists.iterator();

        if(!iterator.hasNext()) return false;
        List list = iterator.next();
        return list.get(0) instanceof Member[];
    }

    private static StarPredicate makeCompoundPredicateForAMember(
        RolapCubeMember member,
        RolapCube baseCube,
        StarPredicate memberPredicate)
    {
        while (member != null) {
            RolapCubeLevel level = member.getLevel();
            if (!level.isAll()) {
//                RolapStar.Column column = null;
//                if (level instanceof RolapCubeLevel) {
//                    column = ((RolapCubeLevel)level).getBaseStarKeyColumn(baseCube);
//                }
                RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
                if (memberPredicate == null) {
                    memberPredicate =
                        new ValueColumnPredicate(column, member.getKey());
                } else {
                    memberPredicate =
                        memberPredicate.and(
                            new ValueColumnPredicate(column, member.getKey()));
                }
            }
            member = member.getParentMember();
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
     * @return SQL statement
     */
    public abstract String getDrillThroughSql(
        CellRequest request,
        boolean countOnly);

    /**
     * Returns an API with which to explicitly manage the contents of the cache.
     *
     * @param pw Print writer, for tracing
     * @return CacheControl API
     */
    public CacheControl getCacheControl(final PrintWriter pw) {
        return new CacheControlImpl() {
            protected void flushNonUnion(final CellRegion region) {
                final List<RolapStar> starList = getStarList(region);

                // For each of the candidate stars, scan the list of aggregates.
                for (RolapStar star : starList) {
                    star.flush(this, region);
                }
            }

            public void flush(final CellRegion region) {
                if (pw != null) {
                    pw.println("Cache state before flush:");
                    printCacheState(pw, region);
                    pw.println();
                }
                super.flush(region);
                if (pw != null) {
                    pw.println("Cache state after flush:");
                    printCacheState(pw, region);
                    pw.println();
                }
            }

            public void trace(final String message) {
                if (pw != null) {
                    pw.println(message);
                }
            }
        };
    }

    public static RolapCacheRegion makeCacheRegion(
        final RolapStar star,
        final CacheControl.CellRegion region)
    {
        final List<Member> measureList = CacheControlImpl.findMeasures(region);
        final List<RolapStar.Measure> starMeasureList =
            new ArrayList<RolapStar.Measure>();
        RolapCube baseCube = null;
        for (Member measure : measureList) {
            if (!(measure instanceof RolapStoredMeasure)) {
                continue;
            }
            final RolapStoredMeasure storedMeasure =
                (RolapStoredMeasure) measure;
            final RolapStar.Measure starMeasure =
                (RolapStar.Measure) storedMeasure.getStarMeasure();
            assert starMeasure != null;
            if (star != starMeasure.getStar()) {
                continue;
            }
            // TODO: each time this code executes, baseCube is set.
            // Should there be a 'break' here? Are all of the
            // storedMeasure cubes the same cube? Is the measureList always
            // non-empty so that baseCube is always set?
            baseCube = storedMeasure.getCube();
            starMeasureList.add(starMeasure);
        }
        final RolapCacheRegion cacheRegion =
            new RolapCacheRegion(star, starMeasureList);
        if (region instanceof CacheControlImpl.CrossjoinCellRegion) {
            final CacheControlImpl.CrossjoinCellRegion crossjoin =
                (CacheControlImpl.CrossjoinCellRegion) region;
            for (CacheControl.CellRegion component : crossjoin.getComponents()) {
                constrainCacheRegion(cacheRegion, baseCube, component);
            }
        } else {
            constrainCacheRegion(cacheRegion, baseCube, region);
        }
        return cacheRegion;
    }

    private static void constrainCacheRegion(
        final RolapCacheRegion cacheRegion,
        final RolapCube baseCube,
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
                final RolapCubeMember rolapMember = (RolapCubeMember) member;
                final RolapCubeLevel level = rolapMember.getLevel();
                RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
                
                level.getLevelReader().constrainRegion(
                    new MemberColumnPredicate(column, rolapMember), 
                    baseCube, 
                    cacheRegion);
            }
        } else if (region instanceof CacheControlImpl.MemberRangeCellRegion) {
            final CacheControlImpl.MemberRangeCellRegion rangeRegion =
                (CacheControlImpl.MemberRangeCellRegion) region;
            final RolapCubeLevel level = (RolapCubeLevel)rangeRegion.getLevel();
            RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
            
            level.getLevelReader().constrainRegion(
                new RangeColumnPredicate(
                    column,
                    rangeRegion.getLowerInclusive(),
                    (rangeRegion.getLowerBound() == null ?
                        null :
                        new MemberColumnPredicate(
                            column, rangeRegion.getLowerBound())),
                    rangeRegion.getUpperInclusive(),
                    (rangeRegion.getUpperBound() == null ?
                        null :
                        new MemberColumnPredicate(
                            column, rangeRegion.getUpperBound()))),
                baseCube, 
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
        };
    }

    /**
     * Creates a {@link PinSet}.
     *
     * @return a new PinSet
     */
    public abstract PinSet createPinSet();

    /**
     * Generates a SQL query to retrieve a cell value for an evaluation
     * context where some of the dimensions have more than one member.
     *
     * <p>Returns null if the request is unsatisfiable. This would happen, say,
     * if one of the members is null.
     *
     * @param evaluator Provides evaluation context
     * @param aggregationLists List of aggregations, each of which is a set
     *   of members in the same hierarchy which need to be summed together.
     * @return SQL query, or null if request is unsatisfiable
     */

    /**
     * A set of segments which are pinned for a short duration as a result of a
     * cache inquiry.
     */
    public interface PinSet {}
}

// End RolapAggregationManager.java
