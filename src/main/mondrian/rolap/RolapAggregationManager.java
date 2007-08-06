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

    /**
     * Creates a request to evaluate the cell identified by
     * <code>members</code>. If any of the members is the null member, returns
     * null, since there is no cell. If the measure is calculated, returns
     * null.
     *
     * @param members Set of members which constrain the cell
     * @param extendedContext If true, add non-constraining columns to the
     * query for levels below each current member. This additional context
     * makes the drill-through queries easier for humans to understand.
     * @param drillThrough If true, request returns the list of fact table
     *   rows contributing to the cell
     * @return Cell request, or null if the requst is unsatisfiable
     */
    public static CellRequest makeRequest(
            final Member[] members,
            final boolean extendedContext,
            final boolean drillThrough) {
        if (!(members[0] instanceof RolapStoredMeasure)) {
            return null;
        }

        final RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
        final RolapStar.Measure starMeasure =
            (RolapStar.Measure) measure.getStarMeasure();
        assert starMeasure != null;
        final RolapStar star = starMeasure.getStar();
        final CellRequest request =
            new CellRequest(starMeasure, extendedContext, drillThrough);
        final Map<RolapLevel, RolapStar.Column> levelToColumnMap =
            star.getLevelToColumnMap(measure.getCube());

        // Since 'request.extendedContext == false' is a well-worn code path,
        // we have moved the test outside the loop.
        if (request.extendedContext) {
            for (int i = 1; i < members.length; i++) {
                final RolapMember member = (RolapMember) members[i];
                addNonConstrainingColumns(member, levelToColumnMap, request);

                final RolapLevel level = member.getLevel();
                final boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, levelToColumnMap, request, null);
                if (needToReturnNull) {
                    return null;
                }
            }
        } else {
            for (int i = 1; i < members.length; i++) {
                RolapMember member = (RolapMember) members[i];
                final RolapLevel level = member.getLevel();
                final boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, levelToColumnMap, request, null);
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
     * @param levelToColumnMap Level to star column map
     * @param request Cell request
     */
    private static void addNonConstrainingColumns(
            final RolapMember member,
            final Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            final CellRequest request) {

        final Hierarchy hierarchy = member.getHierarchy();
        final Level[] levels = hierarchy.getLevels();
        for (int j = levels.length - 1, depth = member.getLevel().getDepth();
             j > depth; j--) {
            final RolapLevel level = (RolapLevel) levels[j];
            final RolapStar.Column column = levelToColumnMap.get(level);

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

    protected RolapAggregationManager() {
    }

    /**
     * Returns the value of a cell from an existing aggregation.
     */
    public Object getCellFromCache(final Member[] members) {
        final CellRequest request = makeRequest(members, false, false);
        return (request == null || request.isUnsatisfiable())
            // request out of bounds
            ? Util.nullValue
            : getCellFromCache(request);
    }

    /**
     * Retrieves the value of a cell from the cache.
     *
     * @param request Cell request
     * @pre request != null
     * @return Cell value, or null if cell is not in any aggregation in cache,
     *   or {@link Util#nullValue} if cell's value is null
     */
    public abstract Object getCellFromCache(CellRequest request);

    public abstract Object getCellFromCache(
        CellRequest request,
        PinSet pinSet);

    private Object getCellFromStar(
        Member[] members,
        List<List<Member>> aggregationLists)
    {
        final CellRequest request = makeRequest(members, false, false);
        final RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
        final RolapStar.Measure starMeasure =
            (RolapStar.Measure) measure.getStarMeasure();

        assert starMeasure != null;

        final RolapStar star = starMeasure.getStar();
        return star.getCell(request);
    }

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
        Map<RolapLevel, RolapStar.Column> levelToColumnMap = null;
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
            // TODO: each time this code executes, levelToColumnMap is set.
            // Should there be a 'break' here? Are all of the
            // storedMeasure cubes the same cube? Is the measureList always
            // non-empty so that levelToColumnMap is always set?
            levelToColumnMap =
                star.getLevelToColumnMap(storedMeasure.getCube());
            starMeasureList.add(starMeasure);
        }
        final RolapCacheRegion cacheRegion =
            new RolapCacheRegion(star, starMeasureList);
        if (region instanceof CacheControlImpl.CrossjoinCellRegion) {
            final CacheControlImpl.CrossjoinCellRegion crossjoin =
                (CacheControlImpl.CrossjoinCellRegion) region;
            for (CacheControl.CellRegion component : crossjoin.getComponents()) {
                constrainCacheRegion(cacheRegion, levelToColumnMap, component);
            }
        } else {
            constrainCacheRegion(cacheRegion, levelToColumnMap, region);
        }
        return cacheRegion;
    }

    private static void constrainCacheRegion(
        final RolapCacheRegion cacheRegion,
        final Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        final CacheControl.CellRegion region)
    {
        if (region instanceof CacheControlImpl.MemberCellRegion) {
            final CacheControlImpl.MemberCellRegion memberCellRegion =
                (CacheControlImpl.MemberCellRegion) region;
            final List<Member> memberList = memberCellRegion.getMemberList();
            for (Member member : memberList) {
                final RolapMember rolapMember = (RolapMember) member;
                final RolapLevel level = rolapMember.getLevel();
                final RolapStar.Column column = levelToColumnMap.get(level);
                level.getLevelReader().constrainRegion(
                    new MemberColumnPredicate(column, rolapMember),
                    levelToColumnMap, cacheRegion);
            }
        } else if (region instanceof CacheControlImpl.MemberRangeCellRegion) {
            final CacheControlImpl.MemberRangeCellRegion rangeRegion =
                (CacheControlImpl.MemberRangeCellRegion) region;
            final RolapLevel level = rangeRegion.getLevel();
            final RolapStar.Column column = levelToColumnMap.get(level);
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
                levelToColumnMap, cacheRegion);
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
                Member[] members = evaluator.getMembers();
                return getCellFromCache(members);
            }

            public int getMissCount() {
                return 0; // RolapAggregationManager never lies
            }

            public Object getCompound(
                RolapEvaluator evaluator, List<List<Member>> aggregationLists)
            {
                Member[] members = evaluator.getMembers();
                return getCellFromStar(members, aggregationLists);
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
    public static String generateMultiSql(
        RolapEvaluator evaluator,
        List<List<RolapMember>> aggregationLists)
    {
        final RolapCube cube = (RolapCube) evaluator.getCube();
        final Map<RolapLevel, RolapStar.Column> levelToColumnMap =
            cube.getStar().getLevelToColumnMap(cube);

        // Given an eval context with a compound member:
        //
        //   [Measures].[Customer Count]
        //   [Time].[1997].[Q1]
        //   [Store Type].[All Store Types]
        //   {[Store].[USA].[CA], [Store].[USA].[OR]}
        //   ... everything else 'all'
        //
        // return
        //
        //   SELECT COUNT(DISTINCT cust_id)
        //   FROM sales_fact_1997 AS sales, store, time
        //   WHERE store.store_id = sales.store_id
        //   AND time.time_id = sales.time_id
        //   AND time.year = 1997
        //   AND time.quarter = 'Q1'
        //   AND store.store_state IN ('CA', 'OR')

        // Use a CellRequest to convert members in evaluator into
        // (column, value) pairs; or more accurately, (column, predicate)
        // pairs.
        CellRequest request =
            makeRequest(
                evaluator.getMembers(), false, false);

        // Add in the extra constraints in the aggregation list.
        int[] groupOrdinals = {0, 0, 0};
        for (List<RolapMember> aggregationList : aggregationLists) {
            // The aggregation list is compound if it contains members of
            // different levels.
            int k = 0;
            boolean compound = false;
            final RolapLevel level0 = aggregationList.get(0).getLevel();
            for (RolapMember member : aggregationList) {
                if (k++ > 0) {
                    RolapLevel level = member.getLevel();
                    if (level != level0) {
                        compound = true;
                        if (level.getHierarchy() != level0.getHierarchy()) {
                            throw Util.newInternal(
                                "mixed hierarchies in compound member");
                        }
                    }
                }
            }

            // Compound aggregation lists constrain more than one column.
            // For example,
            //   {[Time].[1997].[Q1], [Time].[1997].[Q3].[7]}
            // generates the SQL predicate
            //  (  (year = 1997 and quarter = 'Q1')
            //  or (year = 1997 and quarter = 'Q3' and month = 7) )
            for (RolapMember member : aggregationList) {
                final boolean unsatisfiable =
                    member.getLevel().getLevelReader().constrainRequest(
                        member, levelToColumnMap, request, groupOrdinals);
                if (unsatisfiable) {
                    return null;
                }
                ++groupOrdinals[1];
                Arrays.fill(groupOrdinals, 2, groupOrdinals.length, 0);
            }
            ++groupOrdinals[0];
            Arrays.fill(groupOrdinals, 1, groupOrdinals.length, 0);
        }

        // Convert the predicates into a SQL query.
        return CompoundQuerySpec.compoundGenerateSql(
            request.getMeasure(),
            request.getConstrainedColumns(),
            request.getValueList(),
            request.getPredicateList());
    }

    /**
     * A set of segments which are pinned for a short duration as a result of a
     * cache inquiry.
     */
    public interface PinSet {}
}

// End RolapAggregationManager.java
