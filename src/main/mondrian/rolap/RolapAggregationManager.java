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

import mondrian.olap.Evaluator;
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
public abstract class RolapAggregationManager implements CellReader {

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
            Member[] members,
            boolean extendedContext,
            final boolean drillThrough) {
        Map<RolapLevel, RolapStar.Column> levelToColumnMap;
        CellRequest request;
        if (members[0] instanceof RolapStoredMeasure) {
            RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
            final RolapStar.Measure starMeasure =
                    (RolapStar.Measure) measure.getStarMeasure();
            assert starMeasure != null;
            RolapStar star = starMeasure.getStar();
            request = new CellRequest(starMeasure, extendedContext, drillThrough);
            levelToColumnMap = star.getLevelToColumnMap(measure.getCube());
        } else {
            return null;
        }

        // Since 'request.extendedContext == false' is a well-worn code path,
        // we have moved the test outside the loop.
        if (request.extendedContext) {
            for (int i = 1; i < members.length; i++) {
                final RolapMember member = (RolapMember) members[i];
                addNonConstrainingColumns(member, levelToColumnMap, request);

                final RolapLevel level = member.getLevel();
                boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, levelToColumnMap, request);
                if (needToReturnNull) {
                    return null;
                }
            }
        } else {
            for (int i = 1; i < members.length; i++) {
                RolapMember member = (RolapMember) members[i];
                final RolapLevel level = member.getLevel();
                boolean needToReturnNull =
                    level.getLevelReader().constrainRequest(
                        member, levelToColumnMap, request);
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
            RolapMember member,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            CellRequest request) {

        Hierarchy hierarchy = member.getHierarchy();
        Level[] levels = hierarchy.getLevels();
        for (int j = levels.length - 1, depth = member.getLevel().getDepth();
             j > depth; j--) {
            final RolapLevel level = (RolapLevel) levels[j];
            RolapStar.Column column = levelToColumnMap.get(level);

            if (column != null) {
                request.addConstrainedColumn(column, null);
                if (request.extendedContext &&
                        level.getNameExp() != null) {
                    RolapStar.Column nameColumn = column.getNameColumn();
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
    public Object getCellFromCache(Member[] members) {
        CellRequest request = makeRequest(members, false, false);
        return (request == null)
            // request out of bounds
            ? Util.nullValue
            : getCellFromCache(request);
    }

    public abstract Object getCellFromCache(CellRequest request);

    public abstract Object getCellFromCache(
        CellRequest request,
        PinSet pinSet);

    public Object getCell(Member[] members) {
        CellRequest request = makeRequest(members, false, false);
        RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
        final RolapStar.Measure starMeasure = (RolapStar.Measure)
                measure.getStarMeasure();

        assert starMeasure != null;

        RolapStar star = starMeasure.getStar();
        return star.getCell(request);
    }

    // implement CellReader
    public Object get(Evaluator evaluator) {
        final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
        return getCell(rolapEvaluator.getCurrentMembers());
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

    public int getMissCount() {
        return 0; // never lies
    }

    /**
     * Returns an API with which to explicitly manage the contents of the cache.
     *
     * @param pw Print writer, for tracing
     * @return CacheControl API
     */
    public CacheControl getCacheControl(final PrintWriter pw) {
        return new CacheControlImpl() {
            protected void flushNonUnion(CellRegion region) {
                List<RolapStar> starList = getStarList(region);

                // For each of the candidate stars, scan the list of aggregates.
                for (RolapStar star : starList) {
                    star.flush(this, region);
                }
            }

            public void flush(CellRegion region) {
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

            public void trace(String message) {
                if (pw != null) {
                    pw.println(message);
                }
            }
        };
    }

    public static RolapCacheRegion makeCacheRegion(
        RolapStar star,
        CacheControl.CellRegion region)
    {
        final List<Member> measureList = CacheControlImpl.findMeasures(region);
        List<RolapStar.Measure> starMeasureList =
            new ArrayList<RolapStar.Measure>();
        Map<RolapLevel, RolapStar.Column> levelToColumnMap = null;
        for (Member measure : measureList) {
            if (!(measure instanceof RolapStoredMeasure)) {
                continue;
            }
            RolapStoredMeasure storedMeasure = (RolapStoredMeasure) measure;
            final RolapStar.Measure starMeasure =
                (RolapStar.Measure) storedMeasure.getStarMeasure();
            assert starMeasure != null;
            if (star != starMeasure.getStar()) {
                continue;
            }
            levelToColumnMap =
                star.getLevelToColumnMap(storedMeasure.getCube());
            starMeasureList.add(starMeasure);
        }
        final RolapCacheRegion cacheRegion =
            new RolapCacheRegion(star, starMeasureList);
        if (region instanceof CacheControlImpl.CrossjoinCellRegion) {
            CacheControlImpl.CrossjoinCellRegion crossjoin =
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
        RolapCacheRegion cacheRegion,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        CacheControl.CellRegion region)
    {
        if (region instanceof CacheControlImpl.MemberCellRegion) {
            CacheControlImpl.MemberCellRegion memberCellRegion =
                (CacheControlImpl.MemberCellRegion) region;
            final List<Member> memberList = memberCellRegion.getMemberList();
            for (Member member : memberList) {
                RolapMember rolapMember = (RolapMember) member;
                final RolapLevel level = rolapMember.getLevel();
                RolapStar.Column column = levelToColumnMap.get(level);
                level.getLevelReader().constrainRegion(
                    new MemberColumnPredicate(column, rolapMember),
                    levelToColumnMap, cacheRegion);
            }
        } else if (region instanceof CacheControlImpl.MemberRangeCellRegion) {
            CacheControlImpl.MemberRangeCellRegion rangeRegion =
                (CacheControlImpl.MemberRangeCellRegion) region;
            RolapLevel level = rangeRegion.getLevel();
            RolapStar.Column column = levelToColumnMap.get(level);
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
     * Creates a {@link PinSet}.
     *
     * @return a new PinSet
     */
    public abstract PinSet createPinSet();

    /**
     * A set of segments which are pinned for a short duration as a result of a
     * cache inquiry.
     */
    public interface PinSet {}
}

// End RolapAggregationManager.java
