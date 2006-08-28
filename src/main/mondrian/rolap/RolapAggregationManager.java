/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
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
import mondrian.rolap.agg.CellRequest;

import java.util.Map;
import java.util.Set;

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
     * @param drillThrough
     * @return Cell request, or null if the requst is unsatisfiable
     */
    static CellRequest makeRequest(
            Member[] members,
            boolean extendedContext,
            final boolean drillThrough) {
        Map mapLevelToColumn;
        CellRequest request;
        if (members[0] instanceof RolapStoredMeasure) {
            RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
            final RolapStar.Measure starMeasure =
                    (RolapStar.Measure) measure.getStarMeasure();
            assert starMeasure != null;
            RolapStar star = starMeasure.getStar();
            request = new CellRequest(starMeasure, extendedContext, drillThrough);
            mapLevelToColumn = star.getMapLevelToColumn(measure.getCube());
        } else {
            return null;
        }

        // Since 'request.extendedContext == false' is a well-worn code path,
        // we have moved the test outside the loop.
        if (request.extendedContext) {
            for (int i = 1; i < members.length; i++) {
                final RolapMember member = (RolapMember) members[i];
                addNonConstrainingColumns(member, mapLevelToColumn, request);

                final RolapLevel level = (RolapLevel) member.getLevel();
                boolean needToReturnNull =
                        level.getLevelReader().constrainRequest(
                                member, mapLevelToColumn, request);
                if (needToReturnNull) {
                    return null;
                }
            }
        } else {
            for (int i = 1; i < members.length; i++) {
                RolapMember member = (RolapMember) members[i];
                final RolapLevel level = (RolapLevel) member.getLevel();
                boolean needToReturnNull =
                        level.getLevelReader().constrainRequest(
                                member, mapLevelToColumn, request);
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
     * @param mapLevelToColumn Level to star column map
     * @param request Cell request
     */
    private static void addNonConstrainingColumns(
            RolapMember member,
            Map mapLevelToColumn,
            CellRequest request) {

        Hierarchy hierarchy = member.getHierarchy();
        Level[] levels = hierarchy.getLevels();
        for (int j = levels.length - 1, depth = member.getLevel().getDepth();
             j > depth; j--) {
            final RolapLevel level = (RolapLevel) levels[j];
            RolapStar.Column column =
                    (RolapStar.Column) mapLevelToColumn.get(level);

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

    public abstract Object getCellFromCache(CellRequest request, Set pinSet);

    public Object getCell(Member[] members) {
        CellRequest request = makeRequest(members, false, false);
        RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
        final RolapStar.Measure starMeasure = (RolapStar.Measure)
                measure.getStarMeasure();

        Util.assertTrue(starMeasure != null);

        RolapStar star = starMeasure.getStar();
        return star.getCell(request);
    }

    // implement CellReader
    public Object get(Evaluator evaluator) {
        final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
        return getCell(rolapEvaluator.getCurrentMembers());
    }

    public abstract String getDrillThroughSQL(CellRequest request);

    public int getMissCount() {
        return 0; // never lies
    }
}

// End RolapAggregationManager.java
