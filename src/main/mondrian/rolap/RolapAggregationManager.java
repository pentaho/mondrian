/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
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
 **/
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
     **/
    static CellRequest makeRequest(Member[] members,
            boolean extendedContext)
    {
        boolean showNames = extendedContext;
        if (!(members[0] instanceof RolapStoredMeasure)) {
            return null;
        }
        RolapStoredMeasure measure = (RolapStoredMeasure) members[0];
        final RolapStar.Measure starMeasure = (RolapStar.Measure)
                measure.starMeasure;
        Util.assertTrue(starMeasure != null);
        RolapStar star = starMeasure.table.star;
        CellRequest request = new CellRequest(starMeasure);
        Map mapLevelToColumn = star.getMapLevelToColumn(measure.cube);
        Map mapLevelNameToColumn = star.getMapLevelToNameColumn(measure.cube);
        for (int i = 1; i < members.length; i++) {
            Member member = members[i];
            RolapLevel previousLevel = null;
            Hierarchy hierarchy = member.getHierarchy();
            if (extendedContext) {
                // Add the key columns as non-constraining columns. For
                // example, if they asked for [Gender].[M], [Store].[USA].[CA]
                // then the following levels are in play:
                //   Gender = 'M'
                //   Marital Status not constraining
                //   Nation = 'USA'
                //   State = 'CA'
                //   City not constraining
                //
                // Note that [Marital Status] column is present by virtue of
                // the implicit [Marital Status].[All] member. Hence the SQL
                //
                //   select [Marital Status], [City]
                //   from [Star]
                //   where [Gender] = 'M'
                //   and [Nation] = 'USA'
                //   and [State] = 'CA'
                //

                Level[] levels = hierarchy.getLevels();
                for (int j = levels.length - 1,
                        depth = member.getLevel().getDepth(); j > depth; j--) {
                    final RolapLevel level = (RolapLevel) levels[j];
                    RolapStar.Column column = (RolapStar.Column)
                        mapLevelToColumn.get(level);
                    if (column != null) {
                        request.addConstrainedColumn(column, null);
                        if (showNames && level.nameExp != null) {
                            RolapStar.Column nameColumn = (RolapStar.Column)
                                mapLevelNameToColumn.get(level);
                            Util.assertTrue(nameColumn != null);
                            request.addConstrainedColumn(nameColumn, null);
                        }
                    }
                }
            }
            for (Member m = member; m != null; m = m.getParentMember()) {
                RolapMember rm = (RolapMember) m;
                if (rm.getKey() == null) {
                    if (m == m.getHierarchy().getNullMember()) {
                        // cannot form a request if one of the members is null
                        return null;
                    } else if (m.isAll()) {
                        continue;
                    } else {
                        throw Util.getRes().newInternal("why is key null?");
                    }
                }
                RolapLevel level = (RolapLevel) m.getLevel();
                if (level == previousLevel) {
                    // We are looking at a parent in a parent-child hierarchy,
                    // for example, we have moved from Fred to Fred's boss,
                    // Wilma. We don't want to include Wilma's key in the
                    // request.
                    continue;
                }
                previousLevel = level;

                // Replace a parent/child level by its closed equivalent, when
                // available; this is always valid, and improves performance by
                // enabling the database to compute aggregates.
                if (level.hasClosedPeer()) {
                    if (member.getDataMember() == null) {
                        // Member has no data member because it IS the data
                        // member of a parent-child hierarchy member. Leave
                        // it be. We don't want to aggregate.
                    } else {
                        level = level.getClosedPeer();
                        final RolapMember allMember = (RolapMember)
                            hierarchy.getDefaultMember();
                        assert allMember.isAll();
                        member = new RolapMember(allMember, level,
                            ((RolapMember) member).getKey());
                    }
                }
                RolapStar.Column column =
                    (RolapStar.Column) mapLevelToColumn.get(level);
                if (column == null) {
                    // This hierarchy is not one which qualifies the starMeasure
                    // (this happens in virtual cubes). The starMeasure only has
                    // a value for the 'all' member of the hierarchy.
                    return null;
                }
                // use the member as constraint, this will give us some
                //  optimization potential
                request.addConstrainedColumn(column, m);
                if (showNames && level.nameExp != null) {
                    RolapStar.Column nameColumn = (RolapStar.Column)
                        mapLevelNameToColumn.get(level);
                    Util.assertTrue(nameColumn != null);
                    request.addConstrainedColumn(nameColumn, null);
                }
            }
        }
        return request;
    }

    protected RolapAggregationManager() {
    }

    /**
     * Returns the value of a cell from an existing aggregation.
     **/
    public Object getCellFromCache(Member[] members)
    {
        CellRequest request = makeRequest(members, false);
        if (request == null) {
            return Util.nullValue; // request out of bounds
        }
        return getCellFromCache(request);
    }

    public abstract Object getCellFromCache(CellRequest request);

    public abstract Object getCellFromCache(CellRequest request, Set pinSet);

    public Object getCell(Member[] members)
    {
        CellRequest request = makeRequest(members, false);
        RolapMeasure measure = (RolapMeasure) members[0];
        final RolapStar.Measure starMeasure = (RolapStar.Measure)
                measure.starMeasure;
        Util.assertTrue(starMeasure != null);
        RolapStar star = starMeasure.table.star;
        return star.getCell(request);
    }

    // implement CellReader
    public Object get(Evaluator evaluator) {
        final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
        return getCell(rolapEvaluator.getCurrentMembers());
    }

    public abstract String getDrillThroughSQL(CellRequest request);

}

// End RolapAggregationManager.java
