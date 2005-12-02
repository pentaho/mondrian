/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap.agg;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.resource.MondrianResource;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * <code>RolapAggregationManager</code> manages all {@link Aggregation}s
 * in the system. It is a singleton class.
 *
 * @author jhyde
 * @since 30 August, 2001
 * @version $Id$
 **/
public class AggregationManager extends RolapAggregationManager {
    private static final Logger LOGGER =
                    Logger.getLogger(AggregationManager.class);

    private static AggregationManager instance;

    /** Returns or creates the singleton. **/
    public static synchronized AggregationManager instance() {
        if (instance == null) {
            instance = new AggregationManager();
        }
        return instance;
    }

    AggregationManager() {
        super();
    }

    public Logger getLogger() {
        return LOGGER;
    }

    public void loadAggregation(
            RolapStar.Measure[] measures,
            RolapStar.Column[] columns,
            BitKey bitKey,
            ColumnConstraint[][] constraintses,
            Collection pinnedSegments) {
        RolapStar star = measures[0].getStar();
        Aggregation aggregation = star.lookupOrCreateAggregation(bitKey);

        // synchronized access
        synchronized (aggregation) {
            // try to eliminate unneccessary constraints
            // for Oracle: prevent an IN-clause with more than 1000 elements
            constraintses =
                aggregation.optimizeConstraints(columns, constraintses);

            aggregation.load(columns, measures, constraintses, pinnedSegments);
        }
    }



    public Object getCellFromCache(CellRequest request) {
        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getBatchKey());

        if (aggregation == null) {
            // cell is not in any aggregation
            return null;
        }
        Object o = aggregation.get(
                measure, request.getSingleValues(), null);
        if (o != null) {
            return o;
        }
        throw Util.newInternal("not found");
    }

    public Object getCellFromCache(CellRequest request, Set pinSet) {
        Util.assertPrecondition(pinSet != null);

        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getBatchKey());

        if (aggregation == null) {
            // cell is not in any aggregation
            return null;
        } else {
            return aggregation.get(measure, request.getSingleValues(), pinSet);
        }
    }

    public String getDrillThroughSQL(final CellRequest request) {
        DrillThroughQuerySpec spec = new DrillThroughQuerySpec(request);
        String sql = spec.generateSqlQuery();

        if (getLogger().isDebugEnabled()) {
            StringBuffer buf = new StringBuffer(256);
            buf.append("DrillThroughSQL: ");
            buf.append(sql);
            buf.append(Util.nl);
            getLogger().debug(buf.toString());
        }

        return sql;
    }

    /**
     * Generates the query to retrieve the cells for a list of segments.
     */
    public String generateSQL(final Segment[] segments,
                              final BitKey fkBK,
                              final BitKey measureBK,
                              final boolean isDistinct) {

        // Check if using aggregates is enabled.
        if (MondrianProperties.instance().UseAggregates.get()) {
            RolapStar star = segments[0].aggregation.getStar();


            // If there is no distinct count measure, isDistinct == false,
            // then all we want is an AggStar whose BitKey is a superset
            // of the combined measure BitKey and foreign-key/level BitKey.
            //
            // On the other hand, if there is at least one distinct count
            // measure, isDistinct == true, then want is wanted is an AggStar
            // whose measure BitKey is a superset of the measure BitKey,
            // whose level BitKey is an exact match and the aggregate table
            // can NOT have any foreign keys.
            AggStar aggStar = null;
            if (isDistinct) {
                // no foreign keys
                // level key exact match
                // measure superset match

                for (Iterator it = star.getAggStars(); it.hasNext(); ){
                    AggStar as = (AggStar) it.next();
                    if (! as.hasForeignKeys() && 
                            as.select(fkBK, measureBK)) {
                        aggStar = as;
                        break;
                    }
                }

            } else {
                BitKey fullBK = fkBK.or(measureBK);
                // superset match
                aggStar = star.superSetMatch(fullBK);
            }

            if (aggStar != null) {
                // Got a match, hot damn

                if (getLogger().isDebugEnabled()) {
                    StringBuffer buf = new StringBuffer(256);
                    buf.append("MATCH: ");
                    buf.append(star.getFactTable().getAlias());
                    buf.append(" isDistinct=");
                    buf.append(isDistinct);
                    buf.append(Util.nl);
                    buf.append("   foreign=");
                    buf.append(fkBK);
                    buf.append(Util.nl);
                    buf.append("   measure=");
                    buf.append(measureBK);
                    buf.append(Util.nl);
                    buf.append("   aggstar=");
                    buf.append(aggStar.getBitKey());
                    buf.append(Util.nl);
                    buf.append("AggStar=");
                    buf.append(aggStar.getFactTable().getName());
                    buf.append(Util.nl);
                    for (Iterator it = aggStar.getFactTable().getColumns();
                            it.hasNext(); ) {
                        AggStar.Table.Column column =
                            (AggStar.Table.Column) it.next();
                        buf.append("   ");
                        buf.append(column);
                        buf.append(Util.nl);
                    }
                    getLogger().debug(buf.toString());
                }

                AggQuerySpec aggQuerySpec =
                    new AggQuerySpec(aggStar, segments, isDistinct);
                String sql = aggQuerySpec.generateSqlQuery();
                return sql;

            }

            // No match, fall through and use fact table.
        }

        if (getLogger().isDebugEnabled()) {
            RolapStar star = segments[0].aggregation.getStar();

            StringBuffer buf = new StringBuffer(256);
            buf.append("NO MATCH: ");
            buf.append(star.getFactTable().getAlias());
            buf.append(" isDistinct=");
            buf.append(isDistinct);
            buf.append(Util.nl);
            buf.append("   foreign=");
            buf.append(fkBK);
            buf.append(Util.nl);
            buf.append("   measure=");
            buf.append(measureBK);
            buf.append(Util.nl);

            getLogger().debug(buf.toString());
        }


        // Fact table query
        SegmentArrayQuerySpec spec =
            new SegmentArrayQuerySpec(segments, isDistinct);
        String sql = spec.generateSqlQuery();
        return sql;
    }
}

// End RolapAggregationManager.java
