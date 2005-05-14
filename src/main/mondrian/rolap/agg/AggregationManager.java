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

import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.RolapAggregationManager;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.RolapStar;
import mondrian.rolap.BitKey;
import mondrian.rolap.sql.SqlQuery;
import org.apache.log4j.Logger;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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

    public  Logger getLogger() {
        return LOGGER;
    }

    public void loadAggregation(RolapStar.Measure[] measures, 
                                RolapStar.Column[] columns,
                                BitKey bitKey,
                                ColumnConstraint[][] constraintses, 
                                Collection pinnedSegments) {
        RolapStar star = measures[0].getStar();
        Aggregation aggregation = 
            star.lookupOrCreateAggregation(columns, bitKey);
        // try to eliminate unneccessary constraints
        // for Oracle: prevent an IN-clause with more than 1000 elements
        constraintses = aggregation.optimizeConstraints(constraintses);

        aggregation.load(measures, constraintses, pinnedSegments);
    }



    public Object getCellFromCache(CellRequest request) {
        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getBatchKey());

        if (aggregation == null) {
            return null; // cell is not in any aggregation
        }
        Object o = aggregation.get(
                measure, request.getSingleValues(), null);
        if (o != null) {
            return o;
        }
        throw Util.getRes().newInternal("not found");
    }

    public Object getCellFromCache(CellRequest request, Set pinSet) {
        Util.assertPrecondition(pinSet != null);

        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getBatchKey());

        return (aggregation == null)
            // cell is not in any aggregation
            ? null 
            : aggregation.get(measure, request.getSingleValues(), pinSet);
    }

    public String getDrillThroughSQL(final CellRequest request) {
        DrillThroughQuerySpec spec = new DrillThroughQuerySpec(request);
        String sql = spec.generateSqlQuery();
        return sql;
    }

    /**
     * Generates the query to retrieve the cells for a list of segments.
     */
    public String generateSQL(final Segment[] segments, 
                              final BitKey bitKey,
                              final boolean isDistinct) {

        // Check if using aggregates is enabled.
        if (MondrianProperties.instance().getUseAggregates()) {
            RolapStar star = segments[0].aggregation.getStar();


            // Does any aggregate table match the current query
            AggStar aggStar = star.select(bitKey);

            if (aggStar != null) {
                // Got a match, hot damn

                if (getLogger().isDebugEnabled()) {
                    StringBuffer buf = new StringBuffer(256);
                    buf.append("MATCH: ");
                    buf.append('\n'); 
                    buf.append("  bitKey="); 
                    buf.append(bitKey); 
                    buf.append('\n'); 
                    buf.append("  bitkey=");
                    buf.append(aggStar.getBitKey());
                    buf.append('\n'); 
                    buf.append("AggStar=");
                    buf.append(aggStar.getFactTable().getName());
                    buf.append('\n'); 
                    for (Iterator it = aggStar.getFactTable().getColumns(); 
                            it.hasNext(); ) {
                        AggStar.Table.Column column = 
                            (AggStar.Table.Column) it.next();
                        buf.append("   "); 
                        buf.append(column);
                        buf.append('\n'); 
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
            buf.append('\n'); 
            buf.append("  bitKey="); 
            buf.append(bitKey); 
            buf.append('\n'); 
            buf.append(star.getFactTable().getAlias()); 
            buf.append('\n'); 

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
