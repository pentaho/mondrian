/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;

import java.sql.*;

/**
 * <code>RolapCell</code> implements {@link mondrian.olap.Cell} within a
 * {@link RolapResult}.
 */
class RolapCell implements Cell {
    private final RolapResult result;
    protected final Object value;
    protected String cachedFormatString;
    protected final int[] pos;

    RolapCell(RolapResult result, int[] pos, Object value) {
        this(result, pos, value, null);
    }
    
    RolapCell(RolapResult result, int[] pos, Object value, String cachedFormatString) {
        this.result = result;
        this.value = value;
        this.pos = pos;
        this.cachedFormatString = cachedFormatString;
    }

    public Object getValue() {
        return value;
    }
    
    public String getCachedFormatString() {
        return cachedFormatString;
    }
    
    public String getFormattedValue() {
        final Evaluator evaluator = result.getEvaluator(pos);
        RolapCube c = (RolapCube) evaluator.getCube();
        Dimension measuresDim = c.getMeasuresHierarchy().getDimension();
        RolapMeasure m = (RolapMeasure) evaluator.getContext(measuresDim);

        CellFormatter cf = m.getFormatter();
        
        if (cf != null) {
            return cf.formatCell(value);
        } else {                                
            if (cachedFormatString == null) {
                cachedFormatString = evaluator.getFormatString();
            }
            return evaluator.format(value, cachedFormatString);    
        } 
    }
    
    public boolean isNull() {
        return (value == Util.nullValue);
    }
    
    public boolean isError() {
        return (value instanceof Throwable);
    }

    /**
     * Create an sql query that, when executed, will return the drill through
     * data for this cell. If the parameter extendedContext is true, then the
     * query will include all the levels (i.e. columns) of non-constraining
     * members (i.e. members which are at the "All" level).
     * If the parameter extendedContext is false, the query will exclude
     * the levels (coulmns) of non-constraining members.
     */
    public String getDrillThroughSQL(boolean extendedContext) {
        RolapAggregationManager aggMan = AggregationManager.instance();

        final RolapEvaluator evaluator = getEvaluator();
        final Member[] currentMembers = evaluator.getCurrentMembers();
        CellRequest cellRequest = RolapAggregationManager.makeRequest(
                currentMembers, extendedContext, true);
        return (cellRequest == null)
            ? null
            : aggMan.getDrillThroughSql(cellRequest, false);
    }


    public int getDrillThroughCount() {
        RolapAggregationManager aggMan = AggregationManager.instance();

        final RolapEvaluator evaluator = getEvaluator();
        final Member[] currentMembers = evaluator.getCurrentMembers();
        CellRequest cellRequest = RolapAggregationManager.makeRequest(
            currentMembers, false, true);
        if (cellRequest == null) {
            return -1;
        }
        RolapConnection connection =
            (RolapConnection) evaluator.getQuery().getConnection();
        java.sql.Connection jdbcConnection = null;
        ResultSet rs = null;
        final String sql = aggMan.getDrillThroughSql(cellRequest, true);
        try {
            jdbcConnection = connection.getDataSource().getConnection();

            rs = RolapUtil.executeQuery(
                jdbcConnection,
                sql,
                "RolapCell.getDrillThroughCount");
            rs.next();
            int count = rs.getInt(1);
            rs.close();
            return count;
        } catch (SQLException e) {
            throw Util.newError(
                e,
                "Error while counting drill-through, SQL ='" + sql + "'");
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException ignored) {
            }
            try {
                if (jdbcConnection != null && !jdbcConnection.isClosed()) {
                    jdbcConnection.close();
                }
            } catch (SQLException ignored) {
            }
        }
    }

    /**
     * Returns whether it is possible to drill through this cell.
     * Drill-through is possible if the measure is a stored measure
     * and not possible for calculated measures.
     *
     * @return true if can drill through
     */
    public boolean canDrillThrough() {
        // Cannot drill through query based on virtual cube.
        if (((RolapCube) getEvaluator().getCube()).isVirtual()) {
            return false;
        }
        // get current members
        final Member[] currentMembers = getEvaluator().getCurrentMembers();
        // First member is the measure, test if it is stored measure, return
        // true if it is, false if not.
        return (currentMembers[0] instanceof RolapStoredMeasure);
    }

    private RolapEvaluator getEvaluator() {
        return result.getCellEvaluator(pos);
    }

    public Object getPropertyValue(String propertyName) {
        Property property = Property.lookup(propertyName, true);
        Object defaultValue = null;
        if (property != null) {
            switch (property.ordinal) {
            case Property.CELL_ORDINAL_ORDINAL:
                return result.getCellOrdinal(pos);
            case Property.VALUE_ORDINAL:
                return getValue();
            case Property.FORMAT_STRING_ORDINAL:
                return getEvaluator().getFormatString();
            case Property.FORMATTED_VALUE_ORDINAL:
                return getFormattedValue();
            case Property.FONT_FLAGS_ORDINAL:
                defaultValue = 0;
                break;
            case Property.SOLVE_ORDER_ORDINAL:
                defaultValue = 0;
                break;
            default:
                // fall through
            }
        }
        return getEvaluator().getProperty(propertyName, defaultValue);
    }

    public Member getContextMember(Dimension dimension) {
        return result.getMember(pos, dimension);
    }
}

// End RolapCell.java
