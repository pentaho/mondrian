/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde
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
    protected final int[] pos;
    protected RolapResult.CellInfo ci;

    RolapCell(RolapResult result, int[] pos, RolapResult.CellInfo ci) {
        this.result = result;
        this.pos = pos;
        this.ci = ci;
    }

    public Object getValue() {
        return ci.value;
    }

    public String getCachedFormatString() {
        return ci.formatString;
    }

    public String getFormattedValue() {
        return ci.getFormatValue();
    }

    public boolean isNull() {
        return (ci.value == Util.nullValue);
    }

    public boolean isError() {
        return (ci.value instanceof Throwable);
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
        final Member[] currentMembers = getMembers();
        CellRequest cellRequest = RolapAggregationManager.makeRequest(
                currentMembers, extendedContext, true);
        return (cellRequest == null)
            ? null
            : aggMan.getDrillThroughSql(cellRequest, false);
    }


    public int getDrillThroughCount() {
        RolapAggregationManager aggMan = AggregationManager.instance();
        final Member[] currentMembers = getMembers();
        CellRequest cellRequest = RolapAggregationManager.makeRequest(
            currentMembers, false, true);
        if (cellRequest == null) {
            return -1;
        }
        RolapConnection connection =
            (RolapConnection) result.getQuery().getConnection();
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
        if (((RolapCube) result.getCube()).isVirtual()) {
            return false;
        }
        // get current members
        final Member[] currentMembers = getMembers();
        // First member is the measure, test if it is stored measure, return
        // true if it is, false if not.
        return (currentMembers[0] instanceof RolapStoredMeasure);
    }

    private RolapEvaluator getEvaluator() {
        return result.getCellEvaluator(pos);
    }
    private Member[] getMembers() {
        return result.getCellMembers(pos);
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
                if (ci.formatString == null) {
                    ci.formatString = getEvaluator().getFormatString();
                }
                return ci.formatString;
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
