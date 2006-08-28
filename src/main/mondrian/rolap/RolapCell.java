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

/**
 * <code>RolapCell</code> implements {@link mondrian.olap.Cell} within a
 * {@link RolapResult}.
 */
class RolapCell implements Cell {
    private final RolapResult result;
    protected final Object value;
    private final int ordinal;

    RolapCell(RolapResult result, int ordinal, Object value) {
        this.result = result;
        this.value = value;
        this.ordinal = ordinal;
    }

    public Object getValue() {
        return value;
    }

    public String getFormattedValue() {
        final int[] pos = result.getCellPos(ordinal);
        final Evaluator evaluator = result.getEvaluator(pos);
        RolapCube c = (RolapCube) evaluator.getCube();
        Dimension measuresDim = c.getMeasuresHierarchy().getDimension();
        RolapMeasure m = (RolapMeasure) evaluator.getContext(measuresDim);

        CellFormatter cf = m.getFormatter();
        return (cf != null)
            ? cf.formatCell(value)
            : evaluator.format(value);
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
            : aggMan.getDrillThroughSQL(cellRequest);
    }

    /**
     * Returns whether it is possible to drill through this cell.
     * Drill-through is possible if the measure is a stored measure
     * and not possible for calculated measures.
     *
     * @return true if can drill through
     */
    public boolean canDrillThrough() {
        // get current members
        final Member[] currentMembers = getEvaluator().getCurrentMembers();
        // First member is the measure, test if it is stored measure, return
        // true if it is, false if not.
        return (currentMembers[0] instanceof RolapStoredMeasure);
    }

    private RolapEvaluator getEvaluator() {
        final int[] pos = result.getCellPos(ordinal);
        return result.getCellEvaluator(pos);
    }

    public Object getPropertyValue(String propertyName) {
        Property property = Property.lookup(propertyName);
        Object defaultValue = null;
        if (property != null) {
            switch (property.ordinal) {
            case Property.CELL_ORDINAL_ORDINAL:
                return new Integer(ordinal);
            case Property.VALUE_ORDINAL:
                return getValue();
            case Property.FORMAT_STRING_ORDINAL:
                return getEvaluator().getFormatString();
            case Property.FORMATTED_VALUE_ORDINAL:
                return getFormattedValue();
            case Property.FONT_FLAGS_ORDINAL:
                defaultValue = new Integer(0);
                break;
            case Property.SOLVE_ORDER_ORDINAL:
                defaultValue = new Integer(0);
                break;
            default:
                // fall through
            }
        }
        return getEvaluator().getProperty(propertyName, defaultValue);
    }

    public Member getContextMember(Dimension dimension) {
        return result.getMember(result.getCellPos(ordinal), dimension);
    }
}

// End RolapCell.java
