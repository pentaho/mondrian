/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.olap.fun.MondrianEvaluationException;
import mondrian.rolap.agg.AggregationManager;

import org.apache.log4j.Logger;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

/**
 * A <code>RolapResult</code> is the result of running a query.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapResult extends ResultBase {

    private static final Logger LOGGER = Logger.getLogger(ResultBase.class);

    private static final int MAX_AGGREGATION_PASS_COUNT = 5;

    private final RolapEvaluator evaluator;
    private final CellKey point;
    private final Map cellValues;
    private final FastBatchingCellReader batchingReader;
    private final int[] modulos;

    RolapResult(Query query) {
        super(query, new RolapAxis[query.axes.length]);

        this.point = new CellKey(new int[query.axes.length]);
        this.evaluator = new RolapEvaluator(
                            (RolapCube) query.getCube(),
                            (RolapConnection) query.getConnection());
        AggregatingCellReader aggregatingReader = new AggregatingCellReader();
        this.batchingReader = new FastBatchingCellReader(
                                    (RolapCube) query.getCube());
        this.cellValues = new HashMap();

        try {
            for (int i = -1; i < axes.length; i++) {
                QueryAxis axis;
                if (i == -1) {
                    if (query.slicer != null) {
                        axis = new QueryAxis(
                            false,
                            new FunCall(
                                "{}", Syntax.Braces, new Exp[] {query.slicer}
                            ).accept(query.createValidator()),
                            "slicer",
                            QueryAxis.SubtotalVisibility.Undefined);
                    } else {
                        axis = null;
                    }
                } else {
                    axis = query.axes[i];
                }

                int attempt = 0;
                while (true) {
                    evaluator.setCellReader(batchingReader);
                    RolapAxis axisResult = executeAxis(evaluator.push(), axis);
                    evaluator.clearExpResultCache();
                    if (!batchingReader.loadAggregations()) {
                        break;
                    }
                    if (attempt++ > MAX_AGGREGATION_PASS_COUNT) {
                        throw Util.newInternal("Failed to load all aggregations after " +
                                MAX_AGGREGATION_PASS_COUNT +
                                "passes; there's probably a cycle");
                    }
                }

                evaluator.setCellReader(aggregatingReader);
                RolapAxis axisResult = executeAxis(evaluator.push(), axis);
                evaluator.clearExpResultCache();

                if (i == -1) {
                    this.slicerAxis = axisResult;
                    // Use the context created by the slicer for the other
                    // axes.  For example, "select filter([Customers], [Store
                    // Sales] > 100) on columns from Sales where
                    // ([Time].[1998])" should show customers whose 1998 (not
                    // total) purchases exceeded 100.
                    Position position = this.slicerAxis.positions[0];
                    for (int j = 0; j < position.members.length; j++) {
                        evaluator.setContext(position.members[j]);
                    }
                } else {
                    this.axes[i] = axisResult;
                }
            }
            // now, that the axes are evaluated,
            //  make sure, that the total number of positions does
            //  not exceed the result limit
            // throw an exeption, if the total numer of positions gets too large
            int limit = MondrianProperties.instance().getResultLimit();
            if (limit > 0) {
                // result limit exceeded, throw an exception
                long n = 1;
                for (int i = 0; i < axes.length; i++) {
                    n = n*axes[i].positions.length;
                }
                if ( n > limit) {
                    throw MondrianResource.instance().
                        newLimitExceededDuringCrossjoin(
                                new Long(n), new Long(limit));
                }
            }

            // Suppose the result is 4 x 3 x 2, then modulo = {1, 4, 12, 24}.
            //
            // Then the ordinal of cell (3, 2, 1)
            //  = (modulo[0] * 3) + (modulo[1] * 2) + (modulo[2] * 1)
            //  = (1 * 3) + (4 * 2) + (12 * 1)
            //  = 23
            //
            // Reverse calculation:
            // p[0] = (23 % modulo[1]) / modulo[0] = (23 % 4) / 1 = 3
            // p[1] = (23 % modulo[2]) / modulo[1] = (23 % 12) / 4 = 2
            // p[2] = (23 % modulo[3]) / modulo[2] = (23 % 24) / 12 = 1
            this.modulos = new int[axes.length + 1];
            int modulo = modulos[0] = 1;
            for (int i = 0; i < axes.length; i++) {
                modulo *= axes[i].positions.length;
                modulos[i + 1] = modulo;
            }
            executeBody(query);
        } finally {
            evaluator.clearExpResultCache();
        }
        query.getCube().getDimensions();
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    // implement Result
    public Axis[] getAxes() {
        return axes;
    }
    public Cell getCell(int[] pos) {
        if (pos.length != point.ordinals.length) {
            throw Util.newError(
                    "coordinates should have dimension " + point.ordinals.length);
        }
        Object value = cellValues.get(new CellKey(pos));
        if (value == null) {
            value = Util.nullValue;
        }
        return new RolapCell(this, getCellOrdinal(pos), value);
    }

    private RolapAxis executeAxis(Evaluator evaluator, QueryAxis axis) {
        Position[] positions;
        if (axis == null) {
            // Create an axis containing one position with no members (not
            // the same as an empty axis).
            Member[] members = new Member[0];
            RolapPosition position = new RolapPosition(members);
            positions = new Position[] {position};

        } else {
            Exp exp = axis.set;
            evaluator.setNonEmpty(axis.nonEmpty);
            Object value = exp.evaluate(evaluator);
            evaluator.setNonEmpty(false);
            if (value == null) {
                value = Collections.EMPTY_LIST;
            }
            Util.assertTrue(value instanceof List);
            List vector = (List) value;
            positions = new Position[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                Member[] members = null;
                Object o = vector.get(i);
                if (o instanceof Object[]) {
                    Object[] a = (Object[]) o;
                    members = new Member[a.length];
                    for (int j = 0; j < a.length; j++) {
                        members[j] = (Member) a[j];
                    }
                } else {
                    members = new Member[] {(Member) o};
                }
                RolapPosition position = new RolapPosition(members);
                positions[i] = position;
            }
        }
        return new RolapAxis(positions);
    }

    private void executeBody(Query query) {
        // Compute the cells several times. The first time, use a dummy
        // evaluator which collects requests.
        int count = 0;
        while (true) {
            //cellValues = new HashMap();
            cellValues.clear();

            evaluator.setCellReader(this.batchingReader);
            executeStripe(query.axes.length - 1, (RolapEvaluator) evaluator.push());
            evaluator.clearExpResultCache();

            // Retrieve the aggregations collected.
            //
            //
            if (!batchingReader.loadAggregations()) {
                // We got all of the cells we needed, so the result must be
                // correct.
                return;
            }
            if (count++ > MAX_AGGREGATION_PASS_COUNT) {
                throw Util.newInternal("Query required more than " + count + " iterations");
            }
        }
    }

    /**
     * An <code>AggregatingCellReader</code> reads cell values from the
     * {@link RolapAggregationManager}.
     **/
    private static class AggregatingCellReader implements CellReader {
        private final RolapAggregationManager aggregationManager =
            AggregationManager.instance();
        /**
         * Overrides {@link CellReader#get}. Returns <code>null</code> if no
         * aggregation contains the required cell.
         **/
        // implement CellReader
        public Object get(Evaluator evaluator) {
            final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
            return aggregationManager.getCellFromCache(
                rolapEvaluator.getCurrentMembers());
        }
    };

    private void executeStripe(int axis, RolapEvaluator evaluator) {
        if (axis < 0) {
            RolapAxis _axis = (RolapAxis) slicerAxis;
            int count = _axis.positions.length;
            for (int i = 0; i < count; i++) {
                RolapPosition position = (RolapPosition) _axis.positions[i];
                for (int j = 0; j < position.members.length; j++) {
                    evaluator.setContext(position.members[j]);
                }
                Object o;
                try {
                    o = evaluator.evaluateCurrent();
                } catch (MondrianEvaluationException e) {
                    o = e;
                }
                if (o != null && o != RolapUtil.valueNotReadyException) {
                    CellKey key = point.copy();
                    cellValues.put(key, o);
                    // Compute the formatted value, to ensure that any needed
                    // values are in the cache.
                    try {
                        Cell cell = getCell(point.ordinals);
                        Util.discard(cell.getFormattedValue());
                    } catch (MondrianEvaluationException e) {
                        // ignore
                    } catch (Throwable e) {
                        Util.discard(e);
                    }
                }
            }
        } else {
            RolapAxis _axis = (RolapAxis) axes[axis];
            int count = _axis.positions.length;
            for (int i = 0; i < count; i++) {
                point.ordinals[axis] = i;
                RolapPosition position = (RolapPosition) _axis.positions[i];
                for (int j = 0; j < position.members.length; j++) {
                    evaluator.setContext(position.members[j]);
                }
                executeStripe(axis - 1, evaluator);
            }
        }
    }

    /**
     * Converts a cell ordinal to a set of cell coordinates. Converse of
     * {@link #getCellOrdinal}. For example, if this result is 10 x 10 x 10,
     * then cell ordinal 537 has coordinates (5, 3, 7).
     */
    public int[] getCellPos(int cellOrdinal) {
        int[] pos = new int[axes.length];
        for (int j = 0; j < axes.length; j++) {
            pos[j] = (cellOrdinal % modulos[j + 1]) / modulos[j];
        }
        return pos;
    }

    /**
     * Converts a set of cell coordinates to a cell ordinal. Converse of
     * {@link #getCellPos}.
     */
    int getCellOrdinal(int[] pos) {
        int ordinal = 0;
        for (int j = 0; j < axes.length; j++) {
            ordinal += pos[j] * modulos[j];
        }
        return ordinal;
    }

    RolapEvaluator getCellEvaluator(int[] pos) {
        final RolapEvaluator cellEvaluator = (RolapEvaluator) evaluator.push();
        for (int i = 0; i < pos.length; i++) {
            Position position = axes[i].positions[pos[i]];
            for (int j = 0; j < position.members.length; j++) {
                cellEvaluator.setContext(position.members[j]);
            }
        }
        return cellEvaluator;
    }

    Evaluator getEvaluator(int[] pos) {
        // Set up evaluator's context, so that context-dependent format
        // strings work properly.
        Evaluator cellEvaluator = evaluator.push();
        for (int i = -1; i < axes.length; i++) {
            Axis axis;
            int index;
            if (i < 0) {
                axis = slicerAxis;
                index = 0;
            } else {
                axis = axes[i];
                index = pos[i];
            }
            Position position = axis.positions[index];
            for (int j = 0; j < position.members.length; j++) {
                Member member = position.members[j];
                cellEvaluator.setContext(member);
            }
        }
        return cellEvaluator;
    }
}

// End RolapResult.java
