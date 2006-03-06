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
import mondrian.calc.Calc;
import mondrian.olap.*;
import mondrian.olap.fun.MondrianEvaluationException;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.AggregationManager;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * A <code>RolapResult</code> is the result of running a query.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapResult extends ResultBase {

    private static final Logger LOGGER = Logger.getLogger(ResultBase.class);

    private final RolapEvaluator evaluator;
    private final CellKey point;
    private final Map cellValues;
    private final FastBatchingCellReader batchingReader;
    AggregatingCellReader aggregatingReader = new AggregatingCellReader();
    private final int[] modulos;
    private final int maxEvalDepth =
            MondrianProperties.instance().MaxEvalDepth.get();

    RolapResult(Query query, boolean execute) {
        super(query, new RolapAxis[query.axes.length]);

        this.point = new CellKey(new int[query.axes.length]);
        final int expDeps = MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            this.evaluator = new RolapDependencyTestingEvaluator(this, expDeps);
        } else {
            final RolapEvaluator.RolapEvaluatorRoot root =
                    new RolapResultEvaluatorRoot(this);
            this.evaluator = new RolapEvaluator(root);
        }
        RolapCube rcube = (RolapCube) query.getCube();
        this.batchingReader = new FastBatchingCellReader(rcube);
        this.cellValues = new HashMap();
        this.modulos = new int[axes.length + 1];
        if (!execute) {
            return;
        }

        try {
            for (int i = -1; i < axes.length; i++) {
                QueryAxis axis;
                final Calc calc;
                if (i == -1) {
                    axis = query.slicerAxis;
                    calc = query.slicerCalc;
                } else {
                    axis = query.axes[i];
                    calc = query.axisCalcs[i];
                }

                int attempt = 0;
                while (true) {
                    evaluator.setCellReader(batchingReader);
                    RolapAxis axisResult =
                            executeAxis(evaluator.push(), axis, calc);
                    Util.discard(axisResult);
                    evaluator.clearExpResultCache();
                    if (!batchingReader.loadAggregations()) {
                        break;
                    }
                    if (attempt++ > maxEvalDepth) {
                        throw Util.newInternal("Failed to load all aggregations after " +
                                maxEvalDepth +
                                "passes; there's probably a cycle");
                    }
                }

                evaluator.setCellReader(aggregatingReader);
                RolapAxis axisResult = executeAxis(evaluator.push(), axis, calc);
                evaluator.clearExpResultCache();

                if (i == -1) {
                    this.slicerAxis = axisResult;
                    // Use the context created by the slicer for the other
                    // axes.  For example, "select filter([Customers], [Store
                    // Sales] > 100) on columns from Sales where
                    // ([Time].[1998])" should show customers whose 1998 (not
                    // total) purchases exceeded 100.
                    switch (this.slicerAxis.positions.length) {
                    case 0:
                        throw MondrianResource.instance().EmptySlicer.ex();
                    case 1:
                        break;
                    default:
                        throw MondrianResource.instance().CompoundSlicer.ex();
                    }
                    Position position = this.slicerAxis.positions[0];
                    for (int j = 0; j < position.members.length; j++) {
                        Member member = position.members[j];
                        if (member == null) {
                            throw MondrianResource.instance().EmptySlicer.ex();
                        }
                        evaluator.setContext(member);
                    }
                } else {
                    this.axes[i] = axisResult;
                }
            }
            // Now that the axes are evaluated, make sure that the number of
            // cells does not exceed the result limit.
            int limit = MondrianProperties.instance().ResultLimit.get();
            if (limit > 0) {
                // result limit exceeded, throw an exception
                long n = 1;
                for (int i = 0; i < axes.length; i++) {
                    n = n * axes[i].positions.length;
                }
                if (n > limit) {
                    throw MondrianResource.instance().
                        LimitExceededDuringCrossjoin.ex(
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
            int modulo = modulos[0] = 1;
            for (int i = 0; i < axes.length; i++) {
                modulo *= axes[i].positions.length;
                modulos[i + 1] = modulo;
            }
            executeBody(query);
        } finally {
            evaluator.clearExpResultCache();
        }
        // RME : what is this doing???
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

    private RolapAxis executeAxis(
            Evaluator evaluator, QueryAxis axis, Calc axisCalc) {
        Position[] positions;
        if (axis == null) {
            // Create an axis containing one position with no members (not
            // the same as an empty axis).
            Member[] members = new Member[0];
            RolapPosition position = new RolapPosition(members);
            positions = new Position[] {position};

        } else {
            evaluator.setNonEmpty(axis.nonEmpty);
            Object value = axisCalc.evaluate(evaluator);
            evaluator.setNonEmpty(false);
            if (value == null) {
                value = Collections.EMPTY_LIST;
            }
            Util.assertTrue(value instanceof List);
            List list = (List) value;
            positions = new Position[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Member[] members = null;
                Object o = list.get(i);
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
        try {
            // Compute the cells several times. The first time, use a dummy
            // evaluator which collects requests.
            int count = 0;
            while (true) {
                cellValues.clear();

                evaluator.setCellReader(this.batchingReader);
                executeStripe(query.axes.length - 1,
                    (RolapEvaluator) evaluator.push());
                evaluator.clearExpResultCache();

                // Retrieve the aggregations collected.
                //
                if (!batchingReader.loadAggregations()) {
                    // We got all of the cells we needed, so the result must be
                    // correct.
                    return;
                }
                if (count++ > maxEvalDepth) {
                    if (evaluator instanceof RolapDependencyTestingEvaluator) {
                        // The dependency testing evaluator can trigger new
                        // requests every cycle. So let is run as normal for
                        // the first N times, then run it disabled.
                        ((RolapDependencyTestingEvaluator.DteRoot) evaluator.root).disabled = true;
                        if (count > maxEvalDepth * 2) {
                            throw Util.newInternal("Query required more than "
                                + count + " iterations");
                        }
                    } else {
                        throw Util.newInternal("Query required more than "
                            + count + " iterations");
                    }
                }
            }
        } finally {
            RolapCube cube = (RolapCube) query.getCube();
            cube.clearCache();
        }
    }

    boolean isDirty() {
        return batchingReader.isDirty();
    }

    private Object evaluateExp(Calc calc, Evaluator evaluator) {
        int attempt = 0;
        boolean dirty = batchingReader.isDirty();
        while (true) {
            RolapEvaluator ev = (RolapEvaluator) evaluator.push();

            ev.setCellReader(batchingReader);
            Object preliminaryValue = calc.evaluate(ev);
            Util.discard(preliminaryValue);
            if (!batchingReader.loadAggregations()) {
                break;
            }
            if (attempt++ > maxEvalDepth) {
                throw Util.newInternal(
                        "Failed to load all aggregations after " +
                        maxEvalDepth + "passes; there's probably a cycle");
            }
        }

        // If there were pending reads when we entered, some of the other
        // expressions may have been evaluated incorrectly. Set the reaader's
        // 'dirty' flag so that the caller knows that it must re-evaluate them.
        if (dirty) {
            batchingReader.setDirty(true);
        }
        RolapEvaluator ev = (RolapEvaluator) evaluator.push();
        ev.setCellReader(aggregatingReader);
        Object value = calc.evaluate(ev);
        return value;
    }

    /**
     * An <code>AggregatingCellReader</code> reads cell values from the
     * {@link RolapAggregationManager}.
     **/
    private static class AggregatingCellReader implements CellReader {
        private final RolapAggregationManager aggMan =
            AggregationManager.instance();

        // implement CellReader
        public Object get(Evaluator evaluator) {
            final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
            return aggMan.getCellFromCache(rolapEvaluator.getCurrentMembers());
        }

        public int getMissCount() {
            return aggMan.getMissCount();
        }
    }

    private void executeStripe(int axisOrdinal, RolapEvaluator evaluator) {
        if (axisOrdinal < 0) {
            RolapAxis axis = (RolapAxis) slicerAxis;
            int count = axis.positions.length;
            for (int i = 0; i < count; i++) {
                RolapPosition position = (RolapPosition) axis.positions[i];
                for (int j = 0; j < position.members.length; j++) {
                    evaluator.setContext(position.members[j]);
                }
                Object o;
                try {
                    o = evaluator.evaluateCurrent();
                } catch (MondrianEvaluationException e) {
                    o = e;
                }
                if (o == null ||
                        o == Util.nullValue ||
                        o == RolapUtil.valueNotReadyException) {
                    continue;
                }
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
        } else {
            RolapAxis axis = (RolapAxis) axes[axisOrdinal];
            int count = axis.positions.length;
            for (int i = 0; i < count; i++) {
                point.ordinals[axisOrdinal] = i;
                RolapPosition position = (RolapPosition) axis.positions[i];
                for (int j = 0; j < position.members.length; j++) {
                    evaluator.setContext(position.members[j]);
                }
                executeStripe(axisOrdinal - 1, evaluator);
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

    Evaluator getRootEvaluator()
    {
        return evaluator;
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

    /**
     * Extension to {@link RolapEvaluator.RolapEvaluatorRoot} which is capable
     * of evaluating named sets.<p/>
     *
     * A given set is only evaluated once each time a query is executed; the
     * result is added to the {@link #namedSetValues} cache on first execution
     * and re-used.<p/>
     *
     * Named sets are always evaluated in the context of the slicer.<p/>
     */
    protected static class RolapResultEvaluatorRoot
            extends RolapEvaluator.RolapEvaluatorRoot {
        /**
         * Maps the names of sets to their values. Populated on demand.
         */
        private final Map namedSetValues = new HashMap();

        /**
         * Evaluator containing context resulting from evaluating the slicer.
         */
        private RolapEvaluator slicerEvaluator;
        private final RolapResult result;

        public RolapResultEvaluatorRoot(RolapResult result) {
            super(result.query);
            this.result = result;
        }

        protected void init(Evaluator evaluator) {
            slicerEvaluator = (RolapEvaluator) evaluator;
        }

        protected Object evaluateNamedSet(String name, Exp exp) {
            Object value = namedSetValues.get(name);
            if (value == null) {
                final RolapEvaluator.RolapEvaluatorRoot root =
                        ((RolapEvaluator) slicerEvaluator).root;
                final Calc calc = root.getCompiled(exp, false);
                value = result.evaluateExp(calc, slicerEvaluator.push());
                namedSetValues.put(name, value);
            }
            return value;
        }
    }

}

// End RolapResult.java
