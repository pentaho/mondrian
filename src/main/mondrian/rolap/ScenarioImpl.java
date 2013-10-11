/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.calc.DummyExp;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.type.ScalarType;

import org.olap4j.AllocationPolicy;
import org.olap4j.Scenario;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link org.olap4j.Scenario}.
 *
 * @author jhyde
 * @since 24 April, 2009
 */
public final class ScenarioImpl implements Scenario {

    private final int id;

    private final List<WritebackCell> writebackCells =
        new ArrayList<WritebackCell>();

    private RolapMember member;

    private static int nextId;

    /**
     * Creates a ScenarioImpl.
     */
    public ScenarioImpl() {
        id = nextId++;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ScenarioImpl
            && id == ((ScenarioImpl) obj).id;
    }

    @Override
    public String toString() {
        return "scenario #" + id;
    }

    /**
     * Sets the value of a cell.
     *
     * @param connection Connection (not currently used)
     * @param members Coordinates of cell
     * @param newValue New value
     * @param currentValue Current value
     * @param allocationPolicy Allocation policy
     * @param allocationArgs Additional arguments of allocation policy
     */
    public void setCellValue(
        Connection connection,
        List<RolapMember> members,
        double newValue,
        double currentValue,
        AllocationPolicy allocationPolicy,
        Object[] allocationArgs)
    {
        Util.discard(connection); // for future use
        assert allocationPolicy != null;
        assert allocationArgs != null;
        switch (allocationPolicy) {
        case EQUAL_ALLOCATION:
        case EQUAL_INCREMENT:
            if (allocationArgs.length != 0) {
                throw Util.newError(
                    "Allocation policy " + allocationPolicy
                    + " takes 0 arguments; " + allocationArgs.length
                    + " were supplied");
            }
            break;
        default:
            throw Util.newError(
                "Allocation policy " + allocationPolicy + " is not supported");
        }

        // Compute the set of columns which are constrained by the cell's
        // coordinates.
        //
        // NOTE: This code is very similar to code in
        // RolapAggregationManager.makeCellRequest. Consider creating a
        // CellRequest then mining it. It will work better in the presence of
        // calculated members, compound members, parent-child hierarchies,
        // hierarchies whose default member is not the 'all' member, and so
        // forth.
        final RolapStoredMeasure measure = (RolapStoredMeasure) members.get(0);
        final RolapCube baseCube = measure.getCube();
        final RolapStar.Measure starMeasure = measure.getStarMeasure();
        assert starMeasure != null;
        int starColumnCount = starMeasure.getStar().getColumnCount();
        final BitKey constrainedColumnsBitKey =
            BitKey.Factory.makeBitKey(starColumnCount);
        Object[] keyValues = new Object[starColumnCount];
        for (int i = 1; i < members.size(); i++) {
            final RolapMember member = members.get(i);
            final List<Comparable> keyList = member.getKeyAsList();
            int j = 0;
            for (RolapSchema.PhysColumn physColumn
                : member.getLevel().getAttribute().getKeyList())
            {
                final RolapStar.Column column =
                    measure.getMeasureGroup().getRolapStarColumn(
                        member.getDimension(),
                        physColumn);
                if (column != null) {
                    final int bitPos = column.getBitPosition();
                    keyValues[bitPos] = keyList.get(j);
                    constrainedColumnsBitKey.set(bitPos);
                }
                ++j;
            }
        }

        // Squish the values down. We want the compactKeyValues[i] to correspond
        // to the i'th set bit in the key. This is the same format used by
        // CellRequest.
        Object[] compactKeyValues =
            new Object[constrainedColumnsBitKey.cardinality()];
        int k = 0;
        for (int bitPos : constrainedColumnsBitKey) {
            compactKeyValues[k++] = keyValues[bitPos];
        }

        // Record the override.
        //
        // TODO: add a mechanism for persisting the overrides to a file.
        //
        // FIXME: make thread-safe
        writebackCells.add(
            new WritebackCell(
                baseCube,
                new ArrayList<RolapMember>(members),
                constrainedColumnsBitKey,
                compactKeyValues,
                newValue,
                currentValue,
                allocationPolicy));
    }

    public String getId() {
        return Integer.toString(id);
    }

    /**
     * Returns the scenario inside a calculated member in the scenario
     * dimension. For example, applied to [Scenario].[1], returns the Scenario
     * object representing scenario #1.
     *
     * @param member Wrapper member
     * @return Wrapped scenario
     */
    static Scenario forMember(final RolapMember member) {
        if (isScenario(member.getHierarchy())) {
            final Formula formula = ((RolapCalculatedMember) member)
                .getFormula();
            final ResolvedFunCall resolvedFunCall =
                (ResolvedFunCall) formula.getExpression();
            final Calc calc = resolvedFunCall.getFunDef()
                .compileCall(null, null);
            return ((ScenarioCalc) calc).getScenario();
        } else {
            return null;
        }
    }

    /**
     * Registers this Scenario with a Schema, creating a calculated member
     * [Scenario].[{id}] for each cube that has writeback enabled. (Currently
     * a cube has writeback enabled iff it has a dimension called "Scenario".)
     *
     * @param schema Schema
     */
    void register(RolapSchema schema) {
        // Add a value to the [Scenario] dimension of every cube that has
        // writeback enabled.
        for (RolapCube cube : schema.getCubeList()) {
            if (cube.scenarioHierarchy != null) {
                member =
                    cube.createCalculatedMember(
                        cube.scenarioHierarchy,
                        getId() + "",
                        new ScenarioCalc(this));
                assert member != null;
            }
        }
    }

    /**
     * Returns whether a hierarchy is the [Scenario] hierarchy.
     *
     * <p>TODO: use a flag
     *
     * @param hierarchy Hierarchy
     * @return Whether hierarchy is the scenario hierarchy
     */
    public static boolean isScenario(Hierarchy hierarchy) {
        return hierarchy.getName().equals("Scenario");
    }

    /**
     * Returns the number of atomic cells that contribute to the current
     * cell.
     *
     * <p>If the current cell is based on a calculated measure, returns null.
     *
     * @param evaluator Evaluator
     * @return Number of atomic cells in the current cell
     */
    private static double evaluateAtomicCellCount(RolapEvaluator evaluator) {
        final Member measure = evaluator.getMembers()[0];
        if (!(measure instanceof RolapStoredMeasure)) {
            return FunUtil.DoubleNull;
        }
        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setContext(
                evaluator.getMeasureGroup().getAtomicCellCountMeasure());
            final Object o = evaluator.evaluateCurrent();
            return ((Number) o).doubleValue();
        } finally {
            evaluator.restore(savepoint);
        }
    }

    /**
     * Computes the number of atomic cells in a cell identified by a list
     * of members.
     *
     * <p>The method may be expensive. If the value is not in the cache,
     * computes it immediately using a database access. It uses an aggregate
     * table if applicable, and puts the value into the cache.
     *
     * <p>Compare with {@link #evaluateAtomicCellCount(RolapEvaluator)}, which
     * gets the value from the cache but may lie (and generate a cache miss) if
     * the value is not present.
     *
     * @param cube Cube
     * @param memberList Coordinate members of cell
     * @return Number of atomic cells in cell
     */
    private static double computeAtomicCellCount(
        RolapCube cube, List<RolapMember> memberList)
    {
        // Implementation generates and executes a recursive MDX query. This
        // may not be the most efficient implementation, but achieves the
        // design goals of (a) immediacy, (b) cache use, (c) aggregate table
        // use.
        final StringBuilder buf = new StringBuilder();
        buf.append("select from ");
        buf.append(cube.getUniqueName());
        int k = 0;
        for (Member member : memberList) {
            if (member.isMeasure()) {
                member = ((RolapStoredMeasure) member).getMeasureGroup()
                    .factCountMeasure;
                assert member != null
                    : "fact count measure is required for writeback cubes";
            }
            if (!member.equals(member.getHierarchy().getDefaultMember())) {
                if (k++ > 0) {
                    buf.append(", ");
                } else {
                    buf.append(" where (");
                }
                buf.append(member.getUniqueName());
            }
        }
        if (k > 0) {
            buf.append(")");
        }
        final String mdx = buf.toString();
        final RolapConnection connection =
            cube.getSchema().getInternalConnection();
        final Query query = connection.parseQuery(mdx);
        final Result result = connection.execute(query);
        final Object o = result.getCell(new int[0]).getValue();
        return o instanceof Number
            ? ((Number) o).doubleValue()
            : 0d;
    }

    /**
     * Returns the member of the [Scenario] dimension that represents this
     * scenario. Including that member in the slicer will automatically use
     * this scenario.
     *
     * <p>The result is not null, provided that {@link #register(RolapSchema)}
     * has been called.
     *
     * @return Scenario member
     */
    public RolapMember getMember() {
        return member;
    }

    /**
     * Created by a call to
     * {@link org.olap4j.Cell#setValue(Object, org.olap4j.AllocationPolicy, Object...)},
     * records that a cell's value has been changed.
     *
     * <p>From this, other cell values can be modified as they are read into
     * cache. Only the cells specifically modified by the client have a
     * {@code CellValueOverride}.
     *
     * <p>In future, a {@link ScenarioImpl} could be persisted by
     * serializing all {@code WritebackCell}s to a file.
     */
    private static class WritebackCell {
        private final double newValue;
        private final double currentValue;
        private final AllocationPolicy allocationPolicy;
        private Member[] membersByOrdinal;
        private final double atomicCellCount;

        /**
         * Creates a WritebackCell.
         *
         * @param cube Cube
         * @param members Members that form context
         * @param constrainedColumnsBitKey Bitmap of columns which have values
         * @param keyValues List of values, by bit position
         * @param newValue New value
         * @param currentValue Current value
         * @param allocationPolicy Allocation policy
         */
        WritebackCell(
            RolapCube cube,
            List<RolapMember> members,
            BitKey constrainedColumnsBitKey,
            Object[] keyValues,
            double newValue,
            double currentValue,
            AllocationPolicy allocationPolicy)
        {
            assert keyValues.length == constrainedColumnsBitKey.cardinality();
            Util.discard(cube); // not used currently
            Util.discard(constrainedColumnsBitKey); // not used currently
            Util.discard(keyValues); // not used currently
            this.newValue = newValue;
            this.currentValue = currentValue;
            this.allocationPolicy = allocationPolicy;
            this.atomicCellCount = computeAtomicCellCount(cube, members);

            // Build the array of members by ordinal. If a member is not
            // specified for a particular dimension, use the 'all' member (not
            // necessarily the same as the default member).
            final List<Member> memberList = new ArrayList<Member>();
            for (RolapCubeHierarchy hierarchy : cube.getHierarchyList()) {
                memberList.add(hierarchy.getDefaultMember());
            }
            membersByOrdinal =
                memberList.toArray(new Member[memberList.size()]);
            for (RolapMember member : members) {
                final RolapCubeHierarchy hierarchy = member.getHierarchy();
                if (hierarchy.isScenario) {
                    assert member.isAll();
                }
                final int ordinal = hierarchy.getOrdinalInCube();
                membersByOrdinal[ordinal] = member;
            }
        }

        /**
         * Returns the amount by which the cell value has increased with this
         * override.
         *
         * @return Amount by which value has increased
         */
        public double getOffset() {
            return newValue - currentValue;
        }

        /**
         * Returns the position of this writeback cell relative to another
         * co-ordinate.
         *
         * <p>Assumes that {@code members} contains an entry for each dimension
         * in the cube.
         *
         * @param members Co-ordinates of another cell
         * @return Relation of this writeback cell to other co-ordinate, never
         * null
         */
        CellRelation getRelationTo(Member[] members) {
            int aboveCount = 0;
            int belowCount = 0;
            for (int i = 0; i < members.length; i++) {
                Member thatMember = members[i];
                Member thisMember = membersByOrdinal[i];
                // FIXME: isChildOrEqualTo is very inefficient. It should use
                // level depth as a guideline, at least.
                if (thatMember.isChildOrEqualTo(thisMember)) {
                    if (thatMember.equals(thisMember)) {
                        // thisMember equals member
                    } else {
                        // thisMember is ancestor of member
                        ++aboveCount;
                        if (belowCount > 0) {
                            return CellRelation.NONE;
                        }
                    }
                } else if (thisMember.isChildOrEqualTo(thatMember)) {
                    // thisMember is descendant of member
                    ++belowCount;
                    if (aboveCount > 0) {
                        return CellRelation.NONE;
                    }
                } else {
                    return CellRelation.NONE;
                }
            }
            assert aboveCount == 0 || belowCount == 0;
            if (aboveCount > 0) {
                return CellRelation.ABOVE;
            } else if (belowCount > 0) {
                return CellRelation.BELOW;
            } else {
                return CellRelation.EQUAL;
            }
        }
    }

    /**
     * Decribes the relationship between two cells.
     */
    enum CellRelation {
        ABOVE,
        EQUAL,
        BELOW,
        NONE
    }

    /**
     * Compiled expression to implement a [Scenario].[{name}] calculated member.
     *
     * <p>When evaluated, replaces the value of a cell with the value overridden
     * by a writeback value, per
     * {@link org.olap4j.Cell#setValue(Object, org.olap4j.AllocationPolicy, Object...)},
     * and modifies the values of ancestors or descendants of such cells
     * according to the allocation policy.
     */
    private static class ScenarioCalc extends GenericCalc {
        private final ScenarioImpl scenario;

        /**
         * Creates a ScenarioCalc.
         *
         * @param scenario Scenario whose writeback values should be substituted
         * for the values stored in the database.
         */
        public ScenarioCalc(ScenarioImpl scenario) {
            super(new DummyExp(new ScalarType()));
            this.scenario = scenario;
        }

        /**
         * Returns the Scenario this writeback cell belongs to.
         *
         * @return Scenario, never null
         */
        private Scenario getScenario() {
            return scenario;
        }

        public Object evaluate(Evaluator evaluator) {
            // Evaluate current member in the given scenario by expanding in
            // terms of the writeback cells.

            // First, evaluate in the null scenario.
            final Member defaultMember =
                scenario.member.getHierarchy().getDefaultMember();
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setContext(defaultMember);
                final Object o = evaluator.evaluateCurrent();
                double d =
                    o instanceof Number
                        ? ((Number) o).doubleValue()
                        : 0d;

                // Look for writeback cells which are equal to, ancestors of,
                // or descendants of, the current cell. Modify the value
                // accordingly.
                //
                // It is possible that the value is modified by several
                // writebacks. If so, order is important.
                int changeCount = 0;
                for (ScenarioImpl.WritebackCell writebackCell
                    : scenario.writebackCells)
                {
                    CellRelation relation =
                        writebackCell.getRelationTo(evaluator.getMembers());
                    switch (relation) {
                    case ABOVE:
                        // This cell is below the writeback cell. Value is
                        // determined by allocation policy.
                        double atomicCellCount =
                        evaluateAtomicCellCount((RolapEvaluator) evaluator);
                        if (atomicCellCount == 0d) {
                            // Sometimes the value comes back zero if the cache
                            // is not ready. Switch to 1, which at least does
                            // not give divide-by-zero. We will be invoked again
                            // for the correct answer when the cache has been
                            // populated.
                            atomicCellCount = 1d;
                        }
                        switch (writebackCell.allocationPolicy) {
                        case EQUAL_ALLOCATION:
                            d = writebackCell.newValue
                            * atomicCellCount
                            / writebackCell.atomicCellCount;
                            break;
                        case EQUAL_INCREMENT:
                            d += writebackCell.getOffset()
                            * atomicCellCount
                            / writebackCell.atomicCellCount;
                            break;
                        default:
                            throw Util.unexpected(
                                writebackCell.allocationPolicy);
                        }
                        ++changeCount;
                        break;
                    case EQUAL:
                        // This cell is the writeback cell. Value is the value
                        // written back.
                        d = writebackCell.newValue;
                        ++changeCount;
                        break;
                    case BELOW:
                        // This cell is above the writeback cell. Value is the
                        // current value plus the change in the writeback cell.
                        d += writebackCell.getOffset();
                        ++changeCount;
                        break;
                    case NONE:
                        // Writeback cell is unrelated. It has no effect on
                        // cell's value.
                        break;
                    default:
                        throw Util.unexpected(relation);
                    }
                }
                // Don't create a new object if value has not changed.
                if (changeCount == 0) {
                    return o;
                } else {
                    return d;
                }
            } finally {
                evaluator.restore(savepoint);
            }
        }
    }
}

// End ScenarioImpl.java
