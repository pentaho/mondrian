/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;

import java.sql.*;
import java.util.*;

import org.olap4j.AllocationPolicy;
import org.olap4j.Scenario;

/**
 * <code>RolapCell</code> implements {@link mondrian.olap.Cell} within a
 * {@link RolapResult}.
 *
 * @version $Id$
 */
class RolapCell implements Cell {
    private final RolapResult result;
    protected final int[] pos;
    protected RolapResult.CellInfo ci;

    /**
     * Creates a RolapCell.
     *
     * @param result Result cell belongs to
     * @param pos Coordinates of cell
     * @param ci Cell information, containing value et cetera
     */
    RolapCell(RolapResult result, int[] pos, RolapResult.CellInfo ci) {
        this.result = result;
        this.pos = pos;
        this.ci = ci;
    }

    public List<Integer> getCoordinateList() {
        return new AbstractList<Integer>() {
            public Integer get(int index) {
                return pos[index];
            }

            public int size() {
                return pos.length;
            }
        };
    }

    public Object getValue() {
        if (ci.value == Util.nullValue) {
            return null;
        }
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
        final Member[] currentMembers = getMembersForDrillThrough();
        CellRequest cellRequest =
            RolapAggregationManager.makeDrillThroughRequest(
                currentMembers, extendedContext, result.getCube());
        return (cellRequest == null)
            ? null
            : aggMan.getDrillThroughSql(cellRequest, false);
    }


    public int getDrillThroughCount() {
        RolapAggregationManager aggMan = AggregationManager.instance();
        final Member[] currentMembers = getMembersForDrillThrough();
        CellRequest cellRequest =
            RolapAggregationManager.makeDrillThroughRequest(
                currentMembers, false, result.getCube());
        if (cellRequest == null) {
            return -1;
        }
        RolapConnection connection =
            (RolapConnection) result.getQuery().getConnection();
        final String sql = aggMan.getDrillThroughSql(cellRequest, true);
        final SqlStatement stmt =
            RolapUtil.executeQuery(
                connection.getDataSource(),
                sql,
                "RolapCell.getDrillThroughCount",
                "Error while counting drill-through");
        try {
            ResultSet rs = stmt.getResultSet();
            rs.next();
            ++stmt.rowCount;
            return rs.getInt(1);
        } catch (SQLException e) {
            throw stmt.handle(e);
        } finally {
            stmt.close();
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
        // get current members
        final Member[] currentMembers = getMembersForDrillThrough();
        Cube x = chooseDrillThroughCube(currentMembers, result.getCube());
        return x != null;
    }

    public static RolapCube chooseDrillThroughCube(
        Member[] currentMembers,
        RolapCube defaultCube)
    {
        if (defaultCube != null && defaultCube.isVirtual()) {
            List<RolapCube> cubes = new ArrayList<RolapCube>();
            for (RolapMember member : defaultCube.getMeasuresMembers()) {
                if (member instanceof RolapVirtualCubeMeasure) {
                    RolapVirtualCubeMeasure measure =
                        (RolapVirtualCubeMeasure) member;
                    cubes.add(measure.getCube());
                }
            }
            defaultCube = cubes.get(0);
            assert !defaultCube.isVirtual();
        }
        final DrillThroughVisitor visitor =
            new DrillThroughVisitor();
        try {
            for (Member member : currentMembers) {
                visitor.handleMember(member);
            }
        } catch (RuntimeException e) {
            if (e == DrillThroughVisitor.bomb) {
                // No cubes left
                return null;
            } else {
                throw e;
            }
        }
        return visitor.cube == null
             ? defaultCube
             : visitor.cube;
    }

    private RolapEvaluator getEvaluator() {
        return result.getCellEvaluator(pos);
    }

    private Member[] getMembersForDrillThrough() {
        final Member[] currentMembers = result.getCellMembers(pos);

        // replace member if we're dealing with a trivial formula
        if (currentMembers[0]
            instanceof RolapHierarchy.RolapCalculatedMeasure)
        {
            RolapHierarchy.RolapCalculatedMeasure measure =
                (RolapHierarchy.RolapCalculatedMeasure)currentMembers[0];
            if (measure.getFormula().getExpression() instanceof MemberExpr) {
                currentMembers[0] =
                    ((MemberExpr) measure.getFormula().getExpression())
                    .getMember();
            }
        }
        return currentMembers;
    }

    public Object getPropertyValue(String propertyName) {
        final boolean matchCase =
            MondrianProperties.instance().CaseSensitive.get();
        Property property = Property.lookup(propertyName, matchCase);
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

    public Member getContextMember(Hierarchy hierarchy) {
        return result.getMember(pos, hierarchy);
    }

    public void setValue(
        Scenario scenario,
        Object newValue,
        AllocationPolicy allocationPolicy,
        Object... allocationArgs)
    {
        if (allocationPolicy == null) {
            // user error
            throw Util.newError(
                "Allocation policy must not be null");
        }
        final RolapMember[] members = result.getCellMembers(pos);
        for (int i = 0; i < members.length; i++) {
            Member member = members[i];
            if (ScenarioImpl.isScenario(member.getHierarchy())) {
                scenario =
                    (Scenario) member.getPropertyValue(Property.SCENARIO.name);
                members[i] = (RolapMember) member.getHierarchy().getAllMember();
            } else if (member.isCalculated()) {
                throw Util.newError(
                    "Cannot write to cell: one of the coordinates ("
                    + member.getUniqueName()
                    + ") is a calculcated member");
            }
        }
        if (scenario == null) {
            throw Util.newError("No active scenario");
        }
        if (allocationArgs == null) {
            allocationArgs = new Object[0];
        }
        final Object currentValue = getValue();
        if (!(currentValue instanceof Number)) {
            // Cell is not a number. Likely it is a string or a
            // MondrianEvaluationException. Do not attempt to change the value
            // in this case. (REVIEW: Is this the correct behavior?)
            return;
        }
        double doubleCurrentValue = ((Number) currentValue).doubleValue();
        double doubleNewValue = ((Number) newValue).doubleValue();
        ((ScenarioImpl) scenario).setCellValue(
            result.getQuery().getConnection(),
            Arrays.asList(members),
            doubleNewValue,
            doubleCurrentValue,
            allocationPolicy,
            allocationArgs);
    }

    /**
     * Visitor that walks over a cell's expression and checks whether the
     * cell should allow drill-through. If not, throws the {@link #bomb}
     * exception.
     *
     * <p>Examples:</p>
     * <ul>
     * <li>Literal 1 is drillable</li>
     * <li>Member [Measures].[Unit Sales] is drillable</li>
     * <li>Calculated member with expression [Measures].[Unit Sales] +
     *     1 is drillable</li>
     * <li>Calculated member with expression
     *     ([Measures].[Unit Sales], [Time].PrevMember) is not drillable</li>
     * </ul>
     */
    private static class DrillThroughVisitor extends MdxVisitorImpl {
        static final RuntimeException bomb = new RuntimeException();
        RolapCube cube = null;

        DrillThroughVisitor() {
        }

        public Object visit(MemberExpr memberExpr) {
            handleMember(memberExpr.getMember());
            return null;
        }

        public Object visit(ResolvedFunCall call) {
            final FunDef def = call.getFunDef();
            final Exp[] args = call.getArgs();
            final String name = def.getName();
            if (name.equals("+")
                || name.equals("-")
                || name.equals("/")
                || name.equals("*")
                || name.equals("CoalesceEmpty")
                // Allow parentheses but don't allow tuple
                || name.equals("()") && args.length == 1)
            {
                return null;
            }
            throw bomb;
        }

        public void handleMember(Member member) {
            if (member instanceof RolapStoredMeasure) {
                // If this member is in a different cube that previous members
                // we've seen, we cannot drill through.
                final RolapCube cube = ((RolapStoredMeasure) member).getCube();
                if (this.cube == null) {
                    this.cube = cube;
                } else if (this.cube != cube) {
                    // this measure lives in a different cube than previous
                    // measures we have seen
                    throw bomb;
                }
            } else if (member instanceof RolapCubeMember) {
                handleMember(((RolapCubeMember) member).rolapMember);
            } else if (member
                instanceof RolapHierarchy.RolapCalculatedMeasure)
            {
                RolapHierarchy.RolapCalculatedMeasure measure =
                    (RolapHierarchy.RolapCalculatedMeasure) member;
                measure.getFormula().getExpression().accept(this);
            } else if (member instanceof RolapMember) {
                // regular RolapMember - fine
            } else {
                // don't know what this is!
                throw bomb;
            }
        }

        public Object visit(NamedSetExpr namedSetExpr) {
            throw Util.newInternal("not valid here: " + namedSetExpr);
        }

        public Object visit(Literal literal) {
            return null; // literals are drillable
        }

        public Object visit(Query query) {
            throw Util.newInternal("not valid here: " + query);
        }

        public Object visit(QueryAxis queryAxis) {
            throw Util.newInternal("not valid here: " + queryAxis);
        }

        public Object visit(Formula formula) {
            throw Util.newInternal("not valid here: " + formula);
        }

        public Object visit(UnresolvedFunCall call) {
            throw Util.newInternal("expected resolved expression");
        }

        public Object visit(Id id) {
            throw Util.newInternal("expected resolved expression");
        }

        public Object visit(ParameterExpr parameterExpr) {
            // Not valid in general; might contain complex expression
            throw bomb;
        }

        public Object visit(DimensionExpr dimensionExpr) {
            // Not valid in general; might be part of complex expression
            throw bomb;
        }

        public Object visit(HierarchyExpr hierarchyExpr) {
            // Not valid in general; might be part of complex expression
            throw bomb;
        }

        public Object visit(LevelExpr levelExpr) {
            // Not valid in general; might be part of complex expression
            throw bomb;
        }
    }
}

// End RolapCell.java
