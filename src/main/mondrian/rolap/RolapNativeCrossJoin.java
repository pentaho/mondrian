/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.NonEmptyCrossJoinFunDef;
import mondrian.rolap.sql.*;

import java.util.*;

/**
 * Creates a {@link mondrian.olap.NativeEvaluator} that evaluates NON EMPTY
 * CrossJoin in SQL. The generated SQL will join the dimension tables with
 * the fact table and return all combinations that have a
 * corresponding row in the fact table. The current context (slicer) is
 * used for filtering (WHERE clause in SQL). This very effective computes
 * queries like
 *
 * <pre>
 *   SELECT ...
 *   NON EMTPY Crossjoin(
 *       [product].[name].members,
 *       [customer].[name].members) ON ROWS
 *   FROM [Sales]
 *   WHERE ([store].[store #14])
 * </pre>
 *
 * where both, customer.name and product.name have many members, but the
 * resulting crossjoin only has few.
 *
 * <p>The implementation currently can not handle sets containting
 * parent/child hierarchies, ragged hierarchies, calculated members and
 * the ALL member. Otherwise all
 *
 * @author av
 * @since Nov 21, 2005
 */
public class RolapNativeCrossJoin extends RolapNativeSet {

    public RolapNativeCrossJoin() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeCrossJoin.get());
    }

    /**
     * Constraint that restricts the result to the current context.
     *
     * <p>If the current context contains calculated members, silently ignores
     * them. This means means that too many members are returned, but this does
     * not matter, because the {@link RolapConnection.NonEmptyResult} will
     * filter out these later.</p>
     */
    static class NonEmptyCrossJoinConstraint extends SetConstraint {
        NonEmptyCrossJoinConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator)
        {
            // Cross join ignores calculated members, including the ones from
            // the slicer.
            super(args, evaluator, false);
        }

        public RolapMember findMember(Object key) {
            for (CrossJoinArg arg : args) {
                if (arg instanceof MemberListCrossJoinArg) {
                    final MemberListCrossJoinArg crossJoinArg =
                        (MemberListCrossJoinArg) arg;
                    final List<RolapMember> memberList =
                        crossJoinArg.getMembers();
                    for (RolapMember rolapMember : memberList) {
                        if (key.equals(rolapMember.getKey())) {
                            return rolapMember;
                        }
                    }
                }
            }
            return null;
        }
    }

    protected boolean restrictMemberTypes() {
        return false;
    }

    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        if (!isEnabled()) {
            // native crossjoins were explicitly disabled, so no need
            // to alert about not using them
            return null;
        }
        RolapCube cube = evaluator.getCube();

        List<CrossJoinArg[]> allArgs =
            crossJoinArgFactory()
                .checkCrossJoin(evaluator, fun, args, false);

        // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
        // array is the CrossJoin dimensions.  The second array, if any,
        // contains additional constraints on the dimensions. If either the list
        // or the first array is null, then native cross join is not feasible.
        if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
            // Something in the arguments to the crossjoin prevented
            // native evaluation; may need to alert
            alertCrossJoinNonNative(
                evaluator,
                fun,
                "arguments not supported");
            return null;
        }

        CrossJoinArg[] cjArgs = allArgs.get(0);

        // check if all CrossJoinArgs are "All" members or Calc members
        // "All" members do not have relational expression, and Calc members
        // in the input could produce incorrect results.
        //
        // If NECJ only has AllMembers, or if there is at least one CalcMember,
        // then sql evaluation is not possible.
        int countNonNativeInputArg = 0;

        for (CrossJoinArg arg : cjArgs) {
            if (arg instanceof MemberListCrossJoinArg) {
                MemberListCrossJoinArg cjArg =
                    (MemberListCrossJoinArg)arg;
                if (cjArg.hasAllMember() || cjArg.isEmptyCrossJoinArg()) {
                    ++countNonNativeInputArg;
                }
                if (cjArg.hasCalcMembers()) {
                    countNonNativeInputArg = cjArgs.length;
                    break;
                }
            }
        }

        if (countNonNativeInputArg == cjArgs.length) {
            // If all inputs contain "All" members; or
            // if all inputs are MemberListCrossJoinArg with empty member list
            // content, then native evaluation is not feasible.
            alertCrossJoinNonNative(
                evaluator,
                fun,
                "either all arguments contain the ALL member, "
                + "or empty member lists, or one has a calculated member");
            return null;
        }

        if (isPreferInterpreter(cjArgs, true)) {
            // Native evaluation wouldn't buy us anything, so no
            // need to alert
            return null;
        }

        // Verify that args are valid
        List<RolapLevel> levels = new ArrayList<RolapLevel>();
        for (CrossJoinArg cjArg : cjArgs) {
            RolapLevel level = cjArg.getLevel();
            if (level != null) {
                // Only add non null levels. These levels have real
                // constraints.
                levels.add(level);
            }
        }

        if (cube.isVirtual()
            && !evaluator.getQuery().nativeCrossJoinVirtualCube())
        {
            // Something in the query at large (namely, some unsupported
            // function on the [Measures] dimension) prevented native
            // evaluation with virtual cubes; may need to alert
            alertCrossJoinNonNative(
                evaluator,
                fun,
                "not all functions on [Measures] dimension supported");
            return null;
        }

        if (!NonEmptyCrossJoinConstraint.isValidContext(
                evaluator,
                false,
                levels.toArray(new RolapLevel[levels.size()]),
                restrictMemberTypes()))
        {
            // Missing join conditions due to non-conforming dimensions
            // meant native evaluation would have led to a true cross
            // product, which we want to defer instead of pushing it down;
            // so no need to alert
            return null;
        }

        // join with fact table will always filter out those members
        // that dont have a row in the fact table
        if (!evaluator.isNonEmpty()) {
            return null;
        }

        LOGGER.debug("using native crossjoin");

        // Create a new evaluation context, eliminating any outer context for
        // the dimensions referenced by the inputs to the NECJ
        // (otherwise, that outer context would be incorrectly intersected
        // with the constraints from the inputs).
        final int savepoint = evaluator.savepoint();

        try {
            Member[] evalMembers = evaluator.getMembers().clone();
            for (RolapLevel level : levels) {
                RolapHierarchy hierarchy = level.getHierarchy();
                memberLoop:
                for (int i = 0; i < evalMembers.length; ++i) {
                    Dimension evalMemberDimension =
                        evalMembers[i].getHierarchy().getDimension();
                    if (evalMemberDimension == hierarchy.getDimension()
                        && !evalMembers[i].isAll())
                    {
                        evalMembers[i] = hierarchy.getAllMember();
                        break memberLoop;
                    }
                }
            }
            evaluator.setContext(evalMembers);

            // Use the combined CrossJoinArg for the tuple constraint,
            // which will be translated to the SQL WHERE clause.
            CrossJoinArg[] cargs = combineArgs(allArgs);

            // Now construct the TupleConstraint that contains both the CJ
            // dimensions and the additional filter on them. It will make a
            // copy of the evaluator.
            TupleConstraint constraint =
                buildConstraint(evaluator, fun, cargs);

            // Use the just the CJ CrossJoiArg for the evaluator context,
            // which will be translated to select list in sql.
            final SchemaReader schemaReader = evaluator.getSchemaReader();
            return new SetEvaluator(cjArgs, schemaReader, constraint);
        } finally {
            evaluator.restore(savepoint);
        }
    }

    CrossJoinArg[] combineArgs(
        List<CrossJoinArg[]> allArgs)
    {
        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (allArgs.size() == 2) {
            CrossJoinArg[] predicateArgs = allArgs.get(1);
            if (predicateArgs != null) {
                // Combine the CJ and the additional predicate args.
                return Util.appendArrays(cjArgs, predicateArgs);
            }
        }
        return cjArgs;
    }

    private TupleConstraint buildConstraint(
        final RolapEvaluator evaluator,
        final FunDef fun,
        final CrossJoinArg[] cargs)
    {
        CrossJoinArg[] myArgs;
        if (safeToConstrainByOtherAxes(fun)) {
            myArgs = buildArgs(evaluator, cargs);
        } else {
            myArgs = cargs;
        }
        return new NonEmptyCrossJoinConstraint(myArgs, evaluator);
    }

    private CrossJoinArg[] buildArgs(
        final RolapEvaluator evaluator, final CrossJoinArg[] cargs)
    {
        Set<CrossJoinArg> joinArgs =
            crossJoinArgFactory().buildConstraintFromAllAxes(evaluator);
        joinArgs.addAll(Arrays.asList(cargs));
        return joinArgs.toArray(new CrossJoinArg[joinArgs.size()]);
    }

    private boolean safeToConstrainByOtherAxes(final FunDef fun) {
        return !(fun instanceof NonEmptyCrossJoinFunDef);
    }

    private void alertCrossJoinNonNative(
        RolapEvaluator evaluator,
        FunDef fun,
        String reason)
    {
        if (!(fun instanceof NonEmptyCrossJoinFunDef)) {
            // Only alert for an explicit NonEmptyCrossJoin,
            // since query authors use that to indicate that
            // they expect it to be "wicked fast"
            return;
        }
        if (!evaluator.getQuery().shouldAlertForNonNative(fun)) {
            return;
        }
        RolapUtil.alertNonNative("NonEmptyCrossJoin", reason);
    }
}

// End RolapNativeCrossJoin.java

