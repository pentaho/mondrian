/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.*;
import mondrian.rolap.sql.TupleConstraint;

/**
 * creates a {@link mondrian.olap.NativeEvaluator} that evaluates NON EMPTY
 * CrossJoin in SQL. The generated SQL will join the dimension tables with
 * the fact table and return all combinations that have a
 * corresponding row in the fact table. The current context (slicer) is
 * used for filtering (WHERE clause in SQL). This very effective computes
 * queris like
 * <pre>
 *   select ...
 *   NON EMTPY crossjoin([product].[name].members, [customer].[name].members) on rows
 *   froms [Sales]
 *   where ([store].[store #14])
 * </pre>
 * where both, customer.name and product.name have many members, but the resulting
 * crossjoin only has few.
 * <p>
 * The implementation currently can not handle sets containting
 * parent/child hierarchies, ragged hierarchies, calculated members and
 * the ALL member. Otherwise all
 *
 * @author av
 * @since Nov 21, 2005
 */
public class RolapNativeCrossJoin extends RolapNativeSet {

    public RolapNativeCrossJoin() {
        super.setEnabled(MondrianProperties.instance().EnableNativeCrossJoin.get());
    }

    /**
     * restricts the result to the current context.
     *
     * If the current context contains calculated members, these are silently ignored
     * which means, that too many members are return. This does not harm, because the
     * {@link RolapConnection}.NonEmptyResult will filter out these later.
     *
     * @author av
     * @since Nov 17, 2005
     */
    static class NonEmptyCrossJoinConstraint extends SetConstraint {
        NonEmptyCrossJoinConstraint(CrossJoinArg[] args, RolapEvaluator evaluator) {
            super(args, evaluator, false);
        }

    }

    protected boolean isStrict() {
        return false;
    }

    NativeEvaluator createEvaluator(RolapEvaluator evaluator, FunDef fun, Exp[] args) {
        if (!isEnabled()) {
            // native crossjoins were explicitly disabled, so no need
            // to alert about not using them
            return null;
        }
        RolapCube cube = (RolapCube) evaluator.getCube();

        CrossJoinArg[] cargs = checkCrossJoin(fun, args);
        if (cargs == null) {
            // Something in the arguments to the crossjoin prevented
            // native evaluation; may need to alert
            alertCrossJoinNonNative(
                evaluator,
                fun,
                "arguments not supported");
            return null;
        }
        if (isPreferInterpreter(cargs, true)) {
            // Native evaluation wouldn't buy us anything, so no
            // need to alert
            return null;
        }
        RolapLevel [] levels = new RolapLevel[cargs.length];
        for (int i = 0; i < cargs.length; i++) {
            levels[i] = cargs[i].getLevel();
        }

        if ((cube.isVirtual() &&
                !evaluator.getQuery().nativeCrossJoinVirtualCube())) {
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
                levels)) {
            // Missing join conditions due to non-conforming dimensions
            // meant native evaluation would have led to a true cross
            // product, which we want to defer instead of pushing it down;
            // so no need to alert
            return null;
        }

        // join with fact table will always filter out those members
        // that dont have a row in the fact table
        if (!evaluator.isNonEmpty())
            return null;

        LOGGER.debug("using native crossjoin");

        TupleConstraint constraint = new NonEmptyCrossJoinConstraint(cargs, evaluator);
        SchemaReader schemaReader = evaluator.getSchemaReader();
        return new SetEvaluator(cargs, schemaReader, constraint);
    }

    private void alertCrossJoinNonNative(
        RolapEvaluator evaluator,
        FunDef fun,
        String reason) {
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
