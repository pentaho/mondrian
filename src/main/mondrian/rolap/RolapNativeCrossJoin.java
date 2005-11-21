/*
 //This software is subject to the terms of the Common Public License
 //Agreement, available at the following URL:
 //http://www.opensource.org/licenses/cpl.html.
 //Copyright (C) 2004-2005 TONBELLER AG
 //All Rights Reserved.
 //You must accept the terms of that agreement to use this software.
 */
package mondrian.rolap;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.NativeEvaluator;
import mondrian.rolap.sql.TupleConstraint;

public class RolapNativeCrossJoin extends RolapNativeSet {

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

        // join with fact table will always filter out those members
        // that dont have a row in the fact table
        if (!evaluator.isNonEmpty())
            return null;

        CrossJoinArg[] cargs = checkCrossJoin(fun, args);
        if (cargs == null)
            return null;
        if (isPreferInterpreter(cargs))
            return null;

        LOGGER.info("using native crossjoin");

        TupleConstraint constraint = new NonEmptyCrossJoinConstraint(cargs, evaluator);
        RolapSchemaReader schemaReader = (RolapSchemaReader) evaluator.getSchemaReader();
        return new SetEvaluator(getCache(), cargs, schemaReader, constraint);
    }

}
