/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import mondrian.olap.*;
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
        if (!isEnabled())
            return null;
        RolapCube cube = (RolapCube) evaluator.getCube();
        if (!NonEmptyCrossJoinConstraint.isValidContext(evaluator, false) ||
            (cube.isVirtual() &&
                !evaluator.getQuery().nativeCrossJoinVirtualCube()))
        {
            return null;
        }

        // join with fact table will always filter out those members
        // that dont have a row in the fact table
        if (!evaluator.isNonEmpty())
            return null;

        CrossJoinArg[] cargs = checkCrossJoin(fun, args);
        if (cargs == null)
            return null;
        if (isPreferInterpreter(cargs))
            return null;
        
        if (cube.isVirtual() &&
            !validCrossJoinLevels(evaluator.getQuery(), cargs))
        {
            return null;
        }

        LOGGER.debug("using native crossjoin");

        TupleConstraint constraint = new NonEmptyCrossJoinConstraint(cargs, evaluator);
        SchemaReader schemaReader = evaluator.getSchemaReader();
        return new SetEvaluator(cargs, schemaReader, constraint);
    }
    
    /**
     * Determines if the levels referenced in the cross join all join with the
     * underlying fact tables that make up a virtual cube.
     * 
     * @param query query containing the cross join
     * @param cargs arguments to the cross join
     * 
     * @return true if all levels join with the fact tables
     */
    private boolean validCrossJoinLevels(Query query, CrossJoinArg[] cargs)
    {
        Set baseCubesLevelToColumnMaps = query.getVirtualCubeBaseCubeMaps();
        
        // we need to make sure all the levels join with each fact table;
        // otherwise, it doesn't make sense to do the processing
        // natively, as you'll end up with cartesian product joins!
        for (Iterator it = baseCubesLevelToColumnMaps.iterator();
            it.hasNext(); )
        {
            Map map = (Map) it.next();
            for (int i = 0; i < cargs.length; i++) {
                RolapLevel level = cargs[i].getLevel();
                if (map.get(level) == null) {
                    return false;
                }
            }
        }
        return true;
    }
}
