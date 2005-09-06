/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 SAS Institute, Inc.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// sasebb, 16 December, 2004
*/
package mondrian.olap.fun;

import java.util.List;

import mondrian.olap.*;


/**
 * Definition of the <code>NONEMPTYCROSSJOIN</code> MDX function.
 */
public class NonEmptyCrossJoinFunDef extends CrossJoinFunDef {

    public NonEmptyCrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public boolean callDependsOn(FunCall call, Dimension dimension) {
        // First, evaluate the arguments, drawing from the context.
        if (super.callDependsOn(call, dimension)) {
            return true;
        }
        // The arguments, once evaluated, set the context, so if there is an
        // arg of dimension D the function will not depend on D
        for (int i = 0; i < call.getArgs().length; i++) {
            Exp exp = call.getArgs()[i];
            if (exp.getTypeX().usesDimension(dimension)) {
                return false;
            }
        }
        // We depend on every other dimension in the context, because we
        // effectively evaluate the measure for every cell.
        return true;
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        // evaluate the arguments in non empty mode
        evaluator = evaluator.push();
        evaluator.setNonEmpty(true);
        List result = (List)super.evaluate(evaluator, args);

        // remove any remaining empty crossings from the result
        result = nonEmptyList(evaluator, result);
        return result;
    }
}
