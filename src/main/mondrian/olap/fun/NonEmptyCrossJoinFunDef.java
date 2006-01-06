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
import java.util.Collections;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;


/**
 * Definition of the <code>NONEMPTYCROSSJOIN</code> MDX function.
 */
public class NonEmptyCrossJoinFunDef extends CrossJoinFunDef {

    public NonEmptyCrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(FunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
            public List evaluateList(Evaluator evaluator) {
                final List list1 = listCalc1.evaluateList(evaluator);
                if (list1.isEmpty()) {
                    return Collections.EMPTY_LIST;
                }
                final List list2 = listCalc2.evaluateList(evaluator);
                // evaluate the arguments in non empty mode
                evaluator = evaluator.push();
                evaluator.setNonEmpty(true);
                List result = crossJoin(list1, list2, evaluator);

                // remove any remaining empty crossings from the result
                result = nonEmptyList(evaluator, result);
                return result;
            }

            public boolean dependsOn(Dimension dimension) {
                if (super.dependsOn(dimension)) {
                    return true;
                }
                // Member calculations generate members, which mask the actual
                // expression from the inherited context.
                if (listCalc1.getType().usesDimension(dimension, true)) {
                    return false;
                }
                if (listCalc2.getType().usesDimension(dimension, true)) {
                    return false;
                }
                // The implicit value expression, executed to figure out
                // whether a given tuple is empty, depends upon all dimensions.
                return true;
            }
        };
    }

}

// End NonEmptyCrossJoinFunDef.java
