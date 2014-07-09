/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;

/**
 * Definition of the <code>Hierarchize</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class HierarchizeFunDef extends FunDefBase {
    static final String[] prePost = {"PRE", "POST"};
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Hierarchize",
            "Hierarchize(<Set>[, POST])",
            "Orders the members of a set in a hierarchy.",
            new String[] {"fxx", "fxxy"},
            HierarchizeFunDef.class,
            prePost);

    public HierarchizeFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            compiler.compileList(call.getArg(0), true);
        String order = getLiteralArg(call, 1, "PRE", prePost);
        final boolean post = order.equals("POST");
        return new AbstractListCalc(call, new Calc[] {listCalc}) {
            public TupleList evaluateList(Evaluator evaluator) {
                TupleList list = listCalc.evaluateList(evaluator);
                return hierarchizeTupleList(list, post);
            }
        };
    }
}

// End HierarchizeFunDef.java
