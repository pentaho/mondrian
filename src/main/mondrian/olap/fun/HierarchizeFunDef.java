/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;

import java.util.List;

/**
 * Definition of the <code>Hierarchize</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class HierarchizeFunDef extends FunDefBase {
    static final String[] prePost = {"PRE","POST"};
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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
        final ListCalc listCalc = (ListCalc) compiler.compileAs(
            call.getArg(0), null, ResultStyle.MUTABLELIST_ONLY);
        String order = getLiteralArg(call, 1, "PRE", prePost);
        final boolean post = order.equals("POST");
        return new AbstractListCalc(call, new Calc[] {listCalc}) {
            public List evaluateList(Evaluator evaluator) {
                List list = listCalc.evaluateList(evaluator);
                hierarchize(list, post);
                return list;
            }
        };
    }
}

// End HierarchizeFunDef.java
