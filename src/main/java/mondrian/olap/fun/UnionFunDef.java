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
import mondrian.olap.*;

import java.util.*;

/**
 * Definition of the <code>Union</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class UnionFunDef extends FunDefBase {
    static final String[] ReservedWords = new String[] {"ALL", "DISTINCT"};

    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Union",
            "Union(<Set1>, <Set2>[, ALL])",
            "Returns the union of two sets, optionally retaining duplicates.",
            new String[] {"fxxx", "fxxxy"},
            UnionFunDef.class,
            ReservedWords);

    public UnionFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        String allString = getLiteralArg(call, 2, "DISTINCT", ReservedWords);
        final boolean all = allString.equalsIgnoreCase("ALL");
        // todo: do at validate time
        checkCompatible(call.getArg(0), call.getArg(1), null);
        final ListCalc listCalc0 =
            compiler.compileList(call.getArg(0));
        final ListCalc listCalc1 =
            compiler.compileList(call.getArg(1));
        return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
            public TupleList evaluateList(Evaluator evaluator) {
                TupleList list0 = listCalc0.evaluateList(evaluator);
                TupleList list1 = listCalc1.evaluateList(evaluator);
                return union(list0, list1, all);
            }
        };
    }

    TupleList union(TupleList list0, TupleList list1, final boolean all) {
        assert list0 != null;
        assert list1 != null;
        if (all) {
            if (list0.isEmpty()) {
                return list1;
            }
            if (list1.isEmpty()) {
                return list0;
            }
            TupleList result = TupleCollections.createList(list0.getArity());
            result.addAll(list0);
            result.addAll(list1);
            return result;
        } else {
            Set<List<Member>> added = new HashSet<List<Member>>();
            TupleList result = TupleCollections.createList(list0.getArity());
            FunUtil.addUnique(result, list0, added);
            FunUtil.addUnique(result, list1, added);
            return result;
        }
    }
}

// End UnionFunDef.java
