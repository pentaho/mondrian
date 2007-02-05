/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.Evaluator;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

/**
 * Definition of the <code>Union</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class UnionFunDef extends FunDefBase {
    static final String[] ReservedWords = new String[] {"ALL", "DISTINCT"};

    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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
        checkCompatible(call.getArg(0), call.getArg(1), null); // todo: do at validate time
        final ListCalc listCalc0 =
                compiler.compileList(call.getArg(0));
        final ListCalc listCalc1 =
                compiler.compileList(call.getArg(1));
        return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
            public List evaluateList(Evaluator evaluator) {
                List list0 = listCalc0.evaluateList(evaluator);
                List list1 = listCalc1.evaluateList(evaluator);
                return union(list0, list1, all);
            }
        };
    }

    <T> List<T> union(List<T> list0, List<T> list1, final boolean all) {
        assert list0 != null;
        assert list1 != null;
        if (all) {
            if (list0.isEmpty()) {
                return list1;
            }
            if (list1.isEmpty()) {
                return list0;
            }
            List<T> result = new ArrayList<T>();
            result.addAll(list0);
            result.addAll(list1);
            return result;
        } else {
            Set added = new HashSet();
            List<T> result = new ArrayList<T>();
            FunUtil.addUnique(result, list0, added);
            FunUtil.addUnique(result, list1, added);
            return result;
        }
    }
}

// End UnionFunDef.java
