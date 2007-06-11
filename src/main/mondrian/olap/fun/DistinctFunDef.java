/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

/**
 * Definition of the <code>Distinct</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>Distinct(&lt;Set&gt;)</code></blockquote>
*
* @author jhyde
* @version $Id$
* @since Jun 10, 2007
*/
class DistinctFunDef extends FunDefBase {
    public static final DistinctFunDef instance = new DistinctFunDef();

    private DistinctFunDef() {
        super("Distinct",
            "Distinct(<Set>)",
            "Eliminates duplicate tuples from a set.",
            "fxx");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0));
        return new CalcImpl(call, listCalc);
    }

    static class CalcImpl extends AbstractListCalc {
        private final ListCalc listCalc;

        public CalcImpl(ResolvedFunCall call, ListCalc listCalc) {
            super(call, new Calc[]{listCalc});
            this.listCalc = listCalc;
        }

        public List evaluateList(Evaluator evaluator) {
            List list = listCalc.evaluateList(evaluator);
            return distinct(list);
        }

        static List<Object> distinct(List list) {
            Set<MemberHelper> set = new HashSet<MemberHelper>(list.size());
            List<Object> result = new ArrayList<Object>();

            for (Object element : list) {
                MemberHelper lookupObj = new MemberHelper(element);

                if (set.add(lookupObj)) {
                    result.add(element);
                }
            }
            return result;
        }
    }
}

// End DistinctFunDef.java
