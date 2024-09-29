/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.ArrayTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.*;

/**
 * Definition of the <code>Except</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class ExceptFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Except",
            "Except(<Set1>, <Set2>[, ALL])",
            "Finds the difference between two sets, optionally retaining duplicates.",
            new String[]{"fxxx", "fxxxy"},
            ExceptFunDef.class);

    public ExceptFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // todo: implement ALL
        final ListCalc listCalc0 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc1 = compiler.compileList(call.getArg(1));
        return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                TupleList list0 = listCalc0.evaluateList(evaluator);
                if (list0.isEmpty()) {
                    return list0;
                }
                TupleList list1 = listCalc1.evaluateList(evaluator);
                if (list1.isEmpty()) {
                    return list0;
                }
                final Set<List<Member>> set1 = new HashSet<List<Member>>(list1);
                final TupleList result =
                    new ArrayTupleList(list0.getArity(), list0.size());
                for (List<Member> tuple1 : list0) {
                    if (!set1.contains(tuple1)) {
                        result.add(tuple1);
                    }
                }
                return result;
            }
        };
    }
}

// End ExceptFunDef.java
