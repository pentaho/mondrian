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
import mondrian.olap.Member;
import mondrian.olap.type.Type;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.*;

/**
 * Definition of the <code>Except</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class ExceptFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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
        final ListCalc listCalc0 =
                compiler.compileList(call.getArg(0));
        final ListCalc listCalc1 =
                compiler.compileList(call.getArg(1));
        final Type elementType = ((SetType) listCalc0.getType()).getElementType();
        if (elementType instanceof TupleType) {
            return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
                public List evaluateList(Evaluator evaluator) {
                    List list0 = listCalc0.evaluateList(evaluator);
                    if (list0.isEmpty()) {
                        return list0;
                    }
                    List list1 = listCalc1.evaluateList(evaluator);
                    return exceptTuples(list0, list1);
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
                public List evaluateList(Evaluator evaluator) {
                    List list0 = listCalc0.evaluateList(evaluator);
                    if (list0.isEmpty()) {
                        return list0;
                    }
                    List list1 = listCalc1.evaluateList(evaluator);
                    return except(list0, list1);
                }
            };
        }
    }

    List except(final List list0, final List list1) {
        if (list0.size() == 0) {
            return list0;
        }
        Set set = new HashSet(list1);
        List result = new ArrayList();
        for (int i = 0, count = list0.size(); i < count; i++) {
            Object o = list0.get(i);
            if (!set.contains(o)) {
                result.add(o);
            }
        }
        return result;
    }

    List exceptTuples(final List list0, final List list1) {
        if (list0.size() == 0) {
            return list0;
        }
        // Because the .equals and .hashCode methods of
        // Member[] use identity, wrap each tuple in a list.
        Set set = new HashSet();
        for (int i = 0, count1 = list1.size(); i < count1; i++) {
            Member[] members = (Member[]) list1.get(i);
            set.add(Arrays.asList(members));
        }
        List result = new ArrayList();
        for (int i = 0, count0 = list0.size(); i < count0; i++) {
            Member[] members = (Member[]) list0.get(i);
            if (!set.contains(Arrays.asList(members))) {
                result.add(members);
            }
        }
        return result;
    }
}

// End ExceptFunDef.java
