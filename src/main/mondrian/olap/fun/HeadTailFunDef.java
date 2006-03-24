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
import mondrian.olap.Exp;
import mondrian.olap.Evaluator;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.IntegerCalc;
import mondrian.calc.impl.ConstantCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.Collections;

/**
 * Definition of the <code>Head</code> and <code>Tail</code>
 * MDX builtin functions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class HeadTailFunDef extends FunDefBase {
    static final Resolver TailResolver = new ReflectiveMultiResolver(
            "Tail",
            "Tail(<Set>[, <Count>])",
            "Returns a subset from the end of a set.",
            new String[] {"fxx", "fxxn"},
            HeadTailFunDef.class);

    static final Resolver HeadResolver = new ReflectiveMultiResolver(
            "Head",
            "Head(<Set>[, < Numeric Expression >])",
            "Returns the first specified number of elements in a set.",
            new String[] {"fxx", "fxxn"},
            HeadTailFunDef.class);

    private final boolean head;

    public HeadTailFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
        head = dummyFunDef.getName().equals("Head");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0));
        final IntegerCalc integerCalc =
                call.getArgCount() > 1 ?
                compiler.compileInteger(call.getArg(1)) :
                ConstantCalc.constantInteger(1);
        if (head) {
            return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    List list = listCalc.evaluateList(evaluator);
                    int count = integerCalc.evaluateInteger(evaluator);
                    return head(count, list);
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc}) {

                public List evaluateList(Evaluator evaluator) {
                    List list = listCalc.evaluateList(evaluator);
                    int count = integerCalc.evaluateInteger(evaluator);
                    return tail(count, list);
                }
            };
        }
    }

    static List tail(final int count, List members) {
        assert members != null;
        final int memberCount = members.size();
        if (count >= memberCount) {
            return members;
        }
        if (count <= 0) {
            return Collections.EMPTY_LIST;
        }
        return members.subList(memberCount - count, memberCount);
    }

    static List head(final int count, List members) {
        assert members != null;
        if (count >= members.size()) {
            return members;
        }
        if (count <= 0) {
            return Collections.EMPTY_LIST;
        }
        return members.subList(0, count);
    }
}

// End HeadTailFunDef.java
