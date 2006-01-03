/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.olap.type.MemberType;

import java.util.*;

/**
 * Definition of the <code>ORDER</code> MDX function.
 */
class OrderFunDef extends FunDefBase {
    private final boolean desc;
    private final boolean brk;

    public OrderFunDef(FunDef dummyFunDef, boolean desc, boolean brk) {
        super(dummyFunDef);
        this.desc = desc;
        this.brk = brk;
    }

    public Calc compileCall(FunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final Calc expCalc = compiler.compileScalar(call.getArg(1), true);
        if (expCalc instanceof MemberValueCalc) {
            MemberValueCalc memberValueCalc = (MemberValueCalc) expCalc;
            ArrayList constantList = new ArrayList();
            ArrayList variableList = new ArrayList();
            final MemberCalc[] calcs = (MemberCalc[]) memberValueCalc.getCalcs();
            for (int i = 0; i < calcs.length; i++) {
                MemberCalc memberCalc = calcs[i];
                if (memberCalc instanceof ConstantCalc &&
                        !listCalc.dependsOn(((MemberType) memberCalc.getType()).getHierarchy().getDimension())) {
                    constantList.add(memberCalc);
                } else {
                    variableList.add(memberCalc);
                }
            }
            if (constantList.isEmpty()) {
                // All members are non-constant -- cannot optimize
            } else if (variableList.isEmpty()) {
                // All members are constant. Optimize by setting entire context
                // first.
                return new ContextCalc(
                        calcs,
                        new CalcImpl(
                                call,
                                listCalc,
                                new ValueCalc(
                                        new DummyExp(expCalc.getType()))));
            } else {
                // Some members are constant. Evaluate these before evaluating
                // the list expression.
                return new ContextCalc(
                        (MemberCalc[]) constantList.toArray(
                                new MemberCalc[constantList.size()]),
                        new CalcImpl(
                                call,
                                listCalc,
                                new MemberValueCalc(
                                        new DummyExp(expCalc.getType()),
                                        (MemberCalc[]) variableList.toArray(
                                                new MemberCalc[variableList.size()]))));
            }
        }
        return new CalcImpl(call, listCalc, expCalc);
    }

    /**
     * Resolves calls to the <code>ORDER</code> MDX function.
     */
    static class OrderResolver extends MultiResolver {
        public OrderResolver() {
            super("Order",
                "Order(<Set>, <Value Expression>[, ASC | DESC | BASC | BDESC])",
                "Arranges members of a set, optionally preserving or breaking the hierarchy.",
                new String[]{"fxxvy", "fxxv"});
        }

        public String[] getReservedWords() {
            return OrderFunDef.Flags.instance.getNames();
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            int order = getLiteralArg(args, 2, Flags.ASC, Flags.instance, dummyFunDef);
            final boolean desc = Flags.isDescending(order);
            final boolean brk = Flags.isBreak(order);
            return new OrderFunDef(dummyFunDef, desc, brk);
        }
    }

    /**
     * Enumeration of the flags allowed to the <code>ORDER</code> MDX function.
     */
    static class Flags extends EnumeratedValues {
        static final Flags instance = new Flags();
        private Flags() {
            super(new String[] {"ASC","DESC","BASC","BDESC"});
        }
        public static final int ASC = 0;
        public static final int DESC = 1;
        public static final int BASC = 2;
        public static final int BDESC = 3;
        public static final boolean isDescending(int value) {
            return (value & DESC) == DESC;
        }
        public static final boolean isBreak(int value) {
            return (value & BASC) == BASC;
        }
    }

    private class CalcImpl extends AbstractListCalc {
        private final ListCalc listCalc;
        private final Calc expCalc;

        public CalcImpl(FunCall call, ListCalc listCalc, Calc expCalc) {
            super(call, new Calc[]{listCalc, expCalc});
            this.listCalc = listCalc;
            this.expCalc = expCalc;
        }

        public List evaluateList(Evaluator evaluator) {
            List list = listCalc.evaluateList(evaluator);
            sort(evaluator.push(), list, expCalc, desc, brk);
            return list;
        }

        public Calc[] getCalcs() {
            return new Calc[] {listCalc, expCalc};
        }

        public List getArguments() {
            return Collections.singletonList(
                    desc ?
                    (brk ? "BDESC" : "DESC") :
                    (brk ? "BASC" : "ASC"));
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }

    private static class ContextCalc extends GenericCalc {
        private final MemberCalc[] memberCalcs;
        private final Calc calc;
        private final Calc[] calcs;
        private final Member[] members; // workspace

        protected ContextCalc(MemberCalc[] memberCalcs, Calc calc) {
            super(new DummyExp(calc.getType()));
            this.memberCalcs = memberCalcs;
            this.calc = calc;
            this.calcs = new Calc[memberCalcs.length + 1];
            System.arraycopy(memberCalcs, 0, this.calcs, 0, memberCalcs.length);
            this.calcs[this.calcs.length - 1] = calc;
            this.members = new Member[memberCalcs.length];
        }

        public Calc[] getCalcs() {
            return calcs;
        }

        public Object evaluate(Evaluator evaluator) {
            // Evaluate each of the members, and set as context in the
            // sub-evaluator.
            for (int i = 0; i < memberCalcs.length; i++) {
                members[i] = memberCalcs[i].evaluateMember(evaluator);
            }
            final Evaluator subEval = evaluator.push(members);
            // Evaluate the expression in the new context.
            return calc.evaluate(subEval);
        }

        public boolean dependsOn(Dimension dimension) {
            if (anyDepends(memberCalcs, dimension)) {
                return true;
            }
            // Member calculations generate members, which mask the actual
            // expression from the inherited context.
            for (int i = 0; i < memberCalcs.length; i++) {
                MemberCalc memberCalc = memberCalcs[i];
                if (memberCalc.getType().usesDimension(dimension, true)) {
                    return false;
                }
            }
            return calc.dependsOn(dimension);
        }
    }
}

// End OrderFunDef.java
