/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.mdx.ResolvedFunCall;

import java.util.*;

/**
 * Definition of the <code>Order</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class OrderFunDef extends FunDefBase {

    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "Order",
            "Order(<Set>, <Value Expression>[, ASC | DESC | BASC | BDESC])",
            "Arranges members of a set, optionally preserving or breaking the hierarchy.",
            new String[]{"fxxvy", "fxxv"},
            OrderFunDef.class,
            Flag.getNames());

    public OrderFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0), true);
        final Calc expCalc = compiler.compileScalar(call.getArg(1), true);
        final Flag order = getLiteralArg(call, 2, Flag.ASC, Flag.class);

        if (expCalc instanceof MemberValueCalc) {
            MemberValueCalc memberValueCalc = (MemberValueCalc) expCalc;
            List<Calc> constantList = new ArrayList<Calc>();
            List<Calc> variableList = new ArrayList<Calc>();
            final MemberCalc[] calcs = (MemberCalc[]) memberValueCalc.getCalcs();
            for (MemberCalc memberCalc : calcs) {
                if (memberCalc instanceof ConstantCalc &&
                    !listCalc.dependsOn(
                        memberCalc.getType().getHierarchy().getDimension())) {
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
                                        new DummyExp(expCalc.getType())),
                            order.descending,
                            order.brk));
            } else {
                // Some members are constant. Evaluate these before evaluating
                // the list expression.
                return new ContextCalc(
                    constantList.toArray(
                        new MemberCalc[constantList.size()]),
                        new CalcImpl(
                                call,
                                listCalc,
                                new MemberValueCalc(
                                    new DummyExp(expCalc.getType()),
                                    variableList.toArray(
                                        new MemberCalc[variableList.size()])),
                            order.descending,
                            order.brk));
            }
        }
        return new CalcImpl(call, listCalc, expCalc, order.descending, order.brk);
    }

    /**
     * Enumeration of the flags allowed to the <code>ORDER</code> MDX function.
     */
    enum Flag {
        ASC(false, false),
        DESC(true, false),
        BASC(false, true),
        BDESC(true, true);

        private final boolean descending;
        private final boolean brk;

        Flag(boolean descending, boolean brk) {
            this.descending = descending;
            this.brk = brk;
        }

        public static String[] getNames() {
            List<String> names = new ArrayList<String>();
            for (Flag flags : Flag.class.getEnumConstants()) {
                names.add(flags.name());
            }
            return names.toArray(new String[names.size()]);
        }
    }

    private static class CalcImpl extends AbstractListCalc {
        private final ListCalc listCalc;
        private final Calc expCalc;
        private final boolean desc;
        private final boolean brk;

        public CalcImpl(
            ResolvedFunCall call,
            ListCalc listCalc,
            Calc expCalc,
            boolean desc,
            boolean brk)
        {
            super(call, new Calc[]{listCalc, expCalc});
            assert listCalc.getResultStyle() == ExpCompiler.ResultStyle.MUTABLE_LIST;
            this.listCalc = listCalc;
            this.expCalc = expCalc;
            this.desc = desc;
            this.brk = brk;
        }

        public List evaluateList(Evaluator evaluator) {
            List list = listCalc.evaluateList(evaluator);
            sortMembers(evaluator.push(), list, expCalc, desc, brk);
            return list;
        }

        public Calc[] getCalcs() {
            return new Calc[] {listCalc, expCalc};
        }

        public List<Object> getArguments() {
            return Collections.singletonList(
                (Object) (desc ?
                    (brk ? Flag.BDESC : Flag.DESC) :
                    (brk ? Flag.BASC : Flag.ASC)));
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
            for (MemberCalc memberCalc : memberCalcs) {
                if (memberCalc.getType().usesDimension(dimension, true)) {
                    return false;
                }
            }
            return calc.dependsOn(dimension);
        }
        public ExpCompiler.ResultStyle getResultStyle() {
            return calc.getResultStyle();
        }
    }
}

// End OrderFunDef.java
