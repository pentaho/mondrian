/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.olap.type.SetType;
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
        final IterCalc listCalc = compiler.compileIter(call.getArg(0));
        final Calc expCalc = compiler.compileScalar(call.getArg(1), true);
        final Flag order = getLiteralArg(call, 2, Flag.ASC, Flag.class);

        final boolean tuple = ((SetType) listCalc.getType()).getArity() != 1;
        if (expCalc instanceof MemberValueCalc) {
            MemberValueCalc memberValueCalc = (MemberValueCalc) expCalc;
            List<MemberCalc> constantList = new ArrayList<MemberCalc>();
            List<Calc> variableList = new ArrayList<Calc>();
            final MemberCalc[] calcs = (MemberCalc[]) memberValueCalc.getCalcs();
            for (MemberCalc memberCalc : calcs) {
                if (memberCalc instanceof ConstantCalc &&
                    !listCalc.dependsOn(memberCalc.getType().getDimension())) {
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
                if (tuple) {
                    return new ContextCalc<Member[]>(
                        calcs,
                        new TupleCalcImpl(
                            call,
                            (TupleIterCalc) listCalc,
                            new ValueCalc(
                                new DummyExp(expCalc.getType())),
                            order.descending,
                            order.brk));
                } else {
                    return new ContextCalc<Member>(
                        calcs,
                        new MemberCalcImpl(
                            call,
                            (MemberIterCalc) listCalc,
                            new ValueCalc(
                                new DummyExp(expCalc.getType())),
                            order.descending,
                            order.brk));
                }
            } else {
                // Some members are constant. Evaluate these before evaluating
                // the list expression.
                if (tuple) {
                    return new ContextCalc<Member[]>(
                        constantList.toArray(
                            new MemberCalc[constantList.size()]),
                        new TupleCalcImpl(
                            call,
                            (TupleIterCalc) listCalc,
                            new MemberValueCalc(
                                new DummyExp(expCalc.getType()),
                                variableList.toArray(
                                    new MemberCalc[variableList.size()])),
                            order.descending,
                            order.brk));
                } else {
                    return new ContextCalc<Member>(
                        constantList.toArray(
                            new MemberCalc[constantList.size()]),
                        new MemberCalcImpl(
                            call,
                            (MemberIterCalc) listCalc,
                            new MemberValueCalc(
                                new DummyExp(expCalc.getType()),
                                variableList.toArray(
                                    new MemberCalc[variableList.size()])),
                            order.descending,
                            order.brk));
                }
            }
        }
        if (tuple) {
            return new TupleCalcImpl(
                call, (TupleIterCalc) listCalc,
                expCalc, order.descending, order.brk);
        } else {
            return new MemberCalcImpl(
                call, (MemberIterCalc) listCalc,
                expCalc, order.descending, order.brk);
        }
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

    private interface CalcWithDual<T> extends Calc {
        public List<T> evaluateDual(
            Evaluator rootEvaluator,
            Evaluator subEvaluator);
    }

    private static class MemberCalcImpl
        extends AbstractMemberListCalc
        implements CalcWithDual<Member>
    {
        private final MemberIterCalc listCalc;
        private final Calc expCalc;
        private final boolean desc;
        private final boolean brk;

        public MemberCalcImpl(
            ResolvedFunCall call,
            MemberIterCalc listCalc,
            Calc expCalc,
            boolean desc,
            boolean brk)
        {
            super(call, new Calc[]{listCalc, expCalc});
//            assert listCalc.getResultStyle() == ResultStyle.MUTABLE_LIST;
            this.listCalc = listCalc;
            this.expCalc = expCalc;
            this.desc = desc;
            this.brk = brk;
        }

        public List<Member> evaluateDual(
            Evaluator rootEvaluator,
            Evaluator subEvaluator)
        {
            final Iterable<Member> iterable =
                listCalc.evaluateMemberIterable(rootEvaluator);
            // REVIEW: If iterable happens to be a list, we'd like to pass it,
            // but we cannot yet guarantee that it is mutable.
            final List<Member> list = null;
            return sortMembers(
                subEvaluator.push(), iterable, list, expCalc, desc, brk);
        }

        public List<Member> evaluateMemberList(Evaluator evaluator) {
            final Iterable<Member> iterable =
                listCalc.evaluateMemberIterable(evaluator);
            // REVIEW: If iterable happens to be a list, we'd like to pass it,
            // but we cannot yet guarantee that it is mutable.
            final List<Member> list = null;
            return sortMembers(
                evaluator.push(), iterable, list, expCalc, desc, brk);
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

    private static class TupleCalcImpl
        extends AbstractTupleListCalc
        implements CalcWithDual<Member []>
    {
        private final TupleIterCalc iterCalc;
        private final Calc expCalc;
        private final boolean desc;
        private final boolean brk;
        private final int arity;

        public TupleCalcImpl(
            ResolvedFunCall call,
            TupleIterCalc iterCalc,
            Calc expCalc,
            boolean desc,
            boolean brk)
        {
            super(call, new Calc[]{iterCalc, expCalc});
//            assert iterCalc.getResultStyle() == ResultStyle.MUTABLE_LIST;
            this.iterCalc = iterCalc;
            this.expCalc = expCalc;
            this.desc = desc;
            this.brk = brk;
            this.arity = getType().getArity();
        }

        public List<Member[]> evaluateDual(
            Evaluator rootEvaluator,
            Evaluator subEvaluator)
        {
            final Iterable<Member[]> iterable =
                iterCalc.evaluateTupleIterable(rootEvaluator);
            final List<Member[]> list =
                iterable instanceof List && false
                    ? Util.<Member[]>cast((List) iterable)
                    : null;
            Util.discard(iterCalc.getResultStyle());
            return sortTuples(
                subEvaluator.push(), iterable, list, expCalc, desc, brk, arity);
        }

        public List<Member[]> evaluateTupleList(Evaluator evaluator) {
            final Iterable<Member[]> iterable =
                iterCalc.evaluateTupleIterable(evaluator);
            final List<Member[]> list =
                iterable instanceof List && false
                    ? Util.<Member[]>cast((List) iterable)
                    : null;
            return sortTuples(
                evaluator.push(), iterable, list, expCalc, desc, brk, arity);
        }

        public Calc[] getCalcs() {
            return new Calc[] {iterCalc, expCalc};
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

    private static class ContextCalc<T> extends GenericIterCalc {
        private final MemberCalc[] memberCalcs;
        private final CalcWithDual calc;
        private final Calc[] calcs;
        private final Member[] members; // workspace

        protected ContextCalc(MemberCalc[] memberCalcs, CalcWithDual<T> calc) {
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
            return calc.evaluateDual(evaluator, subEval);
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

        public ResultStyle getResultStyle() {
            return calc.getResultStyle();
        }
    }
}

// End OrderFunDef.java
