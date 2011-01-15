/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2011 Julian Hyde and others
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

    static final ResolverImpl Resolver = new ResolverImpl();

    public OrderFunDef(ResolverBase resolverBase, int type, int[] types) {
        super(resolverBase, type, types);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final IterCalc listCalc = compiler.compileIter(call.getArg(0));
        List<SortKeySpec> keySpecList = new ArrayList<SortKeySpec>();
        buildKeySpecList(keySpecList, call, compiler);
        final int keySpecCount = keySpecList.size();
        Calc[] calcList = new Calc[keySpecCount + 1]; // +1 for the listCalc
        calcList[0] = listCalc;

        assert keySpecCount >= 1;
        final Calc expCalc = keySpecList.get(0).getKey();
        calcList[1] = expCalc;
        if (keySpecCount == 1) {
            if (expCalc instanceof MemberValueCalc) {
                MemberValueCalc memberValueCalc = (MemberValueCalc) expCalc;
                List<MemberCalc> constantList = new ArrayList<MemberCalc>();
                List<MemberCalc> variableList = new ArrayList<MemberCalc>();
                final MemberCalc[] calcs =
                    (MemberCalc[]) memberValueCalc.getCalcs();
                for (MemberCalc memberCalc : calcs) {
                    if (memberCalc instanceof ConstantCalc
                        && !listCalc.dependsOn(
                            memberCalc.getType().getHierarchy()))
                    {
                        constantList.add(memberCalc);
                    } else {
                        variableList.add(memberCalc);
                    }
                }
                if (constantList.isEmpty()) {
                    // All members are non-constant -- cannot optimize
                } else if (variableList.isEmpty()) {
                    // All members are constant. Optimize by setting entire
                    // context first.
                    calcList[1] = new ValueCalc(
                        new DummyExp(expCalc.getType()));
                    return new ContextCalc(
                        calcs,
                        new CalcImpl(
                            call, calcList, keySpecList));
                } else {
                    // Some members are constant. Evaluate these before
                    // evaluating the list expression.
                    calcList[1] = new MemberValueCalc(
                        new DummyExp(expCalc.getType()),
                        variableList.toArray(
                            new MemberCalc[variableList.size()]));
                    return new ContextCalc(
                        constantList.toArray(
                            new MemberCalc[constantList.size()]),
                        new CalcImpl(
                            call, calcList, keySpecList));
                }
            }
        }
        for (int i = 1; i < keySpecCount; i++) {
            final Calc expCalcs = keySpecList.get(i).getKey();
            calcList[i + 1] = expCalcs;
        }
        return new CalcImpl(call, calcList, keySpecList);
    }

    private void buildKeySpecList(
        List<SortKeySpec> keySpecList,
        ResolvedFunCall call,
        ExpCompiler compiler)
    {
        final int argCount = call.getArgs().length;
        int j = 1; // args[0] is the input set
        Calc key;
        Flag dir;
        Exp arg;
        while (j < argCount) {
            arg = call.getArg(j);
            key = compiler.compileScalar(arg, true);
            j++;
            if ((j >= argCount)
                || (call.getArg(j).getCategory() != Category.Symbol))
            {
                dir = Flag.ASC;
            } else {
                dir = getLiteralArg(call, j, Flag.ASC, Flag.class);
                j++;
            }
            keySpecList.add(new SortKeySpec(key, dir));
        }
    }

    private interface CalcWithDual extends Calc {
        public TupleList evaluateDual(
            Evaluator rootEvaluator,
            Evaluator subEvaluator);
    }

    private static class CalcImpl
        extends AbstractListCalc
        implements CalcWithDual
    {
        private final IterCalc iterCalc;
        private final Calc sortKeyCalc;
        private final List<SortKeySpec> keySpecList;
        private final int originalKeySpecCount;
        private final int arity;

        public CalcImpl(
            ResolvedFunCall call,
            Calc[] calcList,
            List<SortKeySpec> keySpecList)
        {
            super(call, calcList);
//            assert iterCalc.getResultStyle() == ResultStyle.MUTABLE_LIST;
            this.iterCalc = (IterCalc) calcList[0];
            this.sortKeyCalc = calcList[1];
            this.keySpecList = keySpecList;
            this.originalKeySpecCount = keySpecList.size();
            this.arity = getType().getArity();
        }

        public TupleList evaluateDual(
            Evaluator rootEvaluator, Evaluator subEvaluator)
        {
            assert originalKeySpecCount == 1;
            final TupleIterable iterable =
                iterCalc.evaluateIterable(rootEvaluator);
            // REVIEW: If iterable happens to be a list, we'd like to pass it,
            // but we cannot yet guarantee that it is mutable.
            final TupleList list =
                iterable instanceof TupleList && false
                    ? (TupleList) iterable
                    : null;
            Util.discard(iterCalc.getResultStyle());
            Flag sortKeyDir = keySpecList.get(0).getDirection();
            if (arity == 1) {
                return new UnaryTupleList(
                    sortMembers(
                        subEvaluator.push(false),
                        iterable.slice(0),
                        list == null ? null : list.slice(0),
                        sortKeyCalc,
                        sortKeyDir.descending,
                        sortKeyDir.brk));
            } else {
                return sortTuples(
                    subEvaluator.push(false),
                    iterable,
                    list,
                    sortKeyCalc,
                    sortKeyDir.descending,
                    sortKeyDir.brk,
                    arity);
            }
        }

        public TupleList evaluateList(Evaluator evaluator) {
            final TupleIterable iterable =
                iterCalc.evaluateIterable(evaluator);
            // REVIEW: If iterable happens to be a list, we'd like to pass it,
            // but we cannot yet guarantee that it is mutable.
            final TupleList list =
                iterable instanceof TupleList && false
                    ? (TupleList) iterable
                    : null;
            // go by size of keySpecList before purging
            if (originalKeySpecCount == 1) {
                Flag sortKeyDir = keySpecList.get(0).getDirection();
                if (arity == 1) {
                    return new UnaryTupleList(
                        sortMembers(
                            evaluator.push(false),
                            iterable.slice(0),
                            list == null ? null : list.slice(0),
                            sortKeyCalc,
                            sortKeyDir.descending,
                            sortKeyDir.brk));
                } else {
                    return sortTuples(
                        evaluator.push(false),
                        iterable,
                        list,
                        sortKeyCalc,
                        sortKeyDir.descending,
                        sortKeyDir.brk,
                        arity);
                }
            } else {
                purgeKeySpecList(keySpecList, list);
                if (keySpecList.isEmpty()) {
                    return list;
                }
                if (arity == 1) {
                    return new UnaryTupleList(
                        sortMembers(
                            evaluator.push(false),
                            iterable.slice(0),
                            list == null ? null : list.slice(0),
                            keySpecList));
                } else {
                    return sortTuples(
                        evaluator.push(false),
                        iterable,
                        list,
                        keySpecList,
                        arity);
                }
            }
        }

        public List<Object> getArguments() {
            // only good for original Order syntax
            assert originalKeySpecCount == 1;
            Flag sortKeyDir = keySpecList.get(0).getDirection();
            return Collections.singletonList(
                (Object)
                (sortKeyDir.descending
                 ? (sortKeyDir.brk ? Flag.BDESC : Flag.DESC)
                 : (sortKeyDir.brk ? Flag.BASC : Flag.ASC)));
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }

        private void purgeKeySpecList(
            List<SortKeySpec> keySpecList,
            TupleList list)
        {
            if (list == null || list.isEmpty()) {
                return;
            }
            if (keySpecList.size() == 1) {
                return;
            }
            List<Hierarchy> listHierarchies =
                new ArrayList<Hierarchy>(list.getArity());
            for (Member member : list.get(0)) {
                listHierarchies.add(member.getHierarchy());
            }
            // do not sort (remove sort key spec from the list) if
            // 1. <member_value_expression> evaluates to a member from a
            // level/dimension which is not used in the first argument
            // 2. <member_value_expression> evaluates to the same member for
            // all cells; for example, a report showing all quarters of
            // year 1998 will not be sorted if the sort key is on the constant
            // member [1998].[Q1]
            ListIterator<SortKeySpec> iter = keySpecList.listIterator();
            while (iter.hasNext()) {
                SortKeySpec key = iter.next();
                Calc expCalc = key.getKey();
                if (expCalc instanceof MemberOrderKeyFunDef.CalcImpl) {
                    Calc[] calcs =
                        ((MemberOrderKeyFunDef.CalcImpl) expCalc).getCalcs();
                    MemberCalc memberCalc = (MemberCalc) calcs[0];
                    if (memberCalc instanceof ConstantCalc
                        || !listHierarchies.contains(
                            memberCalc.getType().getHierarchy()))
                    {
                        iter.remove();
                    }
                }
            }
        }
    }

    private static class ContextCalc extends GenericIterCalc {
        private final MemberCalc[] memberCalcs;
        private final CalcWithDual calc;
        private final Member[] members; // workspace

        protected ContextCalc(MemberCalc[] memberCalcs, CalcWithDual calc) {
            super(new DummyExp(calc.getType()), xx(memberCalcs, calc));
            this.memberCalcs = memberCalcs;
            this.calc = calc;
            this.members = new Member[memberCalcs.length];
        }

        private static Calc[] xx(
            MemberCalc[] memberCalcs, CalcWithDual calc)
        {
            Calc[] calcs = new Calc[memberCalcs.length + 1];
            System.arraycopy(memberCalcs, 0, calcs, 0, memberCalcs.length);
            calcs[calcs.length - 1] = calc;
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

        public boolean dependsOn(Hierarchy hierarchy) {
            if (anyDepends(memberCalcs, hierarchy)) {
                return true;
            }
            // Member calculations generate members, which mask the actual
            // expression from the inherited context.
            for (MemberCalc memberCalc : memberCalcs) {
                if (memberCalc.getType().usesHierarchy(hierarchy, true)) {
                    return false;
                }
            }
            return calc.dependsOn(hierarchy);
        }

        public ResultStyle getResultStyle() {
            return calc.getResultStyle();
        }
    }

    private static class ResolverImpl extends ResolverBase {
        private final String[] reservedWords;
        static int[] argTypes;

        private ResolverImpl() {
            super(
                    "Order",
                    "Order(<Set> {, <Key Specification>}...)",
                    "Arranges members of a set, optionally preserving or breaking the hierarchy.",
                    Syntax.Function);
            this.reservedWords = Flag.getNames();
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            argTypes = new int[args.length];

            if (args.length < 2) {
                return null;
            }
            // first arg must be a set
            if (!validator.canConvert(0, args[0], Category.Set, conversions)) {
                return null;
            }
            argTypes[0] = Category.Set;
            // after fist args, should be: value [, symbol]
            int i = 1;
            while (i < args.length) {
                if (!validator.canConvert(
                    i, args[i], Category.Value, conversions))
                {
                    return null;
                } else {
                    argTypes[i] = Category.Value;
                    i++;
                }
                // if symbol is not specified, skip to the next
                if ((i == args.length)) {
                    //done, will default last arg to ASC
                } else {
                    if (!validator.canConvert(
                        i, args[i], Category.Symbol, conversions))
                    {
                        // continue, will default sort flag for prev arg to ASC
                    } else {
                        argTypes[i] = Category.Symbol;
                        i++;
                    }
                }
            }
            return new OrderFunDef(this, Category.Set, argTypes);
        }

        public String[] getReservedWords() {
            if (reservedWords != null) {
                return reservedWords;
            }
            return super.getReservedWords();
        }
    }
}

// End OrderFunDef.java
