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
        final boolean tuple = ((SetType) listCalc.getType()).getArity() != 1;

        assert keySpecCount >= 1;
        final Calc expCalc = keySpecList.get(0).getKey();
        calcList[1] = expCalc;
        if (keySpecCount == 1) {
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
                    calcList[1] = new ValueCalc(
                        new DummyExp(expCalc.getType()));
                    if (tuple) {
                        return new ContextCalc<Member[]>(
                            calcs,
                            new TupleCalcImpl(
                                call, calcList, keySpecList));
                    } else {
                        return new ContextCalc<Member>(
                            calcs,
                            new MemberCalcImpl(
                                call, calcList, keySpecList));
                    }
                } else {
                    // Some members are constant. Evaluate these before evaluating
                    // the list expression.
                    calcList[1] = new MemberValueCalc(
                        new DummyExp(expCalc.getType()),
                        variableList.toArray(
                            new MemberCalc[variableList.size()]));
                    if (tuple) {
                        return new ContextCalc<Member[]>(
                            constantList.toArray(
                                new MemberCalc[constantList.size()]),
                            new TupleCalcImpl(
                                call, calcList, keySpecList));
                    } else {
                        return new ContextCalc<Member>(
                            constantList.toArray(
                                new MemberCalc[constantList.size()]),
                            new MemberCalcImpl(
                                call, calcList, keySpecList));
                    }
                }
            }
        }
        for (int i = 1; i < keySpecCount; i++) {
            final Calc expCalcs = keySpecList.get(i).getKey();
            calcList[i + 1] = expCalcs;
        }
        if (tuple) {
            return new TupleCalcImpl(call, calcList, keySpecList);
        } else {
            return new MemberCalcImpl(call, calcList, keySpecList);
        }
    }

    private void buildKeySpecList(
        List<SortKeySpec> keySpecList, ResolvedFunCall call, ExpCompiler compiler)
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
            if ((j >= argCount) ||
                (call.getArg(j).getCategory() !=  Category.Symbol)) {
                dir = Flag.ASC;
            } else {
                dir = getLiteralArg(call, j, Flag.ASC, Flag.class);
                j++;
            }
            keySpecList.add(new SortKeySpec(key, dir));
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
        private final Calc[] sortKeyCalcList;
        private final List<SortKeySpec> keySpecList;
        private final int originalKeySpecCount;

        public MemberCalcImpl(
            ResolvedFunCall call,
            Calc[] calcList,
            List<SortKeySpec> keySpecList)
        {
            super(call, calcList);
            this.listCalc = (MemberIterCalc) calcList[0];
//            assert listCalc.getResultStyle() == ResultStyle.MUTABLE_LIST;
            this.sortKeyCalcList = calcList;
            this.keySpecList = keySpecList;
            this.originalKeySpecCount = keySpecList.size();
        }

        public List<Member> evaluateDual(
            Evaluator rootEvaluator,
            Evaluator subEvaluator)
        {
            assert originalKeySpecCount == 1;
            Flag sortKeyDir = keySpecList.get(0).getDirection();
            final Iterable<Member> iterable =
                listCalc.evaluateMemberIterable(rootEvaluator);
            // REVIEW: If iterable happens to be a list, we'd like to pass it,
            // but we cannot yet guarantee that it is mutable.
            final List<Member> list = null;
            return sortMembers(
                subEvaluator.push(false),
                iterable,
                list,
                sortKeyCalcList[1],
                sortKeyDir.descending,
                sortKeyDir.brk);
        }

        public List<Member> evaluateMemberList(Evaluator evaluator) {
            final Iterable<Member> iterable =
                listCalc.evaluateMemberIterable(evaluator);
            // REVIEW: If iterable happens to be a list, we'd like to pass it,
            // but we cannot yet guarantee that it is mutable.
            final List<Member> list = null;
            // go by size of keySpecList before purging
            if (originalKeySpecCount == 1) {
                Flag sortKeyDir = keySpecList.get(0).getDirection();
                return sortMembers(
                    evaluator.push(false),
                    iterable,
                    list,
                    sortKeyCalcList[1],
                    sortKeyDir.descending,
                    sortKeyDir.brk);
            } else {
                purgeKeySpecList(keySpecList, list);
                if (!keySpecList.isEmpty()) {
                    return sortMembers(
                        evaluator.push(false), iterable, list, keySpecList);
                } else {
                    return list;
                }
            }
        }

        public Calc[] getCalcs() {
            return sortKeyCalcList;
        }

        public List<Object> getArguments() {
            // only good for original Order syntax
            assert originalKeySpecCount == 1;
            Flag sortKeyDir = keySpecList.get(0).getDirection();
            return Collections.singletonList(
                (Object) (sortKeyDir.descending ?
                    (sortKeyDir.brk ? Flag.BDESC : Flag.DESC) :
                    (sortKeyDir.brk ? Flag.BASC : Flag.ASC)));
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }

        private void purgeKeySpecList(
            List<SortKeySpec> keySpecList, List list)
        {
            if (list == null || list.isEmpty()) {
                return;
            }
            if (keySpecList.size() == 1) {
                return;
            }
            Object head = list.get(0);
            List<Dimension> listDimensions = new ArrayList<Dimension>();
            if (head instanceof Object []) {
                for (Object dim : (Object []) head) {
                    listDimensions.add(((Member) dim).getDimension());
                }
            } else {
                listDimensions.add(((Member) head).getDimension());
            }
            // do not sort (remove sort key spec from the list) if
            // 1. <member_value_expression> evaluates to a member from a
            // level/dimension which is not used in the first argument
            // 2. <member_value_expression> evaluates to the same member for
            // all cells; for example, a report showing all quarters of
            // year 1998 will not be sorted if the sort key is on the constant
            // member [1998].[Q1]
            Iterator iter = keySpecList.listIterator();
            while (iter.hasNext()) {
                SortKeySpec key = (SortKeySpec) iter.next();
                Calc expCalc = key.getKey();
                if (expCalc instanceof MemberOrderKeyFunDef.CalcImpl) {
                    Calc[] calcs = ((MemberOrderKeyFunDef.CalcImpl) expCalc).getCalcs();
                    MemberCalc memberCalc = (MemberCalc) calcs[0];
                    if (memberCalc instanceof ConstantCalc ||
                        !listDimensions.contains(memberCalc.getType().getDimension()))
                    {
                        iter.remove();
                    }
                }
            }
        }
    }

    private static class TupleCalcImpl
        extends AbstractTupleListCalc
        implements CalcWithDual<Member []>
    {
        private final TupleIterCalc iterCalc;
        private final Calc[] sortKeyCalcList;
        private final List<SortKeySpec> keySpecList;
        private final int originalKeySpecCount;
        private final int arity;

        public TupleCalcImpl(
            ResolvedFunCall call,
            Calc[] calcList,
            List<SortKeySpec> keySpecList)
        {
            super(call, calcList);
//            assert iterCalc.getResultStyle() == ResultStyle.MUTABLE_LIST;
            this.iterCalc = (TupleIterCalc) calcList[0];
            this.sortKeyCalcList = calcList;
            this.keySpecList = keySpecList;
            this.originalKeySpecCount = keySpecList.size();
            this.arity = getType().getArity();
        }

        public List<Member[]> evaluateDual(
            Evaluator rootEvaluator,
            Evaluator subEvaluator)
        {
            assert originalKeySpecCount == 1;
            final Iterable<Member[]> iterable =
                iterCalc.evaluateTupleIterable(rootEvaluator);
            final List<Member[]> list =
                iterable instanceof List && false
                    ? Util.<Member[]>cast((List) iterable)
                    : null;
            Util.discard(iterCalc.getResultStyle());
            Flag sortKeyDir = keySpecList.get(0).getDirection();
            return sortTuples(
                subEvaluator.push(false),
                iterable,
                list,
                sortKeyCalcList[1],
                sortKeyDir.descending,
                sortKeyDir.brk,
                arity);
        }

        public List<Member[]> evaluateTupleList(Evaluator evaluator) {
            final Iterable<Member[]> iterable =
                iterCalc.evaluateTupleIterable(evaluator);
            final List<Member[]> list =
                iterable instanceof List && false
                    ? Util.<Member[]>cast((List) iterable)
                    : null;
            // go by size of keySpecList before purging
            if (originalKeySpecCount == 1) {
                Flag sortKeyDir = keySpecList.get(0).getDirection();
                return sortTuples(
                    evaluator.push(false),
                    iterable,
                    list,
                    sortKeyCalcList[1],
                    sortKeyDir.descending,
                    sortKeyDir.brk,
                    arity);
            } else {
                purgeKeySpecList(keySpecList, list);
                if (!keySpecList.isEmpty()) {
                    return sortTuples(
                        evaluator.push(false), iterable, list, keySpecList, arity);
                } else {
                    return list;
                }
            }
        }

        public Calc[] getCalcs() {
            return sortKeyCalcList;
        }

        public List<Object> getArguments() {
            // only good for original Order syntax
            assert originalKeySpecCount == 1;
            Flag sortKeyDir = keySpecList.get(0).getDirection();
            return Collections.singletonList(
                (Object) (sortKeyDir.descending ?
                    (sortKeyDir.brk ? Flag.BDESC : Flag.DESC) :
                    (sortKeyDir.brk ? Flag.BASC : Flag.ASC)));
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }

        private void purgeKeySpecList(
            List<SortKeySpec> keySpecList, List list)
        {
            if (list == null || list.isEmpty()) {
                return;
            }
            if (keySpecList.size() == 1) {
                return;
            }
            Object head = list.get(0);
            List<Dimension> listDimensions = new ArrayList<Dimension>();
            if (head instanceof Object []) {
                for (Object dim : (Object []) head) {
                    listDimensions.add(((Member) dim).getDimension());
                }
            } else {
                listDimensions.add(((Member) head).getDimension());
            }
            // do not sort (remove sort key spec from the list) if
            // 1. <member_value_expression> evaluates to a member from a
            // level/dimension which is not used in the first argument
            // 2. <member_value_expression> evaluates to the same member for
            // all cells; for example, a report showing all quarters of
            // year 1998 will not be sorted if the sort key is on the constant
            // member [1998].[Q1]
            Iterator iter = keySpecList.listIterator();
            while (iter.hasNext()) {
                SortKeySpec key = (SortKeySpec) iter.next();
                Calc expCalc = key.getKey();
                if (expCalc instanceof MemberOrderKeyFunDef.CalcImpl) {
                    Calc[] calcs = ((MemberOrderKeyFunDef.CalcImpl) expCalc).getCalcs();
                    MemberCalc memberCalc = (MemberCalc) calcs[0];
                    if (memberCalc instanceof ConstantCalc ||
                        !listDimensions.contains(memberCalc.getType().getDimension()))
                    {
                        iter.remove();
                    }
                }
            }
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
                Exp[] args, Validator validator, int[] conversionCount)
        {
            argTypes = new int[args.length];

            if (args.length < 2) {
                return null;
            }
            // first arg must be a set
            if (!validator.canConvert(
                args[0], Category.Set, conversionCount))
            {
                return null;
            }
            argTypes[0] = Category.Set;
            // after fist args, should be: value [, symbol]
            int i = 1;
            while (i < args.length) {
                if (!validator.canConvert(
                    args[i], Category.Value, conversionCount))
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
                        args[i], Category.Symbol, conversionCount))
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
