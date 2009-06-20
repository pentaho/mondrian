/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.ConstantCalc;
import mondrian.calc.impl.AbstractStringCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Definition of the <code>Generate</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class GenerateFunDef extends FunDefBase {
    static final ReflectiveMultiResolver ListResolver =
        new ReflectiveMultiResolver(
            "Generate",
            "Generate(<Set1>, <Set2>[, ALL])",
            "Applies a set to each member of another set and joins the resulting sets by union.",
            new String[] {"fxxx", "fxxxy"},
            GenerateFunDef.class);

    static final ReflectiveMultiResolver StringResolver =
        new ReflectiveMultiResolver(
            "Generate",
            "Generate(<Set>, <String>[, <String>])",
            "Applies a set to a string expression and joins resulting sets by string concatenation.",
            new String[] {"fSxS", "fSxSS"},
            GenerateFunDef.class);

    private static final String[] ReservedWords = new String[] {"ALL"};

    public GenerateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        final Type type = args[1].getType();
        if (type instanceof StringType) {
            // Generate(<Set>, <String>[, <String>])
            return type;
        } else {
            final Type memberType = TypeUtil.toMemberOrTupleType(type);
            return new SetType(memberType);
        }
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final IterCalc iterCalc = compiler.compileIter(call.getArg(0));
        final boolean tupleIn = ((SetType) iterCalc.getType()).getArity() != 1;
        if (call.getArg(1).getType() instanceof StringType) {
            final StringCalc stringCalc =
                compiler.compileString(call.getArg(1));
            final StringCalc delimCalc;
            if (call.getArgCount() == 3) {
                delimCalc = compiler.compileString(call.getArg(2));
            } else {
                delimCalc = ConstantCalc.constantString("");
            }

            return new GenerateStringCalcImpl(
                call, iterCalc, stringCalc, tupleIn, delimCalc);
        } else {
            final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
            final String literalArg = getLiteralArg(call, 2, "", ReservedWords);
            final boolean all = literalArg.equalsIgnoreCase("ALL");
            final boolean tupleOut = ((SetType) call.getType()).getArity() != 1;
            return new GenerateListCalcImpl(
                call, iterCalc, listCalc2, tupleIn, tupleOut, all);
        }
    }

    private static class GenerateListCalcImpl extends AbstractListCalc {
        private final MemberIterCalc memberIterCalc1;
        private final TupleIterCalc tupleIterCalc1;
        private final MemberListCalc memberListCalc2;
        private final TupleListCalc tupleListCalc2;
        private final boolean tupleIn;
        private final boolean tupleOut;
        private final boolean all;

        public GenerateListCalcImpl(
            ResolvedFunCall call,
            IterCalc iterCalc,
            ListCalc listCalc2,
            boolean tupleIn,
            boolean tupleOut,
            boolean all)
        {
            super(call, new Calc[]{iterCalc, listCalc2});
            if (tupleIn) {
                this.memberIterCalc1 = null;
                this.tupleIterCalc1 = (TupleIterCalc) iterCalc;
            } else {
                this.memberIterCalc1 = (MemberIterCalc) iterCalc;
                this.tupleIterCalc1 = null;
            }
            if (tupleOut) {
                this.memberListCalc2 = null;
                this.tupleListCalc2 = (TupleListCalc) listCalc2;
            } else {
                this.memberListCalc2 = (MemberListCalc) listCalc2;
                this.tupleListCalc2 = null;
            }
            this.tupleIn = tupleIn;
            this.tupleOut = tupleOut;
            this.all = all;
        }

        public List evaluateList(Evaluator evaluator) {
            evaluator = evaluator.push(false);
            // 8 cases - all of the combinations of tupleIn x tupleOut x all
            final Evaluator evaluator2 = evaluator.push();
            if (tupleIn) {
                final Iterable<Member[]> iterable1 =
                    tupleIterCalc1.evaluateTupleIterable(evaluator);
                if (tupleOut) {
                    List<Member[]> result = new ArrayList<Member[]>();
                    if (all) {
                        for (Member[] members : iterable1) {
                            evaluator2.setContext(members);
                            final List<Member[]> result2 =
                                tupleListCalc2.evaluateTupleList(evaluator2);
                            result.addAll(result2);
                        }
                    } else {
                        final Set<List<Member>> emitted =
                            new HashSet<List<Member>>();
                        for (Member[] members : iterable1) {
                            evaluator2.setContext(members);
                            final List<Member[]> result2 =
                                tupleListCalc2.evaluateTupleList(evaluator2);
                            addDistinctTuples(result, result2, emitted);
                        }
                    }
                    return result;
                } else {
                    List<Member> result = new ArrayList<Member>();
                    final Set<Member> emitted =
                        all ? null : new HashSet<Member>();
                    for (Member[] members : iterable1) {
                        evaluator2.setContext(members);
                        final List<Member> result2 =
                            memberListCalc2.evaluateMemberList(evaluator2);
                        if (emitted != null) {
                            addDistinctMembers(result, result2, emitted);
                        } else {
                            result.addAll(result2);
                        }
                    }
                    return result;
                }
            } else {
                final Iterable<Member> iterable1 =
                    memberIterCalc1.evaluateMemberIterable(evaluator);
                if (tupleOut) {
                    List<Member[]> result = new ArrayList<Member[]>();
                    if (all) {
                        for (Member member : iterable1) {
                            evaluator2.setContext(member);
                            final List<Member[]> result2 =
                                tupleListCalc2.evaluateTupleList(evaluator2);
                            result.addAll(result2);
                        }
                    } else {
                        final Set<List<Member>> emitted =
                            new HashSet<List<Member>>();
                        for (Member member : iterable1) {
                            evaluator2.setContext(member);
                            final List<Member[]> result2 =
                                tupleListCalc2.evaluateTupleList(evaluator2);
                            addDistinctTuples(result, result2, emitted);
                        }
                    }
                    return result;
                } else {
                    List<Member> result = new ArrayList<Member>();
                    if (all) {
                        for (Member member : iterable1) {
                            evaluator2.setContext(member);
                            final List<Member> result2 =
                                memberListCalc2.evaluateMemberList(evaluator2);
                            result.addAll(result2);
                        }
                    } else {
                        final Set<Member> emitted = new HashSet<Member>();
                        for (Member member : iterable1) {
                            evaluator2.setContext(member);
                            final List<Member> result2 =
                                memberListCalc2.evaluateMemberList(evaluator2);
                            addDistinctMembers(result, result2, emitted);
                        }
                    }
                    return result;
                }
            }
        }

        private static void addDistinctMembers(
            List<Member> result,
            List<Member> result2,
            Set<Member> emitted)
        {
            for (Member row : result2) {
                if (emitted.add(row)) {
                    result.add(row);
                }
            }
        }

        private static void addDistinctTuples(
            List<Member[]> result,
            List<Member[]> result2,
            Set<List<Member>> emitted)
        {
            for (Member[] row : result2) {
                // wrap array for correct distinctness test
                if (emitted.add(Arrays.asList(row))) {
                    result.add(row);
                }
            }
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }

    private static class GenerateStringCalcImpl extends AbstractStringCalc {
        private final MemberIterCalc memberIterCalc;
        private final TupleIterCalc tupleIterCalc;
        private final StringCalc stringCalc;
        private final boolean tuple;
        private final StringCalc sepCalc;

        public GenerateStringCalcImpl(
            ResolvedFunCall call,
            IterCalc listCalc,
            StringCalc stringCalc,
            boolean tuple,
            StringCalc sepCalc)
        {
            super(call, new Calc[]{listCalc, stringCalc});
            if (listCalc instanceof MemberIterCalc) {
                this.memberIterCalc = (MemberIterCalc) listCalc;
                this.tupleIterCalc = null;
            } else {
                this.memberIterCalc = null;
                this.tupleIterCalc = (TupleIterCalc) listCalc;
            }
            this.stringCalc = stringCalc;
            this.tuple = tuple;
            this.sepCalc = sepCalc;
        }

        public String evaluateString(Evaluator evaluator) {
            StringBuilder buf = new StringBuilder();
            int k = 0;
            if (tuple) {
                final Iterable<Member[]> iter11 =
                    tupleIterCalc.evaluateTupleIterable(evaluator);
                final Evaluator evaluator2 = evaluator.push();
                for (Member[] members : iter11) {
                    evaluator2.setContext(members);
                    if (k++ > 0) {
                        String sep = sepCalc.evaluateString(evaluator2);
                        buf.append(sep);
                    }
                    final String result2 =
                        stringCalc.evaluateString(evaluator2);
                    buf.append(result2);
                }
            } else {
                final Iterable<Member> iter1 =
                    memberIterCalc.evaluateMemberIterable(evaluator);
                final Evaluator evaluator2 = evaluator.push();
                for (Member member : iter1) {
                    evaluator2.setContext(member);
                    if (k++ > 0) {
                        String sep = sepCalc.evaluateString(evaluator2);
                        buf.append(sep);
                    }
                    final String result2 =
                        stringCalc.evaluateString(evaluator2);
                    buf.append(result2);
                }
            }
            return buf.toString();
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }
}

// End GenerateFunDef.java
