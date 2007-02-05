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

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.StringCalc;
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
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final boolean tuple = ((SetType) listCalc.getType()).getElementType()
            instanceof TupleType;
        if (call.getArg(1).getType() instanceof StringType) {
            final StringCalc stringCalc = compiler.compileString(call.getArg(1));
            final StringCalc delimCalc;
            if (call.getArgCount() == 3) {
                delimCalc = compiler.compileString(call.getArg(2));
            } else {
                delimCalc = ConstantCalc.constantString("");
            }

            return new GenerateStringCalcImpl(
                call, listCalc, stringCalc, tuple, delimCalc);
        } else {
            final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
            final String literalArg = getLiteralArg(call, 2, "", ReservedWords);
            final boolean all = literalArg.equalsIgnoreCase("ALL");

            return new GenerateListCalcImpl(
                call, listCalc, listCalc2, tuple, all);
        }
    }

    private static class GenerateListCalcImpl extends AbstractListCalc {
        private final ListCalc listCalc1;
        private final ListCalc listCalc2;
        private final boolean tuple;
        private final boolean all;

        public GenerateListCalcImpl(
            ResolvedFunCall call,
            ListCalc listCalc1,
            ListCalc listCalc2,
            boolean tuple, boolean all) {
            super(call, new Calc[]{listCalc1, listCalc2});
            this.listCalc1 = listCalc1;
            this.listCalc2 = listCalc2;
            this.tuple = tuple;
            this.all = all;
        }

        public List evaluateList(Evaluator evaluator) {
            List<Object> result = new ArrayList<Object>();
            if (tuple) {
                final List<Member[]> list1 = listCalc1.evaluateList(evaluator);
                final Evaluator evaluator2 = evaluator.push();
                if (all) {
                    for (Member[] members : list1) {
                        evaluator2.setContext(members);
                        final List<Object> result2 =
                            listCalc2.evaluateList(evaluator2);
                        result.addAll(result2);
                    }
                } else {
                    final Set<Object> emitted = new HashSet<Object>();
                    for (Member[] members : list1) {
                        evaluator2.setContext(members);
                        final List<Object> result2 =
                            listCalc2.evaluateList(evaluator2);
                        addDistinct(result, result2, emitted);
                    }
                }
            } else {
                final List<Member> list1 = listCalc1.evaluateList(evaluator);
                final Evaluator evaluator2 = evaluator.push();
                if (all) {
                    for (Member member : list1) {
                        evaluator2.setContext(member);
                        final List<Object> result2 =
                            listCalc2.evaluateList(evaluator2);
                        result.addAll(result2);
                    }
                } else {
                    Set<Object> emitted = new HashSet<Object>();
                    for (Member member : list1) {
                        evaluator2.setContext(member);
                        final List<Object> result2 =
                            listCalc2.evaluateList(evaluator2);
                        addDistinct(result, result2, emitted);
                    }
                }
            }
            return result;
        }

        private void addDistinct(
            List<Object> result,
            List<Object> result2,
            Set<Object> emitted) {

            for (Object row : result2) {
                Object entry = row;
                if (entry instanceof Member []) {
                    // wrap array for correct distinctness test
                    entry = new ArrayHolder<Member>((Member []) entry);
                }
                if (emitted.add(entry)) {
                    result.add(row);
                }
            }
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }

    private static class GenerateStringCalcImpl extends AbstractStringCalc {
        private final ListCalc listCalc;
        private final StringCalc stringCalc;
        private final boolean tuple;
        private final StringCalc sepCalc;

        public GenerateStringCalcImpl(
            ResolvedFunCall call,
            ListCalc listCalc,
            StringCalc stringCalc,
            boolean tuple, StringCalc sepCalc) {
            super(call, new Calc[]{listCalc, stringCalc});
            this.listCalc = listCalc;
            this.stringCalc = stringCalc;
            this.tuple = tuple;
            this.sepCalc = sepCalc;
        }

        public String evaluateString(Evaluator evaluator) {
            StringBuilder buf = new StringBuilder();
            int k = 0;
            if (tuple) {
                final List<Member[]> list1 = listCalc.evaluateList(evaluator);
                final Evaluator evaluator2 = evaluator.push();
                for (Member[] members : list1) {
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
                final List<Member> list1 = listCalc.evaluateList(evaluator);
                final Evaluator evaluator2 = evaluator.push();
                for (Member member : list1) {
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
