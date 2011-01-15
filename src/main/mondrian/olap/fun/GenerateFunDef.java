/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.calc.*;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
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
                call, (IterCalc) iterCalc, stringCalc, delimCalc);
        } else {
            final ListCalc listCalc2 =
                compiler.compileList(call.getArg(1));
            final String literalArg = getLiteralArg(call, 2, "", ReservedWords);
            final boolean all = literalArg.equalsIgnoreCase("ALL");
            final int arityOut = call.getType().getArity();
            return new GenerateListCalcImpl(
                call, iterCalc, listCalc2, arityOut, all);
        }
    }

    private static class GenerateListCalcImpl extends AbstractListCalc {
        private final IterCalc iterCalc1;
        private final ListCalc listCalc2;
        private final int arityOut;
        private final boolean all;

        public GenerateListCalcImpl(
            ResolvedFunCall call,
            IterCalc iterCalc,
            ListCalc listCalc2,
            int arityOut,
            boolean all)
        {
            super(call, new Calc[]{iterCalc, listCalc2});
            this.iterCalc1 = iterCalc;
            this.listCalc2 = listCalc2;
            this.arityOut = arityOut;
            this.all = all;
        }

        public TupleList evaluateList(Evaluator evaluator) {
            evaluator = evaluator.push(false);
            // 4 cases - all of the combinations of tupleIn x all
            final Evaluator evaluator2 = evaluator.push();
            final TupleIterable iterable1 =
                iterCalc1.evaluateIterable(evaluator);
            TupleList result = TupleCollections.createList(arityOut);
            if (all) {
                for (List<Member> members : iterable1) {
                    evaluator2.setContext(members);
                    final TupleList result2 =
                        listCalc2.evaluateList(evaluator2);
                    result.addAll(result2);
                }
            } else {
                final Set<List<Member>> emitted =
                    new HashSet<List<Member>>();
                for (List<Member> members : iterable1) {
                    evaluator2.setContext(members);
                    final TupleList result2 =
                        listCalc2.evaluateList(evaluator2);
                    addDistinctTuples(result, result2, emitted);
                }
            }
            return result;
        }

        private static void addDistinctTuples(
            TupleList result,
            TupleList result2,
            Set<List<Member>> emitted)
        {
            for (List<Member> row : result2) {
                // wrap array for correct distinctness test
                if (emitted.add(row)) {
                    result.add(row);
                }
            }
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }
    }

    private static class GenerateStringCalcImpl extends AbstractStringCalc {
        private final IterCalc iterCalc;
        private final StringCalc stringCalc;
        private final StringCalc sepCalc;

        public GenerateStringCalcImpl(
            ResolvedFunCall call,
            IterCalc iterCalc,
            StringCalc stringCalc,
            StringCalc sepCalc)
        {
            super(call, new Calc[]{iterCalc, stringCalc});
            this.iterCalc = iterCalc;
            this.stringCalc = stringCalc;
            this.sepCalc = sepCalc;
        }

        public String evaluateString(Evaluator evaluator) {
            StringBuilder buf = new StringBuilder();
            int k = 0;
            final TupleIterable iter11 =
                iterCalc.evaluateIterable(evaluator);
            final Evaluator evaluator2 = evaluator.push();
            for (List<Member> members : iter11) {
                evaluator2.setContext(members);
                if (k++ > 0) {
                    String sep = sepCalc.evaluateString(evaluator2);
                    buf.append(sep);
                }
                final String result2 =
                    stringCalc.evaluateString(evaluator2);
                buf.append(result2);
            }
            return buf.toString();
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }
    }
}

// End GenerateFunDef.java
