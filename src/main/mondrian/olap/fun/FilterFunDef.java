/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.server.Locus;

import java.util.List;

/**
 * Definition of the <code>Filter</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>Filter(&lt;Set&gt;, &lt;Search
 * Condition&gt;)</code></blockquote>
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class FilterFunDef extends FunDefBase {

    private static final String TIMING_NAME =
        FilterFunDef.class.getSimpleName();

    static final FilterFunDef instance = new FilterFunDef();

    private FilterFunDef() {
        super(
            "Filter",
            "Returns the set resulting from filtering a set based on a search condition.",
            "fxxb");
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        // Ignore the caller's priority. We prefer to return iterable, because
        // it makes NamedSet.CurrentOrdinal work.
        List<ResultStyle> styles = compiler.getAcceptableResultStyles();
        if (call.getArg(0) instanceof ResolvedFunCall
            && ((ResolvedFunCall) call.getArg(0)).getFunName().equals("AS"))
        {
            styles = ResultStyle.ITERABLE_ONLY;
        }
        if (styles.contains(ResultStyle.ITERABLE)
            || styles.contains(ResultStyle.ANY))
        {
            return compileCallIterable(call, compiler);
        } else if (styles.contains(ResultStyle.LIST)
            || styles.contains(ResultStyle.MUTABLE_LIST))
        {
            return compileCallList(call, compiler);
        } else {
            throw ResultStyleException.generate(
                ResultStyle.ITERABLE_LIST_MUTABLELIST_ANY,
                styles);
        }
    }

    /**
     * Returns an IterCalc.
     *
     * <p>Here we would like to get either a IterCalc or ListCalc (mutable)
     * from the inner expression. For the IterCalc, its Iterator
     * can be wrapped with another Iterator that filters each element.
     * For the mutable list, remove all members that are filtered.
     *
     * @param call Call
     * @param compiler Compiler
     * @return Implementation of this function call in the Iterable result style
     */
    protected IterCalc compileCallIterable(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        // want iterable, mutable list or immutable list in that order
        Calc imlcalc = compiler.compileAs(
            call.getArg(0), null, ResultStyle.ITERABLE_LIST_MUTABLELIST);
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
        Calc[] calcs = new Calc[] {imlcalc, bcalc};

        // check returned calc ResultStyles
        checkIterListResultStyles(imlcalc);

        if (imlcalc.getResultStyle() == ResultStyle.ITERABLE) {
            return new IterIterCalc(call, calcs);
        } else if (imlcalc.getResultStyle() == ResultStyle.LIST) {
            return new ImmutableIterCalc(call, calcs);
        } else {
            return new MutableIterCalc(call, calcs);
        }
    }

    private static abstract class BaseIterCalc extends AbstractIterCalc {
        protected BaseIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }

        public TupleIterable evaluateIterable(Evaluator evaluator) {
            evaluator.getTiming().markStart(TIMING_NAME);
            try {
                ResolvedFunCall call = (ResolvedFunCall) exp;
                // Use a native evaluator, if more efficient.
                // TODO: Figure this out at compile time.
                SchemaReader schemaReader = evaluator.getSchemaReader();
                NativeEvaluator nativeEvaluator =
                    schemaReader.getNativeSetEvaluator(
                        call.getFunDef(), call.getArgs(), evaluator, this);
                if (nativeEvaluator != null) {
                    return (TupleIterable)
                        nativeEvaluator.execute(ResultStyle.ITERABLE);
                } else {
                    return makeIterable(evaluator);
                }
            } finally {
                evaluator.getTiming().markEnd(TIMING_NAME);
            }
        }

        protected abstract TupleIterable makeIterable(Evaluator evaluator);

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }
    }

    private static class MutableIterCalc extends BaseIterCalc {
        MutableIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleIterable makeIterable(Evaluator evaluator) {
            evaluator.getTiming().markStart(TIMING_NAME);
            final int savepoint = evaluator.savepoint();
            try {
                Calc[] calcs = getCalcs();
                ListCalc lcalc = (ListCalc) calcs[0];
                BooleanCalc bcalc = (BooleanCalc) calcs[1];

                TupleList list = lcalc.evaluateList(evaluator);

                // make list mutable; guess selectivity .5
                TupleList result =
                    TupleCollections.createList(
                        list.getArity(), list.size() / 2);
                evaluator.setNonEmpty(false);
                TupleCursor cursor = list.tupleCursor();
                while (cursor.forward()) {
                    cursor.setContext(evaluator);
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
                evaluator.getTiming().markEnd(TIMING_NAME);
            }
        }
    }

    private static class ImmutableIterCalc extends BaseIterCalc {
        ImmutableIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleIterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];
            TupleList members = lcalc.evaluateList(evaluator);

            // Not mutable, must create new list
            TupleList result = members.cloneList(members.size() / 2);
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(false);
                TupleCursor cursor = members.tupleCursor();
                while (cursor.forward()) {
                    cursor.setContext(evaluator);
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
            }
        }
    }

    private static class IterIterCalc
        extends BaseIterCalc
    {
        IterIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof IterCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleIterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            IterCalc icalc = (IterCalc) calcs[0];
            final BooleanCalc bcalc = (BooleanCalc) calcs[1];

            // This does dynamics, just in time,
            // as needed filtering
            final TupleIterable iterable =
                icalc.evaluateIterable(evaluator);
            final Evaluator evaluator2 = evaluator.push();
            evaluator2.setNonEmpty(false);
            return new AbstractTupleIterable(iterable.getArity()) {
                public TupleCursor tupleCursor() {
                    return new AbstractTupleCursor(iterable.getArity()) {
                        final TupleCursor cursor = iterable.tupleCursor();

                        public boolean forward() {
                            while (cursor.forward()) {
                                Locus.peek().execution.checkCancelOrTimeout();
                                cursor.setContext(evaluator2);
                                if (bcalc.evaluateBoolean(evaluator2)) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        public List<Member> current() {
                            return cursor.current();
                        }
                    };
                }
            };
        }
    }


    /**
     * Returns a ListCalc.
     *
     * @param call Call
     * @param compiler Compiler
     * @return Implementation of this function call in the List result style
     */
    protected ListCalc compileCallList(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        Calc ilcalc = compiler.compileList(call.getArg(0), false);
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
        Calc[] calcs = new Calc[] {ilcalc, bcalc};

        // Note that all of the ListCalc's return will be mutable
        switch (ilcalc.getResultStyle()) {
        case LIST:
            return new ImmutableListCalc(call, calcs);
        case MUTABLE_LIST:
            return new MutableListCalc(call, calcs);
        }
        throw ResultStyleException.generateBadType(
            ResultStyle.MUTABLELIST_LIST,
            ilcalc.getResultStyle());
    }

    private static abstract class BaseListCalc extends AbstractListCalc {
        protected BaseListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }

        public TupleList evaluateList(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                schemaReader.getNativeSetEvaluator(
                    call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (TupleList) nativeEvaluator.execute(
                    ResultStyle.ITERABLE);
            } else {
                return makeList(evaluator);
            }
        }
        protected abstract TupleList makeList(Evaluator evaluator);

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }
    }

    private static class MutableListCalc extends BaseListCalc {
        MutableListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleList makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];
            TupleList members0 = lcalc.evaluateList(evaluator);

            // make list mutable;
            // for capacity planning, guess selectivity = .5
            TupleList result = members0.cloneList(members0.size() / 2);
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(false);
                final TupleCursor cursor = members0.tupleCursor();
                while (cursor.forward()) {
                    cursor.setContext(evaluator);
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
            }
        }
    }

    private static class ImmutableListCalc extends BaseListCalc {
        ImmutableListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleList makeList(Evaluator evaluator) {
            evaluator.getTiming().markStart(TIMING_NAME);
            final int savepoint = evaluator.savepoint();
            try {
                Calc[] calcs = getCalcs();
                ListCalc lcalc = (ListCalc) calcs[0];
                BooleanCalc bcalc = (BooleanCalc) calcs[1];
                TupleList members0 = lcalc.evaluateList(evaluator);

                // Not mutable, must create new list;
                // for capacity planning, guess selectivity = .5
                TupleList result = members0.cloneList(members0.size() / 2);
                evaluator.setNonEmpty(false);
                final TupleCursor cursor = members0.tupleCursor();
                while (cursor.forward()) {
                    cursor.setContext(evaluator);
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
                evaluator.getTiming().markEnd(TIMING_NAME);
            }
        }
    }
}

// End FilterFunDef.java
