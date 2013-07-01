/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.olap.type.Type;

import java.util.*;

/**
 * Evaluator that collects profiling information as it evaluates expressions.
 *
 * <p>TODO: Cleanup tasks as part of explain/profiling project:
 *
 * <p>1. Obsolete AbstractCalc.calcs member, AbstractCalc.getCalcs(), and
 *     Calc[] constructor parameter to many Calc subclasses. Store the
 *     tree structure (children of a calc, parent of a calc) in
 *     RolapEvaluatorRoot.compiledExps.
 *
 * <p>Rationale: Children calcs are
 *     used in about 50 places, but mostly for dependency-checking (e.g.
 *     {@link mondrian.calc.impl.AbstractCalc#anyDepends}). A few places uses
 *     the calcs array but should use more strongly typed members. e.g.
 *     FilterFunDef.MutableMemberIterCalc should have data members
 *     'MemberListCalc listCalc' and 'BooleanCalc conditionCalc'.
 *
 * <p>2. Split Query into parse tree, plan, statement. Fits better into the
 *     createStatement - prepare - execute JDBC lifecycle. Currently Query has
 *     aspects of all of these, and some other state is held in RolapResult
 *     (computed in the constructor, unfortunately) and RolapEvaluatorRoot.
 *     This cleanup may not be essential for the explain/profiling task but
 *     should happen soon afterwards.
 *
 * @author jhyde
 * @since October, 2010
  */
public class RolapProfilingEvaluator extends RolapEvaluator {

    /**
     * Creates a profiling evaluator.
     *
     * @param root Shared context between this evaluator and its children
     */
    RolapProfilingEvaluator(RolapEvaluatorRoot root) {
        super(root);
    }

    /**
     * Creates a child evaluator.
     *
     * @param root Root evaluation context
     * @param evaluator Parent evaluator
     */
    private RolapProfilingEvaluator(
        RolapEvaluatorRoot root,
        RolapProfilingEvaluator evaluator,
        List<List<Member>> aggregationList)
    {
        super(
            root,
            evaluator,
            aggregationList);
    }

    @Override
    protected RolapEvaluator _push(List<List<Member>> aggregationList) {
        return new RolapProfilingEvaluator(root, this, aggregationList);
    }

    /**
     * Expression compiler which introduces dependency testing.
     *
     * <p>It also checks that the caller does not modify lists unless it has
     * explicitly asked for a mutable list.
     */
    static class ProfilingEvaluatorCompiler extends DelegatingExpCompiler {
        ProfilingEvaluatorCompiler(ExpCompiler compiler) {
            super(compiler);
        }

        protected Calc afterCompile(Exp exp, Calc calc, boolean mutable) {
            calc = super.afterCompile(exp, calc, mutable);
            if (calc == null) {
                return null;
            }
            if (calc.getType() instanceof SetType) {
                return new ProfilingIterCalc(
                    exp,
                    calc);
            } else {
                return new ProfilingScalarCalc(
                    exp,
                    calc);
            }
        }
    }

    /**
     * Compiled expression that wraps a list or iterator expression and gathers
     * profiling information.
     */
    private static class ProfilingIterCalc extends GenericIterCalc {
        private final Calc calc;
        private int callCount;
        private long callMillis;
        private long elementCount;
        private long elementSquaredCount;

        protected ProfilingIterCalc(Exp exp, Calc calc) {
            super(exp, new Calc[] {calc});
            this.calc = calc;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return calc.isWrapperFor(iface);
        }

        @Override
        public <T> T unwrap(Class<T> iface) {
            return calc.unwrap(iface);
        }

        @Override
        public SetType getType() {
            return (SetType) calc.getType();
        }

        @Override
        public ResultStyle getResultStyle() {
            return calc.getResultStyle();
        }

        @Override
        public boolean dependsOn(Hierarchy hierarchy) {
            return calc.dependsOn(hierarchy);
        }

        public Object evaluate(Evaluator evaluator) {
            ++callCount;
            long start = System.currentTimeMillis();
            final Object o = calc.evaluate(evaluator);
            long end = System.currentTimeMillis();
            callMillis += (end - start);
            if (o instanceof Collection) {
                long size = ((Collection) o).size();
                elementCount += size;
                elementSquaredCount += size * size;
            }
            return o;
        }

        @Override
        public void accept(CalcWriter calcWriter) {
            // Populate arguments with statistics.
            final Map<String, Object> argumentMap =
                new LinkedHashMap<String, Object>();
            if (calcWriter.enableProfiling()) {
                argumentMap.put("callCount", callCount);
                argumentMap.put("callMillis", callMillis);
                argumentMap.put("elementCount", elementCount);
                argumentMap.put("elementSquaredCount", elementSquaredCount);
            }
            calcWriter.setParentArgs(calc, argumentMap);

            // Invoke writer on our child calc. This node won't appear in the
            // query plan, but the arguments we just created will appear as
            // arguments of the child calc.
            calc.accept(calcWriter);
        }
    }

    /**
     * Compiled expression that wraps a scalar expression and gathers profiling
     * information.
     */
    private static class ProfilingScalarCalc extends GenericCalc {
        private final Calc calc;
        private int callCount;
        private long callMillis;

        ProfilingScalarCalc(Exp exp, Calc calc) {
            super(exp, new Calc[] {calc});
            this.calc = calc;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return calc.isWrapperFor(iface);
        }

        @Override
        public <T> T unwrap(Class<T> iface) {
            return calc.unwrap(iface);
        }

        @Override
        public Type getType() {
            return calc.getType();
        }

        @Override
        public Calc[] getCalcs() {
            return ((AbstractCalc) calc).getCalcs();
        }

        @Override
        public boolean dependsOn(Hierarchy hierarchy) {
            return calc.dependsOn(hierarchy);
        }

        public Object evaluate(Evaluator evaluator) {
            ++callCount;
            long start = System.currentTimeMillis();
            final Object o = calc.evaluate(evaluator);
            long end = System.currentTimeMillis();
            callMillis += (end - start);
            return o;
        }

        @Override
        public void accept(CalcWriter calcWriter) {
            final Map<String, Object> argumentMap =
                new LinkedHashMap<String, Object>();
            if (calcWriter.enableProfiling()) {
                argumentMap.put("callCount", callCount);
                argumentMap.put("callMillis", callMillis);
            }
            calcWriter.setParentArgs(calc, argumentMap);

            // Invoke writer on our child calc. This node won't appear in the
            // query plan, but the arguments we just created will appear as
            // arguments of the child calc.
            calc.accept(calcWriter);
        }
    }
}

// End RolapProfilingEvaluator.java
