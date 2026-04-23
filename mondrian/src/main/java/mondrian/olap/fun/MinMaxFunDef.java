/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2021 Hitachi Vantara..  All rights reserved.
* Copyright (c) 2026 eazyBI.  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractDateTimeCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.FormatAwareFunDef;

import java.util.Date;
import java.util.List;

/**
 * Definition of the <code>Min</code> and <code>Max</code> MDX functions.
 *
 * <p>Supports both numeric and date expressions. Mondrian calculated members
 * are always statically typed as Numeric regardless of their formula's actual
 * return type, so date support relies on runtime type detection in
 * {@link #extremeValue}. Format string inference is handled by
 * {@link FormatAwareFunDef} to ensure the value expression's format is used
 * rather than a measure from a Filter condition.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class MinMaxFunDef extends AbstractAggregateFunDef
    implements FormatAwareFunDef
{
    // Extends ReflectiveMultiResolver to maintain binary compatibility with
    // BuiltinFunTable which expects these exact field types.
    static final ReflectiveMultiResolver MinResolver =
        new MinMaxResolverImpl(
            "Min",
            "Min(<Set>[, <Expression>])",
            "Returns the minimum value of a numeric or date expression "
                + "evaluated over a set.");

    static final MultiResolver MaxResolver =
        new MinMaxResolverImpl(
            "Max",
            "Max(<Set>[, <Expression>])",
            "Returns the maximum value of a numeric or date expression "
                + "evaluated over a set.");

    private static final String TIMING_NAME = MinMaxFunDef.class.getSimpleName();
    private final boolean max;

    public MinMaxFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
        this.max = dummyFunDef.getName().equals("Max");
    }

    /**
     * For the 2-arg form, use the value expression (last arg) for format
     * inference instead of the default depth-first walk which may find a
     * numeric measure in a Filter condition first. For the 1-arg form,
     * fall through to the default format-finding behavior.
     */
    public int getFormatExpIndex(Exp[] args) {
        return args.length >= 2 ? args.length - 1 : NOT_PARTICIPATING;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            compiler.compileList(call.getArg(0));
        // specific=false returns a generic Calc instead of a DoubleCalc.
        // Required for Date support: Mondrian statically types every calculated
        // member as Numeric regardless of runtime return type, so a Date-returning
        // calc reaches here with NumericType. compileDouble would wrap it in a
        // DoubleCalc whose evaluate() routes through evaluateDouble(), which
        // throws "Expected NUMERIC" on a Date. The generic Calc hands the raw
        // value to extremeValue for instanceof-based dispatch.
        final Calc calc =
            call.getArgCount() > 1
            ? compiler.compileScalar(call.getArg(1), false)
            : new ValueCalc(call);
        if (getReturnCategory() == Category.Numeric) {
            // Numeric return type (1-arg form and 2-arg with numeric expr).
            // Uses AbstractDoubleCalc so callers like IIf can compile the
            // result as a double. The evaluate() override passes through
            // Date values directly for calculated measures that are typed
            // as Numeric but return dates at runtime.
            return new AbstractDoubleCalc(
                call, new Calc[] {listCalc, calc})
            {
                public Object evaluate(Evaluator evaluator) {
                    evaluator.getTiming().markStart(TIMING_NAME);
                    final int savepoint = evaluator.savepoint();
                    try {
                        TupleList memberList =
                            evaluateCurrentList(listCalc, evaluator);
                        evaluator.setNonEmpty(false);
                        Object result = max
                            ? maxValue(evaluator, memberList, calc)
                            : minValue(evaluator, memberList, calc);
                        if (result instanceof Number) {
                            double d = ((Number) result).doubleValue();
                            return d == FunUtil.DoubleNull ? null : d;
                        }
                        return result;
                    } finally {
                        evaluator.restore(savepoint);
                        evaluator.getTiming().markEnd(TIMING_NAME);
                    }
                }

                public double evaluateDouble(Evaluator evaluator) {
                    Object result = evaluate(evaluator);
                    if (result instanceof Number) {
                        return ((Number) result).doubleValue();
                    }
                    return FunUtil.DoubleNull;
                }

                public boolean dependsOn(Hierarchy hierarchy) {
                    return anyDependsButFirst(getCalcs(), hierarchy);
                }
            };
        }
        // Non-numeric return type (2-arg with DateTime expr).
        // Must implement DateTimeCalc so that compileDateTime() callers
        // (e.g., VBA DateDiff via JavaFunDef) can cast without
        // ClassCastException. Only reachable when the value expression
        // is statically typed DateTime (e.g., DateSerial literal), not
        // when it is a calculated member (always typed Numeric).
        return new AbstractDateTimeCalc(call, new Calc[] {listCalc, calc}) {
            public java.util.Date evaluateDateTime(Evaluator evaluator) {
                Object result = evaluate(evaluator);
                return result instanceof java.util.Date
                    ? (java.util.Date) result : null;
            }

            public Object evaluate(Evaluator evaluator) {
                evaluator.getTiming().markStart(TIMING_NAME);
                final int savepoint = evaluator.savepoint();
                try {
                    TupleList memberList =
                        evaluateCurrentList(listCalc, evaluator);
                    evaluator.setNonEmpty(false);
                    return max
                        ? maxValue(evaluator, memberList, calc)
                        : minValue(evaluator, memberList, calc);
                } finally {
                    evaluator.restore(savepoint);
                    evaluator.getTiming().markEnd(TIMING_NAME);
                }
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                return anyDependsButFirst(getCalcs(), hierarchy);
            }
        };
    }

    private static Object minValue(
        Evaluator evaluator, TupleList members, Calc calc)
    {
        return extremeValue(evaluator, members, calc, false);
    }

    private static Object maxValue(
        Evaluator evaluator, TupleList members, Calc calc)
    {
        return extremeValue(evaluator, members, calc, true);
    }

    /**
     * Evaluates the expression for each tuple in the set and returns the
     * minimum or maximum value. Handles both Number and Date values at
     * runtime based on the actual type of the first non-null value.
     */
    private static Object extremeValue(
        Evaluator evaluator, TupleList members, Calc calc, boolean max)
    {
        SetWrapper sw = evaluateSet(evaluator, members, calc);
        if (sw.errorCount > 0) {
            return Double.NaN;
        }
        final List v = sw.v;
        final int size = v.size();
        if (size == 0) {
            return Util.nullValue;
        }
        Object extreme = v.get(0);
        if (extreme instanceof Date) {
            for (int i = 1; i < size; i++) {
                Date d = (Date) v.get(i);
                if (max
                    ? d.getTime() > ((Date) extreme).getTime()
                    : d.getTime() < ((Date) extreme).getTime())
                {
                    extreme = d;
                }
            }
            return extreme;
        }
        double extremeDouble = ((Number) extreme).doubleValue();
        for (int i = 1; i < size; i++) {
            double iValue = ((Number) v.get(i)).doubleValue();
            if (max ? iValue > extremeDouble : iValue < extremeDouble) {
                extremeDouble = iValue;
            }
        }
        return extremeDouble;
    }

    /**
     * Custom resolver that tries DateTime first, then Numeric for the
     * 2-arg form. The 1-arg form delegates to the standard "fnx" signature.
     *
     * <p>Extends ReflectiveMultiResolver for binary compatibility with
     * BuiltinFunTable which references these fields with specific types.
     */
    private static class MinMaxResolverImpl extends ReflectiveMultiResolver {
        MinMaxResolverImpl(String name, String signature, String description) {
            // Signatures drive FunInfo introspection (XMLA MDSCHEMA_FUNCTIONS,
            // CmdRunner help). Runtime resolution is handled by the overridden
            // resolve() method below, which bypasses the signatures array.
            super(name, signature, description,
                new String[]{"fnx", "fnxn", "fDxD"},
                MinMaxFunDef.class);
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            if (args.length == 1) {
                return super.resolve(args, validator, conversions);
            }
            if (args.length == 2) {
                FunDef dateFun = resolveSetAndExpression(
                    args, validator, conversions, Category.DateTime);
                if (dateFun != null) {
                    return dateFun;
                }
                return resolveSetAndExpression(
                    args, validator, conversions, Category.Numeric);
            }
            return null;
        }

        private FunDef resolveSetAndExpression(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions,
            int expressionCategory)
        {
            conversions.clear();
            if (!validator.canConvert(
                    0, args[0], Category.Set, conversions))
            {
                return null;
            }
            if (!validator.canConvert(
                    1, args[1], expressionCategory, conversions))
            {
                return null;
            }
            FunDef dummy = createDummyFunDef(
                this, expressionCategory, args);
            return new MinMaxFunDef(dummy);
        }
    }
}

// End MinMaxFunDef.java
