/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.calc.Calc;
import mondrian.calc.CalcWriter;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.ResolvedFunCall;

import java.io.PrintWriter;
import java.util.List;
import java.util.Collections;

/**
 * Abstract implementation of the {@link mondrian.calc.Calc} interface.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public abstract class AbstractCalc implements Calc {
    protected final Type type;
    protected final Exp exp;

    protected AbstractCalc(Exp exp) {
        assert exp != null;
        this.exp = exp;
        this.type = exp.getType();
    }

    public Type getType() {
        return type;
    }

    public void accept(CalcWriter calcWriter) {
        final PrintWriter pw = calcWriter.getWriter();
        String name = getName();
        pw.print(name);
        final Calc[] calcs = getCalcs();
        final List<Object> argumentList = getArguments();
        if (calcs.length > 0 || !argumentList.isEmpty()) {
           pw.print("(");
            int k = 0;
            for (Calc calc : calcs) {
                if (k++ > 0) {
                    pw.print(", ");
                }
                calc.accept(calcWriter);
            }
            for (Object o : argumentList) {
                if (k++ > 0) {
                    pw.print(", ");
                }
                pw.print(o);
            }
            pw.print(")");
        }
    }

    /**
     * Returns the name of this expression type, used when serializing an
     * expression to a string.
     *
     * <p>The default implementation tries to extract a name from a function call,
     * if any, then prints the last part of the class name.
     */
    protected String getName() {
        String name;
        if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            name = funCall.getFunDef().getName();
        } else {
            name = getClass().getName();
            int dot = name.lastIndexOf('.');
            int dollar = name.lastIndexOf('$');
            int dotDollar = Math.max(dot, dollar);
            if (dotDollar >= 0) {
                name = name.substring(dotDollar + 1);
            }
        }
        return name;
    }

    /**
     * Returns this expression's child expressions.
     */
    public abstract Calc[] getCalcs();

    public boolean dependsOn(Dimension dimension) {
        return anyDepends(getCalcs(), dimension);
    }

    /**
     * Returns true if one of the calcs depends on the given dimension.
     */
    public static boolean anyDepends(Calc[] calcs, Dimension dimension) {
        for (Calc calc : calcs) {
            if (calc != null && calc.dependsOn(dimension)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if calc[0] depends on dimension,
     * else false if calc[0] returns dimension,
     * else true if any of the other calcs depend on dimension.
     *
     * <p>Typical application: <code>Aggregate({Set}, {Value Expression})</code>
     * depends upon everything {Value Expression} depends upon, except the
     * dimensions of {Set}.
     */
    public static boolean anyDependsButFirst(
            Calc[] calcs, Dimension dimension) {
        if (calcs.length == 0) {
            return false;
        }
        if (calcs[0].dependsOn(dimension)) {
            return true;
        }
        if (calcs[0].getType().usesDimension(dimension, true)) {
            return false;
        }
        for (int i = 1; i < calcs.length; i++) {
            Calc calc = calcs[i];
            if (calc != null && calc.dependsOn(dimension)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if any of the calcs depend on dimension,
     * else false if any of the calcs return dimension,
     * else true.
     */
    public static boolean butDepends(
            Calc[] calcs, Dimension dimension) {
        boolean result = true;
        for (Calc calc : calcs) {
            if (calc != null) {
                if (calc.dependsOn(dimension)) {
                    return true;
                }
                if (calc.getType().usesDimension(dimension, true)) {
                    result = false;
                }
            }
        }
        return result;
    }

    /**
     * Returns any other arguments to this calc.
     * The default implementation returns the empty list.
     */
    public List<Object> getArguments() {
        return Collections.emptyList();
    }

    /**
     * Returns a simplified evalator whose context is the same for every
     * dimension which an expression depends on, and the default member for
     * every dimension which it does not depend on.
     *
     * <p>The default member is oftne the 'all' member, so this evaluator is
     * usually the most efficient context in which to evaluate the expression.
     *
     * @param calc
     * @param evaluator
     */
    public static Evaluator simplifyEvaluator(Calc calc, Evaluator evaluator) {
        if (evaluator.isNonEmpty()) {
            // If NON EMPTY is present, we cannot simplify the context, because
            // we have to assume that the expression depends on everything.
            // TODO: Bug 1456418: Convert 'NON EMPTY Crossjoin' to
            // 'NonEmptyCrossJoin'.
            return evaluator;
        }
        int changeCount = 0;
        Evaluator ev = evaluator;
        final Dimension[] dimensions = evaluator.getCube().getDimensions();
        for (Dimension dimension : dimensions) {
            final Member member = ev.getContext(dimension);
            if (member.isAll()) {
                continue;
            }
            if (calc.dependsOn(dimension)) {
                continue;
            }
            final Member unconstrainedMember =
                member.getHierarchy().getDefaultMember();
            if (member == unconstrainedMember) {
                // This is a hierarchy without an 'all' member, and the context
                // is already the default member.
                continue;
            }
            if (changeCount++ == 0) {
                ev = evaluator.push(unconstrainedMember);
            } else {
                ev.setContext(unconstrainedMember);
            }
        }
        return ev;
    }

    public ExpCompiler.ResultStyle getResultStyle() {
        return ExpCompiler.ResultStyle.VALUE;
    }
}

// End AbstractCalc.java
