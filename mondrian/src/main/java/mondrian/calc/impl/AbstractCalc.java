/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.calc.impl;

import mondrian.calc.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapHierarchy;

import java.util.*;

/**
 * Abstract implementation of the {@link mondrian.calc.Calc} interface.
 *
 * @author jhyde
 * @since Sep 27, 2005
 */
public abstract class AbstractCalc implements Calc {
    private final Calc[] calcs;
    protected final Type type;
    protected final Exp exp;

    /**
     * Creates an AbstractCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    protected AbstractCalc(Exp exp, Calc[] calcs) {
        assert exp != null;
        this.exp = exp;
        this.calcs = calcs;
        this.type = exp.getType();
    }

    public Type getType() {
        return type;
    }

    /**
     * {@inheritDoc}
     *
     * Default implementation just does 'instanceof TargetClass'. Subtypes that
     * are wrappers should override.
     */
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    /**
     * {@inheritDoc}
     *
     * Default implementation just casts to TargetClass.
     * Subtypes that are wrappers should override.
     */
    public <T> T unwrap(Class<T> iface) {
        return iface.cast(this);
    }

    public void accept(CalcWriter calcWriter) {
        calcWriter.visitCalc(this, getName(), getArguments(), getCalcs());
    }

    /**
     * Returns the name of this expression type, used when serializing an
     * expression to a string.
     *
     * <p>The default implementation tries to extract a name from a function
     * call, if any, then prints the last part of the class name.
     */
    protected String getName() {
        String name = lastSegment(getClass());
        if (isDigits(name)
            && exp instanceof ResolvedFunCall)
        {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            name = funCall.getFunDef().getName();
        }
        return name;
    }

    /**
     * Returns the last segment of a class name.
     *
     * <p>Examples:
     * lastSegment("com.acme.Foo") = "Foo"
     * lastSegment("com.acme.Foo$Bar") = "Bar"
     * lastSegment("com.acme.Foo$1") = "1"
     *
     * @param clazz Class
     * @return Last segment of class name
     */
    private String lastSegment(Class clazz) {
        final String name = clazz.getName();
        int dot = name.lastIndexOf('.');
        int dollar = name.lastIndexOf('$');
        int dotDollar = Math.max(dot, dollar);
        if (dotDollar >= 0) {
            return name.substring(dotDollar + 1);
        }
        return name;
    }

    private static boolean isDigits(String name) {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if ("0123456789".indexOf(c) < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns this expression's child expressions.
     */
    public Calc[] getCalcs() {
        return calcs;
    }

    public boolean dependsOn(Hierarchy hierarchy) {
        return anyDepends(getCalcs(), hierarchy);
    }

    /**
     * Returns true if one of the calcs depends on the given dimension.
     */
    public static boolean anyDepends(Calc[] calcs, Hierarchy hierarchy) {
        for (Calc calc : calcs) {
            if (calc != null && calc.dependsOn(hierarchy)) {
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
        Calc[] calcs, Hierarchy hierarchy)
    {
        if (calcs.length == 0) {
            return false;
        }
        if (calcs[0].dependsOn(hierarchy)) {
            return true;
        }
        if (calcs[0].getType().usesHierarchy(hierarchy, true)) {
            return false;
        }
        for (int i = 1; i < calcs.length; i++) {
            Calc calc = calcs[i];
            if (calc != null && calc.dependsOn(hierarchy)) {
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
        Calc[] calcs, Hierarchy hierarchy)
    {
        boolean result = true;
        for (Calc calc : calcs) {
            if (calc != null) {
                if (calc.dependsOn(hierarchy)) {
                    return true;
                }
                if (calc.getType().usesHierarchy(hierarchy, true)) {
                    result = false;
                }
            }
        }
        return result;
    }

    /**
     * Returns any other arguments to this calc.
     *
     * @return Collection of name/value pairs, represented as a map
     */
    protected final Map<String, Object> getArguments() {
        final Map<String, Object> argumentMap =
            new LinkedHashMap<String, Object>();
        collectArguments(argumentMap);
        return argumentMap;
    }

    /**
     * Collects any other arguments to this calc.
     *
     * <p>The default implementation returns name, class, type, resultStyle.
     * A subclass must call super, but may add other arguments.
     *
     * @param arguments Collection of name/value pairs, represented as a map
     */
    protected void collectArguments(Map<String, Object> arguments) {
        arguments.put("name", getName());
        arguments.put("class", getClass());
        arguments.put("type", getType());
        arguments.put("resultStyle", getResultStyle());
    }

    /**
     * Returns a simplified evalator whose context is the same for every
     * dimension which an expression depends on, and the default member for
     * every dimension which it does not depend on.
     *
     * <p>The default member is often the 'all' member, so this evaluator is
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
        final List<RolapHierarchy> hierarchies =
            ((RolapEvaluator) evaluator).getCube().getHierarchies();
        for (RolapHierarchy hierarchy : hierarchies) {
            final Member member = ev.getContext(hierarchy);
            if (member.isAll()) {
                continue;
            }
            if (calc.dependsOn(hierarchy)) {
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
                ev = evaluator.push();
            }
            ev.setContext(unconstrainedMember);
        }
        return ev;
    }

    public ResultStyle getResultStyle() {
        return ResultStyle.VALUE;
    }
}

// End AbstractCalc.java
