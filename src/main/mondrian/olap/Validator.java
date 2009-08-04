/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.olap.fun.Resolver;
import mondrian.olap.type.Type;
import mondrian.mdx.ParameterExpr;

import java.util.List;

/**
 * Provides context necessary to resolve identifiers to objects, function
 * calls to specific functions.
 *
 * <p>An expression calls {@link #validate} on each of its children,
 * which in turn calls {@link Exp#accept}.
 *
 * @author jhyde
 * @version $Id$
 */
public interface Validator {
    /**
     * Returns the {@link Query} which is being validated.
     */
    Query getQuery();

    /**
     * Validates an expression, and returns the expression it resolves to.
     *
     * @param exp Expression to validate
     * @param scalar Whether the context requires that the expression is
     *   evaluated to a value, as opposed to a tuple
     */
    Exp validate(Exp exp, boolean scalar);

    /**
     * Validates a usage of a parameter.
     *
     * <p>It must resolve to the same object (although sub-objects may change).
     */
    void validate(ParameterExpr parameterExpr);

    /**
     * Validates a child member property.
     *
     * <p>It must resolve to the same object (although sub-objects may change).
     */
    void validate(MemberProperty memberProperty);

    /**
     * Validates an axis.
     *
     * It must resolve to the same object (although sub-objects may change).
     */
    void validate(QueryAxis axis);

    /**
     * Validates a formula.
     *
     * It must resolve to the same object (although sub-objects may change).
     */
    void validate(Formula formula);

    /**
     * Returns whether the current context requires an expression.
     */
    boolean requiresExpression();

    /**
     * Returns whether we can convert an argument to a parameter type.
     *
     * @param fromExp argument type
     * @param to   parameter type
     * @param conversions List of conversions performed;
     *             method adds an element for each non-trivial conversion (for
     *             example, converting a member to a level).
     * @return Whether we can convert an argument to a parameter type
     */
    boolean canConvert(
        Exp fromExp,
        int to,
        List<Resolver.Conversion> conversions);

    /**
     * Returns the table of function and operator definitions.
     */
    FunTable getFunTable();

    /**
     * Creates or retrieves the parameter corresponding to a "Parameter" or
     * "ParamRef" function call.
     */
    Parameter createOrLookupParam(
        boolean definition,
        String name,
        Type type,
        Exp defaultExp,
        String description);

    /**
     * Resolves a function call to a particular function. If the function is
     * overloaded, returns as precise a match to the argument types as
     * possible.
     */
    FunDef getDef(
        Exp[] args,
        String name,
        Syntax syntax);

    /**
     * Whether to resolve function name and arguments to a function definition
     * each time a node is validated, not just the first time.
     *
     * <p>Default implementation returns {@code false}.
     *
     * @return whether to resolve function each time
     */
    boolean alwaysResolveFunDef();
}

// End Validator.java
