/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.olap.fun.ParameterFunDef;
import mondrian.olap.type.Type;
import mondrian.mdx.ParameterExpr;

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
     * @param conversionCount in/out count of number of conversions performed;
     *             is incremented if the conversion is non-trivial (for
     *             example, converting a member to a level).
     */
    boolean canConvert(Exp fromExp, int to, int[] conversionCount);

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
}

// End Validator.java
