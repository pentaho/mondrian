/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

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
     */
    Exp validate(Exp exp);

    /**
     * Validates a parameter, and returns the new parameter it resolves to.
     */
    Parameter validate(Parameter parameter);

    /**
     * Validates a child member property.
     *
     * It must resolve to the same object (although sub-objects may change).
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
     * Returns the table of function and operator definitions.
     */
    FunTable getFunTable();

    /**
     * Creates or retrieves the parameter corresponding to a "Parameter" or
     * "ParamRef" function call.
     */
    Parameter createOrLookupParam(FunCall call);
}

// End Validator.java
