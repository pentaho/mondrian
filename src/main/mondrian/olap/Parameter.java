/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.olap.type.Type;

/**
 * Parameter to a Query.
 *
 * <p>A parameter is not an expression; see {@link mondrian.mdx.ParameterExpr}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jul 22, 2006
 */
public interface Parameter {
    /**
     * Returns the scope where this parmater is defined.
     */
    Scope getScope();

    /**
     * Returns the type of this Parameter.
     */
    Type getType();

    /**
     * Returns the expression which provides the default value for this
     * Parameter.
     */
    Exp getDefaultExp();

    /**
     * Returns the name of this Parameter.
     */
    String getName();

    /**
     * Returns the description of this Parameter.
     */
    String getDescription();

    /**
     * Returns whether the value of this Parameter can be modified in a query.
     */
    boolean isModifiable();

    /**
     * Returns the value of this parameter. If {@link #setValue(Object)} has
     * not been called, and the parameter still has its default value, returns
     * null.
     *
     * <p>The type of the value is (depending on the type of the parameter)
     * a {@link String}, {@link Number}, or {@link Member}.
     */
    Object getValue();

    /**
     * Sets the value of this parameter.
     *
     * @param value Value of the parameter; must be a {@link String},
     *   a {@link Double}, or a {@link mondrian.olap.Member}
     */
    void setValue(Object value);

    /**
     * Scope where a parameter is defined.
     */
    static class Scope extends EnumeratedValues.BasicValue {
        private Scope(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final int System_ordinal = 0;
        public static final Scope System = new Scope("System", System_ordinal);

        public static final int Schema_ordinal = 1;
        public static final Scope Schema = new Scope("Schema", Schema_ordinal);

        public static final int Connection_ordinal = 2;
        public static final Scope Connection = new Scope("Connection", Connection_ordinal);

        public static final int Statement_ordinal = 3;
        public static final Scope Statement = new Scope("Statement", Statement_ordinal);
    }
}

// End Parameter.java
