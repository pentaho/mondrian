/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap;

import mondrian.olap.type.Type;

/**
 * Parameter to a Query.
 *
 * <p>A parameter is not an expression; see {@link mondrian.mdx.ParameterExpr}.
 *
 * @author jhyde
 * @since Jul 22, 2006
 */
public interface Parameter {
    /**
     * Returns the scope where this parameter is defined.
     *
     * @return Scope of the parameter
     */
    Scope getScope();

    /**
     * Returns the type of this Parameter.
     *
     * @return Type of the parameter
     */
    Type getType();

    /**
     * Returns the expression which provides the default value for this
     * Parameter. Never null.
     *
     * @return Default value expression of the parameter
     */
    Exp getDefaultExp();

    /**
     * Returns the name of this Parameter.
     *
     * @return Name of the parameter
     */
    String getName();

    /**
     * Returns the description of this Parameter.
     *
     * @return Description of the parameter
     */
    String getDescription();

    /**
     * Returns whether the value of this Parameter can be modified in a query.
     *
     * @return Whether parameter is modifiable
     */
    boolean isModifiable();

    /**
     * Returns the value of this parameter.
     *
     * <p>If {@link #setValue(Object)} has not been called, returns the default
     * value of this parameter.
     *
     * <p>The type of the value is (depending on the type of the parameter)
     * a {@link String}, {@link Number}, or {@link Member}.
     *
     * @return The value of this parameter
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
     * Returns whether the value of this parameter has been set.
     *
     * <p>If the value has not been set, this parameter will return its default
     * value.
     *
     * <p>Setting a parameter to {@code null} is not equivalent to unsetting it.
     * To unset a parameter, call {@link #unsetValue}.
     *
     * @return Whether this parameter has been assigned a value
     */
    boolean isSet();

    /**
     * Unsets the value of this parameter.
     *
     * <p>After calling this method, the parameter will revert to its default
     * value, as if {@link #setValue(Object)} had not been called, and
     * {@link #isSet()} will return {@code false}.
     */
    void unsetValue();

    /**
     * Scope where a parameter is defined.
     */
    enum Scope {
        System,
        Schema,
        Connection,
        Statement
    }
}

// End Parameter.java
