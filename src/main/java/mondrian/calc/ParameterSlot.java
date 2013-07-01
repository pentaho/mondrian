/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2010 Pentaho
// All Rights Reserved.
*/
package mondrian.calc;

import mondrian.olap.Parameter;

/**
 * Implementation of a parameter.
 *
 * @author jhyde
 * @since Jul 25, 2006
 */
public interface ParameterSlot {
    /**
     * Returns the unique index of the slot.
     */
    int getIndex();

    /**
     * Returns a compiled expression to compute the default value of the
     * parameter.
     */
    Calc getDefaultValueCalc();

    /**
     * Returns the parameter.
     */
    Parameter getParameter();

    /**
     * Sets the value of this parameter.
     *
     * <p>NOTE: This method will be removed when we store parameter values
     * in the {@link mondrian.olap.Result} rather than in the
     * {@link mondrian.olap.Query}.
     *
     * @param value New value
     * @param assigned Whether {@link #isParameterSet()} should return true;
     *   supply value {@code false} if this is an internal assignment, to
     *   remember the default value
     */
    void setParameterValue(Object value, boolean assigned);

    /**
     * Returns the value of this parameter.
     *
     * <p>NOTE: This method will be removed when we store parameter values
     * in the {@link mondrian.olap.Result} rather than in the
     * {@link mondrian.olap.Query}.
     */
    Object getParameterValue();

    /**
     * Returns whether the parameter has been assigned a value. (That value
     * may be null.)
     *
     * @return Whether parmaeter has been assigned a value.
     */
    boolean isParameterSet();

    void setCachedDefaultValue(Object value);

    Object getCachedDefaultValue();

    /**
     * Unsets the parameter value.
     */
    void unsetParameterValue();
}

// End ParameterSlot.java
