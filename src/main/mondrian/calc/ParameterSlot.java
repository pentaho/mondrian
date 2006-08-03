/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Parameter;

/**
 * Implementation of a parameter.
 *
 * @author jhyde
 * @version $Id$
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
     */
    void setParameterValue(Object value);

    /**
     * Returns the value of this parameter.
     *
     * <p>NOTE: This method will be removed when we store parameter values
     * in the {@link mondrian.olap.Result} rather than in the
     * {@link mondrian.olap.Query}.
     */
    Object getParameterValue();

    void setCachedDefaultValue(Object value);

    Object getCachedDefaultValue();
}

// End ParameterSlot.java
