/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap;

/**
 * Strategies for applying solve order, exposed via the property
 * {@link MondrianProperties#SolveOrderMode}.
 */
public enum SolveOrderMode {

    /**
     * The SOLVE_ORDER value is absolute regardless of
     * where it is defined; e.g. a query defined calculated
     * member with a SOLVE_ORDER of 1 always takes precedence
     * over a cube defined value of 2.
     *
     * <p>Compatible with Analysis Services 2000, and default behavior
     * up to mondrian-3.0.3.
     */
    ABSOLUTE,

    /**
     * Cube calculated members are resolved before any session
     * scope calculated members, and session scope members are
     * resolved before any query defined calculation.  The
     * SOLVE_ORDER value only applies within the scope in which
     * it was defined.
     *
     * <p>Compatible with Analysis Services 2005, and default behavior
     * from mondrian-3.0.4 and later.
     */
    SCOPED
}

// End SolveOrderMode.java
