/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2006 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * <code>Access</code> enumerates the allowable access rights.
 *
 * @author jhyde
 * @since Feb 21, 2003
 */
public enum Access {
    /** No access to an object. */
    NONE,
    /** Custom access to an object (described by other parameters). */
    CUSTOM,
    /** Access to all shared dimensions (applies to schema grant). */
    ALL_DIMENSIONS,
    /** All access to an object. */
    ALL;
}

// End Access.java