/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
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
    /** No access to an object and its children. */
    NONE,
    /**
     * A grant that covers none of the children
     * unless explicitly granted.
     */
    CUSTOM,
    /**
     * Grant that covers all children except those denied.
     * (internal use only)
     */
    RESTRICTED,
    /** Access to all shared dimensions (applies to schema grant). */
    ALL_DIMENSIONS,
    /** All access to an object and its children. */
    ALL;
    public String toString() {
        return this.name();
    };
}

// End Access.java
