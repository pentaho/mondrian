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
