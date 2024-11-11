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
 * An object which implements <code>Walkable</code> can be tree-walked by
 * {@link Walker}.
 */
interface Walkable {
    /**
     * Returns an array of the object's children.  Those which are not {@link
     * Walkable} are ignored.
     */
    Object[] getChildren();
}

// End Walkable.java
