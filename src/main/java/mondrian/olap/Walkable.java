/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2005 Pentaho and others
// All Rights Reserved.
//
// jhyde, 1 March, 1999
*/
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
