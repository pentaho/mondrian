/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1999-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
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
