/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2000-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 March, 2000
*/

package mondrian.olap;

/**
 * Interface for a class which can lookup dimensions, hierarchies, levels,
 * members.  {@link Cube} is the most typical implementor, but {@link Query}
 * also implements this interface, looking at members defined in its WITH
 * clause before looking to its cube.
 */
public interface NameResolver {

    Cube getCube();

    /**
     * Looks up the child of <code>parent</code> called <code>s</code>; returns
     * null if no element is found.
     */
    OlapElement lookupChild(OlapElement parent, String s);

}

// End NameResolver.java
