/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2005 Kana Software, Inc. and others.
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
 **/
public interface NameResolver {

    Cube getCube();

    /**
     * Looks up the child of <code>parent</code> called <code>s</code>; returns
     * null if no element is found.
     **/
    OlapElement lookupChild(OlapElement parent, String s);

}

// End NameResolver.java
