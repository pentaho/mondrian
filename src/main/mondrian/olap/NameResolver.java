/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2002 Kana Software, Inc. and others.
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
	 * Looks up the child of <code>parent</code> called <code>s</code>; if no
	 * element is found, if <code>failIfNotFound</code> fails, otherwise
	 * returns null.
	 **/
	OlapElement lookupChild(
		OlapElement parent, String s, boolean failIfNotFound);

	/**
	 * Looks up a member by its fully-qualified name.
	 **/
	Member lookupMemberCompound(String[] names, boolean failIfNotFound);

	/**
	 * Looks up a member by its unique name. If you wish to look up a member by
	 * its fully-qualified name, use {@link #lookupMemberCompound}.
	 **/
	Member lookupMemberByUniqueName(String s, boolean failIfNotFound);

	/**
	 * Looks up a member whose unique name is <code>s</code> from cache.  Does
	 * not make a (potentially expensive) call to Plato.  If the member is not
	 * in cache, returns null.
	 **/
	Member lookupMemberFromCache(String s);
}

// End NameResolver.java
