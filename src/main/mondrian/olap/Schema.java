/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 October, 2002
*/
package mondrian.olap;

/**
 * A <code>Schema</code> is a collection of cubes, shared dimensions, and roles.
 */
public interface Schema {
    /**
     * Returns the name of this schema.
     * @post return != null
     * @post return.length() > 0
     */
    String getName();
	/**
	 * Finds a cube called <code>cube</code> in this schema; if no cube
	 * exists, <code>failIfNotFound</code> controls whether to raise an error
	 * or return <code>null</code>.
	 **/
	Cube lookupCube(String cube,boolean failIfNotFound);

	/**
	 * Returns a list of all cubes in this schema.
	 **/
	Cube[] getCubes();

	/**
	 * Returns a list of shared dimensions in this schema.
	 */
	Hierarchy[] getSharedHierarchies();

	/**
	 * Creates a dimension in the given cube by parsing an XML string. The XML
	 * string must be either a &lt;Dimension&gt; or a &lt;DimensionUsage&gt;.
	 * Returns the dimension created.
	 **/
	Dimension createDimension(Cube cube, String xml);

	/**
	 * Creates a {@link SchemaReader} without any access control.
	 */
	SchemaReader getSchemaReader();

	/**
	 * Finds a role with a given name in the current catalog, or returns
	 * <code>null</code> if no such role exists.
	 */
	Role lookupRole(String role);
}

// End Schema.java
