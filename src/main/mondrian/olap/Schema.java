/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 October, 2002
*/
package mondrian.olap;

/**
 * A <code>Schema</code> lists cubes and shared dimensions.
 */
public interface Schema {
	/**
	 * Find a cube called <code>cube</code> in the current catalog; if no cube
	 * exists, <code>failIfNotFound</code> controls whether to raise an error
	 * or return null.
	 **/
	Cube lookupCube(String cube,boolean failIfNotFound);

	/**
	 * Returns a list of all cubes in a given database.
	 **/
	Cube[] getCubes();

	/**
	 * Returns a list of shared dimensions.
	 */
	Hierarchy[] getSharedHierarchies();

	/**
	 * Creates a dimension in the given cube by parsing an XML string. The XML
	 * string must be either a &lt;Dimension&gt; or a &lt;DimensionUsage&gt;.
	 * Returns the dimension created.
	 **/
	Dimension createDimension(Cube cube, String xml);

}

// End Schema.java
