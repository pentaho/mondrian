/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 1999
*/

package mondrian.olap;

/**
 * A <code>Dimension</code> represents a dimension of a cube.
 **/
public interface Dimension extends OlapElement {
	final String MEASURES_UNIQUE_NAME = "[Measures]";
	final String MEASURES_NAME = "Measures";
	/** Returns an array of the hierarchies which belong to this dimension. **/
	Hierarchy[] getHierarchies();
	/** Returns whether this is the measures dimension. **/
	boolean isMeasures();
	/** Returns the type of this dimension: {@link #STANDARD} or
	 * {@link #TIME} */
	int getDimensionType();
	static final int STANDARD = 0;
	static final int TIME = 1;
	/** Returns dimension's ordinal within a given cube. [Measures] are
	 * always 0. */
	int getOrdinal(Cube cube);
	/** Returns the schema this dimension belongs to. **/
	Schema getSchema();
}

// End Dimension.java
