/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 1999
*/

package mondrian.olap;
import java.io.*;
import java.util.*;

/**
 * A <code>Dimension</code> represents a dimension of a cube.
 **/
public interface Dimension extends OlapElement {

	final String CONST_MEASURES = "[Measures]";
	Hierarchy[] getHierarchies();
	boolean isMeasures();
	int getDimensionType();
	static final int STANDARD = 0;
	static final int TIME = 1;
	int getOrdinal();
}

// End Dimension.java
