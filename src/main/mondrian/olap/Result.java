/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

/**
 * A <code>Result</code> is the result of running an MDX query. See {@link
 * Connection#execute}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public interface Result
{
	/** Returns the non-slicer axes. **/
	Axis[] getAxes();
	/** Returns the slicer axis. **/
	Axis getSlicerAxis();
	Cell getCell(int[] pos);
	/**
	 * Returns the current member of a given dimension at a given location.
	 **/
	Member getMember(int[] pos, Dimension dimension);
	void print(java.io.PrintWriter pw);
	void close();
};

// End Result.java
