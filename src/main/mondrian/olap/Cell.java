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
 * A <code>Cell</code> is an item in the grid of a {@link Result}.  It is
 * returned by {@link Result#getCell}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public interface Cell {
	String getFormattedValue();
	boolean isNull();
}


// End Cell.java
