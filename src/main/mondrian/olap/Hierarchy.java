/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 March, 1999
*/

package mondrian.olap;
import java.util.List;

/**
 * Wrapper around an ADO MD hierarchy.
 **/
public interface Hierarchy extends OlapElement {
	Dimension getDimension();
	Level[] getLevels();
	Member getDefaultMember();
	Member getNullMember();
	boolean hasAll();
	/**
	 * Creates a member of this hierarchy. If this is the measures hierarchy, a
	 * calculated member is created, and <code>formula</code> must not be null.
	 **/
	Member createMember(
		Member parent, Level level, String name, Formula formula);

	/** Find a named level in this hierarchy. */
	Level lookupLevel(String s);
}

// End Hierarchy.java

