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
import java.io.*;
import java.util.*;

/**
 * Wrapper around an ADO MD hierarchy.
 **/
public interface Hierarchy extends OlapElement {
	Dimension getDimension();
	Level[] getLevels();
	Member[] getRootMembers();
	Member getDefaultMember();
	Member getNullMember();
	boolean hasAll();
	/**
	 * Creates a member of this hierarchy. If this is the measures hierarchy, a
	 * calculated member is created, and <code>formula</code> must not be null.
	 **/
	Member createMember(
		Member parent, Level level, String name, Formula formula);
	/**
	 * Returns number of members in hierarchy or nMaxMembers+1, whichever
	 * comes first. Set limit to -1 to know total number of members.
	 **/
	int getMembersCount(int nMaxMembers);
	/** Find a named level in this hierarchy. */
	Level lookupLevel(String s);
/*
	void formatMembersXml(
		PrintWriter pw, CubeAccess cubeAccess, Namer namer,
		String startMemberName, String direction, int depth);
*/
	/**
	 * Returns direct children of <code>member</code>.
	 **/
	Member[] getChildMembers(Member member);
	/**
	 * Returns direct children for every member in array. This bulk method may
	 * be much more efficient than {@link #lookupChild}.
	 **/
	Member[] getChildMembers(Member[] parentMembers);
	/**
	 * Looks up a member by its unique name.
	 **/
	Member lookupMemberByUniqueName(String s, boolean failIfNotFound);
	/**
	 * Appends to <code>list</code> all members between <code>startMember</code>
	 * and <code>endMember</code> (inclusive) which belong to
	 * <code>level</code>.
	 */
	void getMemberRange(Level level, Member startMember, Member endMember, List list);
}

// End Hierarchy.java

