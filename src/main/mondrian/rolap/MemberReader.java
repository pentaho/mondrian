/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

import java.util.List;

/**
 * todo:
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
interface MemberReader extends MemberSource
{
	/** Returns the member <code>n</code> after <code>member</code> in the same
	 * level (or before, if <code>n</code> is negative). Returns {@link
	 * Hierarchy#getNullMember} if we run off the beginning or end of the
	 * level. **/
	RolapMember getLeadMember(RolapMember member, int n);
	/** Returns all of the members in <code>level</code> whose ordinal lies
	 * between <code>startOrdinal</code> and <code>endOrdinal</code>. **/
	RolapMember[] getMembersInLevel(
		RolapLevel level, int startOrdinal, int endOrdinal);
	/** Writes all members between <code>startMember</code> and
	 * <code>endMember</code> into <code>list</code>. */
	void getMemberRange(RolapLevel level, RolapMember startMember,
						RolapMember endMember, List list);
	/** Compares two members according to their order in a prefix ordered
	 * traversal. If <code>siblingsAreEqual</code>, then two members with the
	 * same parent will compare equal.
	 *
	 * @return less than zero if m1 occurs before m2,
	 *     greater than zero if m1 occurs after m2,
	 *     zero if m1 is equal to m2, or if <code>siblingsAreEqual</code> and
	 *         m1 and m2 have the same parent
	 */
	int compare(RolapMember m1, RolapMember m2, boolean siblingsAreEqual);
}

// End MemberReader.java
