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
	RolapMember[] getPeriodsToDate(RolapLevel level, RolapMember member);
};



// End MemberReader.java
