/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;

import mondrian.rolap.sql.SqlQuery;

/**
 * A <code>MemberSource</code> is an object which can provide sets of members,
 * and qualify queries. Compare to {@link MemberReader}, which can operate on
 * those sets of members.
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 **/
interface MemberSource {
	/** Returns the hierarchy that this source is reading for. **/
	RolapHierarchy getHierarchy();
	/** Sets the cache to use for subsequent calls to {@link #getMembers}, and
	 * so forth. **/
	void setCache(MemberCache cache);
	/** Returns all members of this hierarchy, sorted by ordinal. **/
	RolapMember[] getMembers();
	/** Returns all members of this hierarchy which do not have a parent,
	 * sorted by ordinal. **/
	RolapMember[] getRootMembers();
	/** Returns all members which are a child of one of the members in
	 * <code>parentMembers</code>, sorted by ordinal. **/
	RolapMember[] getMemberChildren(RolapMember[] parentMembers);
	/** Makes the necessary modifications to <code>query</code> to efficiently
	 * access only rows which relate to <code>member</code>. **/
	void qualifyQuery(SqlQuery query, RolapMember member);
	/** Returns number of members in this hierarchy. May be an estimate. **/
	int getMemberCount();
}

// End MemberSource.java
