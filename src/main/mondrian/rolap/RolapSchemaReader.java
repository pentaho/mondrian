/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 24, 2003
*/
package mondrian.rolap;

import mondrian.olap.*;

import java.util.List;
import java.util.ArrayList;

/**
 * A <code>RolapSchemaReader</code> allows you to read schema objects while
 * observing the access-control profile specified by a given role.
 *
 * @author jhyde
 * @since Feb 24, 2003
 * @version $Id$
 **/
class RolapSchemaReader implements SchemaReader {
	private Role role;

	RolapSchemaReader(Role role) {
		this.role = role;
	}
	public Member[] getHierarchyRootMembers(Hierarchy hierarchy) {
		return getLevelMembers(hierarchy.getLevels()[0]);
	}

	public Member getMemberByUniqueName(Hierarchy hierarchy, String[] uniqueNameParts, boolean failIfNotFound) {
		return getMemberReader(hierarchy).lookupMember(uniqueNameParts, failIfNotFound);
	}

	private MemberReader getMemberReader(Hierarchy hierarchy) {
		return ((RolapHierarchy) hierarchy).memberReader;
	}

	public void getMemberRange(Level level, Member startMember, Member endMember, List list) {
		getMemberReader(level.getHierarchy()).getMemberRange(
				(RolapLevel) level, (RolapMember) startMember, (RolapMember) endMember, list);
	}

	public int compareMembersHierarchically(Member m1, Member m2) {
		final RolapHierarchy hierarchy = (RolapHierarchy) m1.getHierarchy();
		Util.assertPrecondition(hierarchy == m2.getHierarchy());
		return getMemberReader(hierarchy).compare((RolapMember) m1, (RolapMember) m2, true);
	}

	public Member[] getMemberChildren(Member member) {
		ArrayList children = new ArrayList();
		getMemberReader(member.getHierarchy()).getMemberChildren(
				(RolapMember) member, children);
		return RolapUtil.toArray(children);
	}

	public Member[] getMemberChildren(Member[] members) {
		if (members.length == 0) {
			return RolapUtil.emptyMemberArray;
		} else {
			final MemberReader memberReader = getMemberReader(members[0].getHierarchy());
			ArrayList children = new ArrayList();
			for (int i = 0; i < members.length; i++) {
				memberReader.getMemberChildren((RolapMember) members[i], children);
			}
			return RolapUtil.toArray(children);
		}
	}

	public Cube getCube() {
		throw new UnsupportedOperationException();
	}

	public Member getMemberByUniqueName(String uniqueName, boolean failIfNotFound) {
		return getCube().lookupMemberByUniqueName(uniqueName, failIfNotFound);
	}

	public Member getLeadMember(Member member, int n) {
		return getMemberReader(member.getHierarchy()).getLeadMember((RolapMember) member, n);
	}

	public Member[] getLevelMembers(Level level) {
		final List membersInLevel = getMemberReader(level.getHierarchy()).getMembersInLevel(
					(RolapLevel) level, 0, Integer.MAX_VALUE);
		return RolapUtil.toArray(membersInLevel);
	}
}

// End RolapSchemaReader.java