/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 26, 2003
*/
package mondrian.rolap;

import mondrian.olap.Access;
import mondrian.olap.Role;
import mondrian.olap.Util;

import java.util.*;

/**
 * A <code>RestrictedMemberReader</code> reads only the members of a hierarchy
 * allowed by a role's access profile.
 *
 * @author jhyde
 * @since Feb 26, 2003
 * @version $Id$
 **/
class RestrictedMemberReader extends DelegatingMemberReader {
//	private final RolapMember[] allowedMembers;
	private Role.HierarchyAccess hierarchyAccess;
//	private RolapMember[] deniedMembers;

	/**
	 * Creates a <code>RestrictedMemberReader</code>
	 * @param memberReader Underlying (presumably unrestricted) member reader
	 * @param role Role whose access profile to obey. The role must have
	 *   restrictions on this hierarchy
	 * @pre role.getAccessDetails(memberReader.getHierarchy()) != null
	 */
	RestrictedMemberReader(MemberReader memberReader, Role role) {
		super(memberReader);
		hierarchyAccess = role.getAccessDetails(memberReader.getHierarchy());
		Util.assertPrecondition(hierarchyAccess != null, "role.getAccessDetails(memberReader.getHierarchy()) != null");
		if (false) {
			Map memberGrants = hierarchyAccess.getMemberGrants();
			ArrayList allowedMemberList = new ArrayList(),
					deniedMemberList = new ArrayList();
			for (Iterator members = memberGrants.keySet().iterator(); members.hasNext();) {
				RolapMember member = (RolapMember) members.next();
				final Integer access = (Integer) memberGrants.get(member);
				switch (access.intValue()) {
				case Access.NONE:
					deniedMemberList.add(member);
					break;
				case Access.ALL:
					allowedMemberList.add(member);
					break;
				default:
					throw Util.newInternal("Bad case " + access);
				}
			}
//		this.allowedMembers = (RolapMember[]) allowedMemberList.toArray(RolapUtil.emptyMemberArray);
//		this.deniedMembers = (RolapMember[]) allowedMemberList.toArray(RolapUtil.emptyMemberArray);
		}
	}

	public boolean setCache(MemberCache cache) {
		// Don't support cache-writeback. It would confuse the cache!
		return false;
	}

	public void getMemberChildren(RolapMember parentMember, List children) {
		// todo: optimize if parentMember is beyond last level
		ArrayList fullChildren = new ArrayList();
		memberReader.getMemberChildren(parentMember, fullChildren);
		filterMembers(fullChildren, children);
	}

	/**
	 * Writes to members which we can see.
	 * @param members Input list
	 * @param filteredMembers Output list
	 */
	private void filterMembers(List members, List filteredMembers) {
		for (int i = 0, n = members.size(); i < n; i++) {
			RolapMember member = (RolapMember) members.get(i);
			if (canSee(member)) {
				filteredMembers.add(member);
			}
		}
	}

	private boolean canSee(final RolapMember member) {
		final int access = hierarchyAccess.getAccess(member);
		return access != Access.NONE;
	}

	public void getMemberChildren(List parentMembers, List children) {
		super.getMemberChildren(parentMembers, children);
	}

	public List getMembersInLevel(RolapLevel level, int startOrdinal, int endOrdinal) {
		if (hierarchyAccess.getTopLevel() != null &&
				level.getDepth() < hierarchyAccess.getTopLevel().getDepth()) {
			return Collections.EMPTY_LIST;
		}
		if (hierarchyAccess.getBottomLevel() != null &&
				level.getDepth() > hierarchyAccess.getBottomLevel().getDepth()) {
			return Collections.EMPTY_LIST;
		}
		final List membersInLevel = super.getMembersInLevel(level, startOrdinal, endOrdinal);
		ArrayList filteredMembers = new ArrayList();
		filterMembers(membersInLevel, filteredMembers);
		return filteredMembers;
	}
}

// End RestrictedMemberReader.java