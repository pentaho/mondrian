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
import java.util.HashMap;

/**
 * A <code>RolapSchemaReader</code> allows you to read schema objects while
 * observing the access-control profile specified by a given role.
 *
 * @author jhyde
 * @since Feb 24, 2003
 * @version $Id$
 **/
abstract class RolapSchemaReader implements SchemaReader {
	private Role role;
	private HashMap hierarchyReaders = new HashMap();

	RolapSchemaReader(Role role) {
		this.role = role;
	}

	public Role getRole() {
		return role;
	}

	public Member[] getHierarchyRootMembers(Hierarchy hierarchy) {
		final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(hierarchy);
		Level firstLevel;
		if (hierarchyAccess == null) {
			firstLevel = hierarchy.getLevels()[0];
		} else {
			firstLevel = hierarchyAccess.getTopLevel();
			if (firstLevel == null) {
				firstLevel = hierarchy.getLevels()[0];
			}
		}
		return getLevelMembers(firstLevel);
	}

	private synchronized MemberReader getMemberReader(Hierarchy hierarchy) {
		MemberReader memberReader = (MemberReader) hierarchyReaders.get(hierarchy);
		if (memberReader == null) {
			memberReader = ((RolapHierarchy) hierarchy).getMemberReader(role);
			hierarchyReaders.put(hierarchy, memberReader);
		}
		return memberReader;
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

	public abstract Cube getCube();

	public OlapElement getElementChild(OlapElement parent, String name) {
		return parent.lookupChild(this, name);
	}

	public Member getMemberByUniqueName(String[] uniqueNameParts, boolean failIfNotFound) {
		return Util.lookupMemberCompound(this, getCube(), uniqueNameParts, failIfNotFound);
	}

	public Member getLeadMember(Member member, int n) {
		return getMemberReader(member.getHierarchy()).getLeadMember((RolapMember) member, n);
	}

	public Member[] getLevelMembers(Level level) {
		final List membersInLevel = getMemberReader(level.getHierarchy()).getMembersInLevel(
					(RolapLevel) level, 0, Integer.MAX_VALUE);
		return RolapUtil.toArray(membersInLevel);
	}

	public Level[] getHierarchyLevels(Hierarchy hierarchy) {
		final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(hierarchy);
		final Level[] levels = hierarchy.getLevels();
		if (hierarchyAccess == null) {
			return levels;
		}
		Level topLevel = hierarchyAccess.getTopLevel();
		Level bottomLevel = hierarchyAccess.getBottomLevel();
		if (topLevel == null &&
				bottomLevel == null) {
			return levels;
		}
		if (topLevel == null) {
			topLevel = levels[0];
		}
		if (bottomLevel == null) {
			bottomLevel = levels[levels.length - 1];
		}
		final int levelCount = bottomLevel.getDepth() - topLevel.getDepth() + 1;
		Level[] restrictedLevels = new Level[levelCount];
		System.arraycopy(levels, topLevel.getDepth(), restrictedLevels, 0, levelCount);
		return restrictedLevels;
	}
}

// End RolapSchemaReader.java