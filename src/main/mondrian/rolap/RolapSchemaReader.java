/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2004 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 24, 2003
*/
package mondrian.rolap;

import mondrian.olap.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

	public Member getMemberParent(Member member) {
		Member parentMember = member.getParentMember();
        // Skip over hidden parents.
        while (parentMember != null && parentMember.isHidden()) {
            parentMember = parentMember.getParentMember();
        }
        // Skip over non-accessible parents.
		if (parentMember != null) {
			final Role.HierarchyAccess hierarchyAccess =
                    role.getAccessDetails(member.getHierarchy());
			if (hierarchyAccess != null &&
					hierarchyAccess.getAccess(parentMember) == Access.NONE) {
				return null;
			}
		}
		return parentMember;
	}

	public int getMemberDepth(Member member) {
		final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(member.getHierarchy());
		if (hierarchyAccess != null) {
			int memberDepth = member.getLevel().getDepth();
			final Level topLevel = hierarchyAccess.getTopLevel();
			if (topLevel != null) {
				memberDepth -= topLevel.getDepth();
			}
			return memberDepth;
		} else if (((RolapLevel) member.getLevel()).parentExp != null) {
			// For members of parent-child hierarchy, members in the same level may have
			// different depths.
			int depth = 0;
			for (Member m = member.getParentMember(); m != null; m = m.getParentMember()) {
				depth++;
			}
			return depth;
		} else {
			return member.getLevel().getDepth();
		}
	}

	public Member[] getMemberChildren(Member member) {
		ArrayList children = new ArrayList();
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        memberReader.getMemberChildren((RolapMember) member, children);
		return RolapUtil.toArray(children);
	}

	public Member[] getMemberChildren(Member[] members) {
		if (members.length == 0) {
			return RolapUtil.emptyMemberArray;
		} else {
            final Hierarchy hierarchy = members[0].getHierarchy();
            final MemberReader memberReader = getMemberReader(hierarchy);
			ArrayList children = new ArrayList();
			for (int i = 0; i < members.length; i++) {
				memberReader.getMemberChildren((RolapMember) members[i], children);
			}
			return RolapUtil.toArray(children);
		}
	}

    public void getMemberDescendants(Member member, List result, Level level,
            boolean before, boolean self, boolean after) {
        Util.assertPrecondition(level != null, "level != null");
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = getMemberReader(hierarchy);
        memberReader.getMemberDescendants((RolapMember) member, result,
                (RolapLevel) level, before, self, after);
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
        final MemberReader memberReader = getMemberReader(level.getHierarchy());
        final List membersInLevel = memberReader.getMembersInLevel(
					(RolapLevel) level, 0, Integer.MAX_VALUE);
		return RolapUtil.toArray(membersInLevel);
	}

	public Level[] getHierarchyLevels(Hierarchy hierarchy) {
		Util.assertPrecondition(hierarchy != null, "hierarchy != null");
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
		Util.assertPostcondition(restrictedLevels.length >= 1, "return.length >= 1");
		return restrictedLevels;
	}

	public Member getHierarchyDefaultMember(Hierarchy hierarchy) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
		RolapMember member = (RolapMember) hierarchy.getDefaultMember();
		final Role.HierarchyAccess hierarchyAccess = role.getAccessDetails(hierarchy);
		if (hierarchyAccess != null) {
			final Level level = member.getLevel();
			final int levelDepth = level.getDepth();
			final Level topLevel = hierarchyAccess.getTopLevel();
			final MemberReader unrestrictedMemberReader = ((RolapHierarchy) hierarchy).memberReader;
			if (topLevel != null &&
					topLevel.getDepth() > levelDepth) {
				// Find the first child of the first child... until we get to
				// a level we can see.
				ArrayList children = new ArrayList();
				do {
					unrestrictedMemberReader.getMemberChildren(member, children);
					Util.assertTrue(children.size() > 0);
					member = (RolapMember) children.get(0);
					children.clear();
				} while (member.getLevel() != topLevel);
				return member;
			}
			final Level bottomLevel = hierarchyAccess.getBottomLevel();
			if (bottomLevel != null &&
					bottomLevel.getDepth() < levelDepth) {
				do {
					member = (RolapMember) member.getParentMember();
					Util.assertTrue(member != null);
				} while (member.getLevel() != bottomLevel);
				return member;
			}
		}
		return member;
	}

    public boolean isDrillable(Member member) {
        final RolapLevel level = (RolapLevel) member.getLevel();
        if (level.parentExp != null) {
            // This is a parent-child level, so its children, if any, come from
            // the same level.
            //
            // todo: More efficient implementation
            return getMemberChildren(member).length > 0;
        } else {
            // This is a regular level. It has children iff there is a lower
            // level.
            final Level childLevel = level.getChildLevel();
            return childLevel != null &&
                    role.getAccess(childLevel) != Access.NONE;
        }
    }

    public boolean isVisible(Member member) {
        return !member.isHidden() &&
                role.canAccess(member);
    }
}

// End RolapSchemaReader.java
