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
import mondrian.olap.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * <code>CacheMemberReader</code> implements {@link MemberReader} by reading
 * from a pre-populated array of {@link mondrian.olap.Member}s.
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 **/
class CacheMemberReader implements MemberReader, MemberCache
{
	private MemberSource source;
	private RolapMember[] members;
	/** Maps a {@link MemberKey} to a {@link RolapMember}. **/
	private HashMap mapKeyToMember;

	CacheMemberReader(MemberSource source)
	{
		this.source = source;
		source.setCache(this);
		this.mapKeyToMember = new HashMap();
		this.members = source.getMembers();
		for (int i = 0; i < members.length; i++) {
			members[i].ordinal = i;
		}
	}

	// implement MemberReader
	public RolapHierarchy getHierarchy()
	{
		return source.getHierarchy();
	}

	// implement MemberSource
	public void setCache(MemberCache cache)
	{
		throw Util.newInternal(
			getClass() + " must be master of its own cache");
	}

	// implement MemberReader
	public RolapMember[] getMembers()
	{
		return members;
	}

	// implement MemberCache
	public Object makeKey(RolapMember parent, Object key)
	{
		return new MemberKey(parent, key);
	}

	// implement MemberCache
	public RolapMember getMember(Object key)
	{
		return (RolapMember) mapKeyToMember.get(key);
	}

	// implement MemberCache
	public Object putMember(Object key, RolapMember value)
	{
		return mapKeyToMember.put(key, value);
	}

	// don't need to implement this MemberCache method because we're never
	// used in a context where it is needed
	public void putChildren(RolapMember member, ArrayList children) {
		throw new UnsupportedOperationException();
	}

	// don't need to implement this MemberCache method because we're never
	// used in a context where it is needed
	public boolean hasChildren(RolapMember member) {
		return false;
	}

	public RolapMember lookupMember(String uniqueName, boolean failIfNotFound) {
		return lookupMember(this, uniqueName, failIfNotFound);
	}

	static RolapMember lookupMember(
			MemberReader reader, String uniqueName, boolean failIfNotFound) {
		String[] names = Util.explode(uniqueName);
		RolapMember member = null;
		for (int i = 0; i < names.length; i++) {
			String name = names[i];
			RolapMember[] children;
			if (member == null) {
				children = reader.getRootMembers();
			} else {
				children = reader.getMemberChildren(new RolapMember[] {member});
				member = null;
			}
			for (int j = 0; j < children.length; j++) {
				RolapMember child = children[j];
				if (child.getName().equals(name)) {
					member = child;
					break;
				}
			}
			if (member == null) {
				break;
			}
		}
		if (member == null && failIfNotFound) {
			throw Util.getRes().newMdxCantFindMember(uniqueName);
		}
		return member;
	}

	public RolapMember[] getRootMembers() {
		ArrayList list = new ArrayList();
		for (int i = 0; i < members.length; i++) {
			if (members[i].getParentUniqueName() == null) {
				list.add(members[i]);
			}
		}
		return (RolapMember[]) list.toArray(RolapUtil.emptyMemberArray);
	}

	public RolapMember[] getMembersInLevel(
		RolapLevel level, int startOrdinal, int endOrdinal) {
		ArrayList list = new ArrayList();
		int levelDepth = level.getDepth();
		for (int i = 0; i < members.length; i++) {
			RolapMember member = members[i];
			if (member.getLevel().getDepth() == levelDepth &&
				startOrdinal <= member.ordinal &&
				member.ordinal < endOrdinal) {
				list.add(members[i]);
			}
		}
		return (RolapMember[]) list.toArray(RolapUtil.emptyMemberArray);
	}

	public RolapMember[] getMemberChildren(RolapMember[] parentOlapMembers) {
		// Find the children by simply scanning the array of all
		// members. This won't be efficient when there are a lot of
		// members.
		ArrayList list = new ArrayList();
		for (int i = 0; i < members.length; i++) {
			RolapMember member = members[i];
			for (int j = 0; j < parentOlapMembers.length; j++) {
				if (member.getParentMember() == parentOlapMembers[j]) {
					list.add(member);
				}
			}
		}
		return (RolapMember[]) list.toArray(RolapUtil.emptyMemberArray);
	}

	public RolapMember getLeadMember(RolapMember member, int n) {
		if (n >= 0) {
			for (int ordinal = member.ordinal; ordinal < members.length;
				 ordinal++) {
				if (members[ordinal].getLevel() == member.getLevel() &&
					n-- == 0) {
					return members[ordinal];
				}
			}
			return (RolapMember) member.getHierarchy().getNullMember();
		} else {
			for (int ordinal = member.ordinal; ordinal >= 0; ordinal--) {
				if (members[ordinal].getLevel() == member.getLevel() &&
					n++ == 0) {
					return members[ordinal];
				}
			}
			return (RolapMember) member.getHierarchy().getNullMember();
		}
	}

	public void getMemberRange(
			RolapLevel level, RolapMember startMember, RolapMember endMember,
			List list) {
		for (int i = startMember.ordinal; i <= endMember.ordinal; i++) {
			if (members[i].getLevel() == endMember.getLevel()) {
				list.add(members[i]);
			}
		}
	}

	public int getMemberCount() {
		return members.length;
	}

	public int compare(RolapMember m1, RolapMember m2, boolean siblingsAreEqual) {
		if (m1 == m2) {
			return 0;
		}
		if (siblingsAreEqual && m1.getParentMember() == m2.getParentMember()) {
			return 0;
		}
		// Members are stored in hierarchical order.
		int pos1 = -1, pos2 = -1;
		for (int i = 0; i < members.length; i++) {
			RolapMember member = members[i];
			if (member == m1) {
				pos1 = i;
			}
			if (member == m2) {
				pos2 = i;
			}
		}
		if (pos1 == -1) {
			throw Util.newInternal(m1 + " not found among members");
		}
		if (pos2 == -1) {
			throw Util.newInternal(m2 + " not found among members");
		}
		Util.assertTrue(pos1 != pos2);
		return pos1 < pos2 ? -1 : 1;
	}
}

// End CacheMemberReader.java
