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
 * 
 * <code>CacheMemberReader</code> implements {@link MemberReader} by reading
 * from a pre-populated array of {@link mondrian.olap.Member}s.
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 *
 * note: CacheMemberReader can not handle ragged hierarchies (HR Tests fail if SmartMemberReader
 * is replaced with CacheMemberReader).
 */
class CacheMemberReader implements MemberReader, MemberCache
{
	private MemberSource source;
	private RolapMember[] members;
	/** Maps a {@link MemberKey} to a {@link RolapMember}. **/
	private HashMap mapKeyToMember;

	CacheMemberReader(MemberSource source)
	{
		this.source = source;
		if (false) {
			// we don't want the reader to write back to our cache
			Util.discard(source.setCache(this));
		}
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

	public boolean setCache(MemberCache cache) {
		// we do not support cache writeback -- we must be masters of our
		// own cache
		return false;
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

	public RolapMember lookupMember(String[] uniqueNameParts, boolean failIfNotFound) {
		return RolapUtil.lookupMember(this, uniqueNameParts, failIfNotFound);
	}

	public List getRootMembers() {
		ArrayList list = new ArrayList();
		for (int i = 0; i < members.length; i++) {
			if (members[i].getParentUniqueName() == null) {
				list.add(members[i]);
			}
		}
		return list;
	}

	public List getMembersInLevel(
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
		return list;
	}

	public void getMemberChildren(RolapMember parentMember, List children) {
		for (int i = 0; i < members.length; i++) {
			RolapMember member = members[i];
			if (member.getParentMember() == parentMember) {
				children.add(member);
			}
		}
	}

	public void getMemberChildren(List parentMembers, List children) {
		for (int i = 0; i < members.length; i++) {
			RolapMember member = members[i];
			if (parentMembers.contains(member.getParentMember())) {
				children.add(member);
			}
		}
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
		Util.assertPrecondition(startMember != null, "startMember != null");
		Util.assertPrecondition(endMember != null, "endMember != null");
		Util.assertPrecondition(startMember.getLevel() == endMember.getLevel(),
				"startMember.getLevel() == endMember.getLevel()");
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
		Util.assertTrue(members[m1.ordinal] == m1);
		Util.assertTrue(members[m2.ordinal] == m2);
		return m1.ordinal < m2.ordinal ? -1 : 1;
	}

    public void getMemberDescendants(RolapMember member, List result,
            RolapLevel level, boolean before, boolean self, boolean after) {
        RolapUtil.getMemberDescendants(this, member, level, result, before,
                self, after);
    }

}

// End CacheMemberReader.java
