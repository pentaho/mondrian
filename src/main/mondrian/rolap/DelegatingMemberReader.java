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

import mondrian.olap.Member;
import mondrian.olap.Level;

import java.util.List;

/**
 * A <code>DelegatingMemberReader</code> is a {@link MemberReader} which
 * redirects all method calls to an underlying {@link MemberReader}.
 *
 * @author jhyde
 * @since Feb 26, 2003
 * @version $Id$
 **/
class DelegatingMemberReader implements MemberReader {
	protected MemberReader memberReader;

	DelegatingMemberReader(MemberReader memberReader) {
		this.memberReader = memberReader;
	}

	public RolapMember getLeadMember(RolapMember member, int n) {
		return memberReader.getLeadMember(member, n);
	}

	public List getMembersInLevel(RolapLevel level, int startOrdinal, int endOrdinal) {
		return memberReader.getMembersInLevel(level, startOrdinal, endOrdinal);
	}

	public void getMemberRange(RolapLevel level, RolapMember startMember,
							   RolapMember endMember, List list) {
		memberReader.getMemberRange(level, startMember, endMember, list);
	}

	public int compare(RolapMember m1, RolapMember m2, boolean siblingsAreEqual) {
		return memberReader.compare(m1, m2, siblingsAreEqual);
	}

    public void getMemberDescendants(RolapMember member, List result,
            RolapLevel level, boolean before, boolean self, boolean after) {
        memberReader.getMemberDescendants(member, result, level, before, self,
                after);
    }

    public RolapHierarchy getHierarchy() {
		return memberReader.getHierarchy();
	}

	public boolean setCache(MemberCache cache) {
		return memberReader.setCache(cache);
	}

	public RolapMember[] getMembers() {
		return memberReader.getMembers();
	}

	public List getRootMembers() {
		return memberReader.getRootMembers();
	}

	public void getMemberChildren(RolapMember parentMember, List children) {
		memberReader.getMemberChildren(parentMember, children);
	}

	public void getMemberChildren(List parentMembers, List children) {
		memberReader.getMemberChildren(parentMembers, children);
	}

	public int getMemberCount() {
		return memberReader.getMemberCount();
	}

	public RolapMember lookupMember(String[] uniqueNameParts, boolean failIfNotFound) {
		return memberReader.lookupMember(uniqueNameParts, failIfNotFound);
	}
}

// End DelegatingMemberReader.java