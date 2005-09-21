/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.Member;
import mondrian.olap.Util;
import mondrian.resource.MondrianResource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * <code>ArrayMemberSource</code> implements a flat, static hierarchy. There is
 * no root member, and all members are siblings.
 *
 * @author jhyde
 * @since 22 December, 2001
 * @version $Id$
 **/
abstract class ArrayMemberSource implements MemberSource {

    protected final RolapHierarchy hierarchy;
    protected final RolapMember[] members;

    ArrayMemberSource(RolapHierarchy hierarchy, RolapMember[] members) {
        this.hierarchy = hierarchy;
        this.members = members;
    }
    public RolapHierarchy getHierarchy() {
        return hierarchy;
    }
    public boolean setCache(MemberCache cache) {
        return false; // we do not support cache writeback
    }
    public RolapMember[] getMembers() {
        return members;
    }
    public int getMemberCount() {
        return members.length;
    }
    public List getRootMembers() {
        return Collections.EMPTY_LIST;
    }
    public void getMemberChildren(RolapMember parentMember, List children) {
        // there are no children
    }
    public void getMemberChildren(List parentMembers, List children) {
        // there are no children
    }
    public RolapMember lookupMember(String[] uniqueNameParts,
                                    boolean failIfNotFound) {
        String uniqueName = Util.implode(uniqueNameParts);
        for (int i = 0; i < members.length; i++) {
            RolapMember member = members[i];
            if (member.getUniqueName().equals(uniqueName)) {
                return member;
            }
        }
        if (failIfNotFound) {
            throw MondrianResource.instance().MdxCantFindMember.ex(uniqueName);
        } else {
            return null;
        }
    }
}

/*
class HasBoughtDairySource extends ArrayMemberSource
{
    public HasBoughtDairySource(RolapHierarchy hierarchy, Properties properties)
    {
        super(hierarchy, new Thunk(hierarchy).getMembers());
        Util.discard(properties);
    }

    ///
     //Because Java won't allow us to call methods before constructing {@link
     //HasBoughtDairyReader}'s base class.
     ///
    private static class Thunk
    {
        RolapHierarchy hierarchy;

        Thunk(RolapHierarchy hierarchy)
        {
            this.hierarchy = hierarchy;
        }
        RolapMember[] getMembers()
        {
            String[] values = new String[] {"False", "True"};
            List list = new ArrayList();
            int ordinal = 0;
            RolapMember root = null;
            RolapLevel level = (RolapLevel) hierarchy.getLevels()[0];
            if (hierarchy.hasAll()) {
                root = new RolapMember(null, level, null,
                        hierarchy.getAllMemberName(), Member.ALL_MEMBER_TYPE);
                root.setOrdinal(ordinal++);
                list.add(root);
                level = (RolapLevel) hierarchy.getLevels()[1];
            }
            for (int i = 0; i < values.length; i++) {
                RolapMember member = new RolapMember(root, level, values[i]);
                member.setOrdinal(ordinal++);
                list.add(member);
            }
            return (RolapMember[]) list.toArray(RolapUtil.emptyMemberArray);
        }
    }
}
*/

// End ArrayMemberSource.java
