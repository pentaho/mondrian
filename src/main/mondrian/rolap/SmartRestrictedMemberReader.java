/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.sql.*;

import java.util.*;

class SmartRestrictedMemberReader extends DelegatingMemberReader {

    SmartRestrictedMemberReader(MemberReader memberReader, Role role) {
        // We want to extend a RestrictedMemberReader with access details
        // that we cache.
        super(new RestrictedMemberReader(memberReader, role));
    }

    @Override
    public Map<? extends Member, Access> getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        SmartMemberReader reader =
            (SmartMemberReader)((RestrictedMemberReader)memberReader)
                .memberReader;

        synchronized (reader.cacheHelper) {
            reader.checkCacheStatus();
            AccessAwareMemberList list =
                (AccessAwareMemberList) reader.cacheHelper
                    .getChildrenFromCache(parentMember, constraint);
            if (list == null) {
                // the null member has no children
                if (!parentMember.isNull()) {
                    List<RolapMember> computedChildren =
                        new LinkedList<RolapMember>();
                    Map<? extends Member, Access> membersWithAccessDetails =
                        super.getMemberChildren(
                            parentMember,
                            computedChildren,
                            constraint);

                    reader.cacheHelper.putChildren(
                        parentMember,
                        constraint,
                        new AccessAwareMemberList(
                            computedChildren,
                            membersWithAccessDetails));

                    children.addAll(computedChildren);

                    return membersWithAccessDetails;

                } else {
                    return new HashMap<Member, Access>();
                }
            } else {
                children.addAll(list);
                return list.getAccessMap();
            }
        }
    }

    @Override
    public synchronized Map<? extends Member, Access> getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        SmartMemberReader reader =
            (SmartMemberReader)((RestrictedMemberReader)memberReader)
                .memberReader;

        synchronized (reader.cacheHelper) {
            reader.checkCacheStatus();

            Map<Member, Access> allMembersWithAccessDetails =
                new HashMap<Member, Access>();
            for (RolapMember parentMember : parentMembers) {
                allMembersWithAccessDetails.putAll(
                    getMemberChildren(parentMember, children, constraint));
            }
            return allMembersWithAccessDetails;
        }
    }

    private static class AccessAwareMemberList
        extends AbstractList<RolapMember>
    {
        private List<RolapMember> delegate;
        private Map<? extends Member, Access> accessMap;
        public AccessAwareMemberList(
            List<RolapMember> delegate,
            Map<? extends Member, Access> accessMap)
        {
            super();
            this.delegate = delegate;
            this.accessMap = accessMap;
        }
        public RolapMember get(int arg0) {
            return delegate.get(arg0);
        }
        public int size() {
            return delegate.size();
        }
        public Map<? extends Member, Access> getAccessMap() {
            return accessMap;
        }
    }
}

// End SmartRestrictedMemberReader.java