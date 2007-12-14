/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mondrian.olap.*;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;

/**
 * <code>CacheMemberReader</code> implements {@link MemberReader} by reading
 * from a pre-populated array of {@link mondrian.olap.Member}s.
 *
 * <p>Note: CacheMemberReader can not handle ragged hierarchies. (HR
 * Tests fail if {@link SmartMemberReader} is replaced with
 * CacheMemberReader).
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 */
class CacheMemberReader implements MemberReader, MemberCache {
    private final MemberSource source;
    private final RolapMember[] members;
    /** Maps a {@link MemberKey} to a {@link RolapMember}. */
    private final Map<Object, RolapMember> mapKeyToMember;

    CacheMemberReader(MemberSource source) {
        this.source = source;
        if (false) {
            // we don't want the reader to write back to our cache
            Util.discard(source.setCache(this));
        }
        this.mapKeyToMember = new HashMap<Object, RolapMember>();
        this.members = source.getMembers();
        for (int i = 0; i < members.length; i++) {
            members[i].setOrdinal(i);
        }
    }

    // implement MemberReader
    public RolapHierarchy getHierarchy() {
        return source.getHierarchy();
    }

    public boolean setCache(MemberCache cache) {
        // we do not support cache writeback -- we must be masters of our
        // own cache
        return false;
    }

    public RolapMember substitute(RolapMember member) {
        return member;
    }

    public RolapMember desubstitute(RolapMember member) {
        return member;
    }

    // implement MemberReader
    public RolapMember[] getMembers() {
        return members;
    }

    // implement MemberCache
    public Object makeKey(RolapMember parent, Object key) {
        return new MemberKey(parent, key);
    }

    // implement MemberCache
    public RolapMember getMember(Object key) {
        return mapKeyToMember.get(key);
    }
    public RolapMember getMember(Object key, boolean mustCheckCacheStatus) {
        return mapKeyToMember.get(key);
    }

    // implement MemberCache
    public Object putMember(Object key, RolapMember value) {
        return mapKeyToMember.put(key, value);
    }

    // don't need to implement this MemberCache method because we're never
    // used in a context where it is needed
    public void putChildren(
        RolapMember member,
        MemberChildrenConstraint constraint,
        List<RolapMember> children)
    {
        throw new UnsupportedOperationException();
    }

    // don't need to implement this MemberCache method because we're never
    // used in a context where it is needed
    public List<RolapMember> getChildrenFromCache(
        RolapMember member,
        MemberChildrenConstraint constraint)
    {
        return null;
    }

    // don't need to implement this MemberCache method because we're never
    // used in a context where it is needed
    public List<RolapMember> getLevelMembersFromCache(
        RolapLevel level,
        TupleConstraint constraint)
    {
        return null;
    }

    public RolapMember lookupMember(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound)
    {
        return RolapUtil.lookupMember(this, uniqueNameParts, failIfNotFound);
    }

    public List<RolapMember> getRootMembers() {
        List<RolapMember> list = new ArrayList<RolapMember>();
        for (RolapMember member : members) {
            if (member.getParentUniqueName() == null) {
                list.add(member);
            }
        }
        return list;
    }

    public List<RolapMember> getMembersInLevel(
        RolapLevel level,
        int startOrdinal,
        int endOrdinal)
    {
        List<RolapMember> list = new ArrayList<RolapMember>();
        int levelDepth = level.getDepth();
        for (RolapMember member : members) {
            if ((member.getLevel().getDepth() == levelDepth) &&
                (startOrdinal <= member.getOrdinal()) &&
                (member.getOrdinal() < endOrdinal)) {

                list.add(member);
            }
        }
        return list;
    }

    public List<RolapMember> getMembersInLevel(
        RolapLevel level,
        int startOrdinal,
        int endOrdinal,
        TupleConstraint constraint)
    {
        return getMembersInLevel(level, startOrdinal, endOrdinal);
    }

    public int getLevelMemberCount(RolapLevel level) {
        int count = 0;
        int levelDepth = level.getDepth();
        for (RolapMember member : members) {
            if (member.getLevel().getDepth() == levelDepth) {
                ++count;
            }
        }
        return count;
    }

    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children)
    {
        for (RolapMember member : members) {
            if (member.getParentMember() == parentMember) {
                children.add(member);
            }
        }
    }

    public void getMemberChildren(
        RolapMember member,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        getMemberChildren(member, children);
    }

    public void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children) {
        for (RolapMember member : members) {
            if (parentMembers.contains(member.getParentMember())) {
                children.add(member);
            }
        }
    }

    public void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        getMemberChildren(parentMembers, children);
    }

    public RolapMember getLeadMember(RolapMember member, int n) {
        if (n >= 0) {
            for (int ordinal = member.getOrdinal(); ordinal < members.length;
                 ordinal++) {
                if ((members[ordinal].getLevel() == member.getLevel()) &&
                    (n-- == 0)) {

                    return members[ordinal];
                }
            }
            return (RolapMember) member.getHierarchy().getNullMember();

        } else {
            for (int ordinal = member.getOrdinal(); ordinal >= 0; ordinal--) {
                if ((members[ordinal].getLevel() == member.getLevel()) &&
                    (n++ == 0)) {
                    return members[ordinal];
                }
            }
            return (RolapMember) member.getHierarchy().getNullMember();
        }
    }

    public void getMemberRange(
        RolapLevel level,
        RolapMember startMember,
        RolapMember endMember,
        List<RolapMember> list)
    {
        assert startMember != null;
        assert endMember != null;
        assert startMember.getLevel() == endMember.getLevel();
        final int endOrdinal = endMember.getOrdinal();
        for (int i = startMember.getOrdinal(); i <= endOrdinal; i++) {
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
        if (siblingsAreEqual &&
            (m1.getParentMember() == m2.getParentMember())) {
            return 0;
        }
        Util.assertTrue(members[m1.getOrdinal()] == m1);
        Util.assertTrue(members[m2.getOrdinal()] == m2);

        return (m1.getOrdinal() < m2.getOrdinal()) ? -1 : 1;
    }

    public MemberBuilder getMemberBuilder() {
        return null;
    }

    public RolapMember getDefaultMember() {
        RolapMember defaultMember =
            (RolapMember) getHierarchy().getDefaultMember();
        if (defaultMember != null) {
            return defaultMember;
        }
        return getRootMembers().get(0);
    }

    public RolapMember getMemberParent(RolapMember member) {
        return member.getParentMember();
    }
}

// End CacheMemberReader.java
