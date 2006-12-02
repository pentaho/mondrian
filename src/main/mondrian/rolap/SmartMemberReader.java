/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.Util;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

import java.util.*;

/**
 * <code>SmartMemberReader</code> implements {@link MemberReader} by keeping a
 * cache of members and their children. If a member is 'in cache', there is a
 * list of its children. It also caches the members of levels.
 *
 * <p>Synchronization: the MemberReader <code>source</code> must be called
 * from synchronized(this) context - it does not synchronize itself (probably
 * it should).</p>
 *
 * <p>Constraints: Member.Children and Level.Members may be constrained by a
 * SqlConstraint object. In this case a subset of all members is returned.
 * These subsets are cached too and the SqlConstraint is part of the cache key.
 * This is used in NON EMPTY context.</p>
 *
 * <p>Uniqueness. We need to ensure that there is never more than one {@link
 * RolapMember} object representing the same member.</p>
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 */
public class SmartMemberReader implements MemberReader, MemberCache {
    private final SqlConstraintFactory sqlConstraintFactory =
            SqlConstraintFactory.instance();

    /** access to <code>source</code> must be synchronized(this) */
    private final MemberReader source;

    /** maps a parent member to a list of its children */
    final SmartMemberListCache<RolapMember, List<RolapMember>> mapMemberToChildren;

    /** a cache for alle members to ensure uniqueness */
    final SmartCache<Object, RolapMember> mapKeyToMember;

    /** maps a level to its members */
    final SmartMemberListCache<RolapLevel, List<RolapMember>> mapLevelToMembers;

    private List<RolapMember> rootMembers;

    SmartMemberReader(MemberReader source) {
        this.source = source;
        if (!source.setCache(this)) {
            throw Util.newInternal(
                    "MemberSource (" + source + ", " + source.getClass() +
                    ") does not support cache-writeback");
        }
        this.mapLevelToMembers =
            new SmartMemberListCache<RolapLevel, List<RolapMember>>();
        this.mapKeyToMember = new SoftSmartCache<Object, RolapMember>();
        this.mapMemberToChildren =
            new SmartMemberListCache<RolapMember, List<RolapMember>>();
    }

    // implement MemberReader
    public RolapHierarchy getHierarchy() {
        return source.getHierarchy();
    }

    // implement MemberSource
    public boolean setCache(MemberCache cache) {
        // we do not support cache writeback -- we must be masters of our
        // own cache
        return false;
    }

    // implement MemberCache
    public Object makeKey(RolapMember parent, Object key) {
        return new MemberKey(parent, key);
    }

    // implement MemberCache
    // synchronization: Must synchronize, because uses mapKeyToMember
    public synchronized RolapMember getMember(Object key) {
        return mapKeyToMember.get(key);
    }

    // implement MemberCache
    // synchronization: Must synchronize, because modifies mapKeyToMember
    public synchronized Object putMember(Object key, RolapMember value) {
        return mapKeyToMember.put(key, value);
    }

    // implement MemberReader
    public RolapMember[] getMembers() {
        List<RolapMember> v = new ArrayList<RolapMember>();
        RolapLevel[] levels = (RolapLevel[]) getHierarchy().getLevels();
        // todo: optimize by walking to children for members we know about
        for (RolapLevel level : levels) {
            List<RolapMember> membersInLevel = getMembersInLevel(
                level,
                0,
                Integer.MAX_VALUE);
            v.addAll(membersInLevel);
        }
        return v.toArray(new RolapMember[v.size()]);
    }

    public List<RolapMember> getRootMembers() {
        if (rootMembers == null) {
            rootMembers = source.getRootMembers();
        }
        return rootMembers;
    }


    // Synchronization: modifies mapLevelToMembers
    public synchronized List<RolapMember> getMembersInLevel(
            RolapLevel level,
            int startOrdinal,
            int endOrdinal) {
        TupleConstraint constraint =
                sqlConstraintFactory.getLevelMembersConstraint(null);
        return getMembersInLevel(level, startOrdinal, endOrdinal, constraint);
    }

    public synchronized List<RolapMember> getMembersInLevel(
        RolapLevel level,
        int startOrdinal,
        int endOrdinal,
        TupleConstraint constraint)
    {
        List<RolapMember> members = mapLevelToMembers.get(level, constraint);
        if (members != null) {
            return members;
        }
        members = source.getMembersInLevel(level, startOrdinal, endOrdinal, constraint);
        mapLevelToMembers.put(level, constraint, members);
        return members;
    }

    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children)
    {
        MemberChildrenConstraint constraint =
                sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMember, children, constraint);
    }

    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        List<RolapMember> parentMembers = new ArrayList<RolapMember>();
        parentMembers.add(parentMember);
        getMemberChildren(parentMembers, children, constraint);
    }

    public synchronized void getMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children) {
        MemberChildrenConstraint constraint =
                sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMembers, children, constraint);
    }

    public synchronized void getMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint) {
        List<RolapMember> missed = new ArrayList<RolapMember>();
        for (RolapMember parentMember : parentMembers) {
            List<RolapMember> list =
                mapMemberToChildren.get(parentMember, constraint);
            if (list == null) {
                if (parentMember.isNull()) {
                    // the null member has no children
                } else {
                    missed.add(parentMember);
                }
            } else {
                children.addAll(list);
            }
        }
        if (missed.size() > 0) {
            readMemberChildren(missed, children, constraint);
        }
    }

    public RolapMember lookupMember(
            String[] uniqueNameParts,
            boolean failIfNotFound) {
        return RolapUtil.lookupMember(this, uniqueNameParts, failIfNotFound);
    }

    /**
     * Reads the children of <code>member</code> into cache, and also into
     * <code>result</code>.
     *
     * @param result Children are written here, in order
     * @param members Members whose children to read
     * @param constraint restricts the returned members if possible (optional
     *             optimization)
     */
    private void readMemberChildren(
        List<RolapMember> members,
        List<RolapMember> result,
        MemberChildrenConstraint constraint)
    {
        if (false) {
            // Pre-condition disabled. It makes sense to have the pre-
            // condition, because lists of parent members are typically
            // sorted by construction, and we should be able to exploit this
            // when constructing the (significantly larger) set of children.
            // But currently BasicQueryTest.testBasketAnalysis() fails this
            // assert, and I haven't had time to figure out why.
            //   -- jhyde, 2004/6/10.
            Util.assertPrecondition(isSorted(members), "isSorted(members)");
        }
        List<RolapMember> children = new ArrayList<RolapMember>();
        source.getMemberChildren(members, children, constraint);
        // Put them in a temporary hash table first. Register them later, when
        // we know their size (hence their 'cost' to the cache pool).
        Map<RolapMember, List<RolapMember>> tempMap =
            new HashMap<RolapMember, List<RolapMember>>();
        for (RolapMember member1 : members) {
            tempMap.put(member1, Collections.EMPTY_LIST);
        }
        for (int i = 0, childrenCount = children.size(); i < childrenCount; i++) {
            // todo: We could optimize here. If members.length is small, it's
            // more efficient to drive from members, rather than hashing
            // children.length times. We could also exploit the fact that the
            // result is sorted by ordinal and therefore, unless the "members"
            // contains members from different levels, children of the same
            // member will be contiguous.
            RolapMember child = children.get(i);
            assert child != null : "child";
            assert tempMap != null : "tempMap";
            final RolapMember parentMember =
                (RolapMember) child.getParentMember();
            List<RolapMember> list = tempMap.get(parentMember);
            if (list == null) {
                // The list is null if, due to dropped constraints, we now
                // have a children list of a member we didn't explicitly
                // ask for it. Adding it to the cache would be viable, but
                // let's ignore it.
                continue;
            } else if (list == Collections.EMPTY_LIST) {
                list = new ArrayList<RolapMember>();
                tempMap.put(parentMember, list);
            }
            list.add(child);
            result.add(child);
        }
        synchronized (this) {
            for (RolapMember member : tempMap.keySet()) {
                if (getChildrenFromCache(member, constraint) == null) {
                    List<RolapMember> list = tempMap.get(member);
                    putChildren(member, constraint, list);
                }
            }
        }
    }

    /**
     * Returns true if every element of <code>members</code> is not null and is
     * strictly less than the following element; false otherwise.
     */
    public boolean isSorted(List<RolapMember> members) {
        final int count = members.size();
        if (count == 0) {
            return true;
        }
        RolapMember m1 = members.get(0);
        if (m1 == null) {
            // Special case check for 0th element, just in case length == 1.
            return false;
        }
        for (int i = 1; i < count; i++) {
            RolapMember m0 = m1;
            m1 = members.get(i);
            if (m1 == null || compare(m0, m1, false) >= 0) {
                return false;
            }
        }
        return true;
    }

    public synchronized List<RolapMember> getChildrenFromCache(
            RolapMember member,
            MemberChildrenConstraint constraint) {
        if (constraint == null) {
            constraint = sqlConstraintFactory.getMemberChildrenConstraint(null);
        }
        return mapMemberToChildren.get(member, constraint);
    }

    public synchronized List getLevelMembersFromCache(
            RolapLevel level,
            TupleConstraint constraint) {
        if (constraint == null) {
            constraint = sqlConstraintFactory.getLevelMembersConstraint(null);
        }
        return mapLevelToMembers.get(level, constraint);
    }

    public synchronized void putChildren(
        RolapMember member,
        MemberChildrenConstraint constraint,
        List<RolapMember> children)
    {
        if (constraint == null) {
            constraint = sqlConstraintFactory.getMemberChildrenConstraint(null);
        }
        mapMemberToChildren.put(member, constraint, children);
    }

    // synchronization: Must synchronize, because uses mapMemberToChildren
    public synchronized RolapMember getLeadMember(RolapMember member, int n) {
        if (n == 0 || member.isNull()) {
            return member;
        } else {
            SiblingIterator iter = new SiblingIterator(this, member);
            if (n > 0) {
                RolapMember sibling = null;
                while (n-- > 0) {
                    if (!iter.hasNext()) {
                        return (RolapMember) member.getHierarchy().getNullMember();
                    }
                    sibling = iter.nextMember();
                }
                return sibling;
            } else {
                n = -n;
                RolapMember sibling = null;
                while (n-- > 0) {
                    if (!iter.hasPrevious()) {
                        return (RolapMember) member.getHierarchy().getNullMember();
                    }
                    sibling = iter.previousMember();
                }
                return sibling;
            }
        }
    }

    public void getMemberRange(RolapLevel level,
                               RolapMember startMember,
                               RolapMember endMember,
                               List<RolapMember> list) {
        Util.assertPrecondition(startMember != null, "startMember != null");
        Util.assertPrecondition(endMember != null, "endMember != null");
        Util.assertPrecondition(startMember.getLevel() == endMember.getLevel(),
                "startMember.getLevel() == endMember.getLevel()");

        if (compare(startMember, endMember, false) > 0) {
            return;
        }
        list.add(startMember);
        if (startMember == endMember) {
            return;
        }
        SiblingIterator siblings = new SiblingIterator(this, startMember);
        while (siblings.hasNext()) {
            final RolapMember member = siblings.nextMember();
            list.add(member);
            if (member == endMember) {
                return;
            }
        }
        throw Util.newInternal("sibling iterator did not hit end point, start="
                + startMember
                + ", end="
                + endMember);
    }

    public int getMemberCount() {
        return source.getMemberCount();
    }

    public int compare(RolapMember m1,
                       RolapMember m2,
                       boolean siblingsAreEqual) {
        if (m1 == m2) {
            return 0;
        }
        if (m1.getParentMember() == m2.getParentMember()) {
            // including case where both parents are null
            if (siblingsAreEqual) {
                return 0;
            } else if (m1.getParentMember() == null) {
                // at this point we know that both parent members are null.
                int pos1 = -1, pos2 = -1;
                List siblingList = getRootMembers();
                for (int i = 0, n = siblingList.size(); i < n; i++) {
                    RolapMember child = (RolapMember) siblingList.get(i);
                    if (child == m1) {
                        pos1 = i;
                    }
                    if (child == m2) {
                        pos2 = i;
                    }
                }
                if (pos1 == -1) {
                    throw Util.newInternal(m1 + " not found among siblings");
                }
                if (pos2 == -1) {
                    throw Util.newInternal(m2 + " not found among siblings");
                }
                Util.assertTrue(pos1 != pos2);
                return pos1 < pos2 ? -1 : 1;
            } else {
                List<RolapMember> children = new ArrayList<RolapMember>();
                getMemberChildren((RolapMember) m1.getParentMember(), children);
                int pos1 = -1, pos2 = -1;
                for (int i = 0, n = children.size(); i < n; i++) {
                    RolapMember child = children.get(i);
                    if (child == m1) {
                        pos1 = i;
                    }
                    if (child == m2) {
                        pos2 = i;
                    }
                }
                if (pos1 == -1) {
                    throw Util.newInternal(m1 + " not found among siblings");
                }
                if (pos2 == -1) {
                    throw Util.newInternal(m2 + " not found among siblings");
                }
                Util.assertTrue(pos1 != pos2);
                return pos1 < pos2 ? -1 : 1;
            }
        }
        int levelDepth1 = m1.getLevel().getDepth();
        int levelDepth2 = m2.getLevel().getDepth();
        if (levelDepth1 < levelDepth2) {
            final int c = compare(m1, (RolapMember) m2.getParentMember(), false);
            return (c == 0) ? -1 : c;

        } else if (levelDepth1 > levelDepth2) {
            final int c = compare((RolapMember) m1.getParentMember(), m2, false);
            return (c == 0) ? 1 : c;

        } else {
            return compare((RolapMember) m1.getParentMember(), (RolapMember) m2.getParentMember(), false);
        }
    }

    /**
     * <code>SiblingIterator</code> helps traverse a hierarchy of members, by
     * remembering the position at each level. Each SiblingIterator has a
     * parent, to which it defers when the last child of the current member is
     * reached.
     */
    class SiblingIterator {
        private final MemberReader reader;
        private final SiblingIterator parentIterator;
        private RolapMember[] siblings;
        private int position;

        SiblingIterator(MemberReader reader, RolapMember member) {
            this.reader = reader;
            RolapMember parent = (RolapMember) member.getParentMember();
            List<RolapMember> siblingList;
            if (parent == null) {
                siblingList = reader.getRootMembers();
                this.parentIterator = null;
            } else {
                siblingList = new ArrayList<RolapMember>();
                reader.getMemberChildren(parent, siblingList);
                this.parentIterator = new SiblingIterator(reader, parent);
            }
            this.siblings = RolapUtil.toArray(siblingList);
            this.position = -1;
            for (int i = 0; i < this.siblings.length; i++) {
                if (siblings[i] == member) {
                    this.position = i;
                    break;
                }
            }
            if (this.position == -1) {
                throw Util.newInternal(
                    "member " + member + " not found among its siblings");
            }
        }
        boolean hasNext() {
            return (this.position < this.siblings.length - 1) ||
                (parentIterator != null) &&
                parentIterator.hasNext();
        }
        Object next() {
            return nextMember();
        }
        RolapMember nextMember() {
            if (++this.position >= this.siblings.length) {
                if (parentIterator == null) {
                    throw Util.newInternal("there is no next member");
                }
                RolapMember parent = parentIterator.nextMember();
                List<RolapMember> siblingList = new ArrayList<RolapMember>();
                reader.getMemberChildren(parent, siblingList);
                this.siblings = RolapUtil.toArray(siblingList);
                this.position = 0;
            }
            return this.siblings[this.position];
        }
        boolean hasPrevious() {
            return (this.position > 0) ||
                (parentIterator != null) &&
                parentIterator.hasPrevious();
        }
        Object previous() {
            return previousMember();
        }
        RolapMember previousMember() {
            if (--this.position < 0) {
                if (parentIterator == null) {
                    throw Util.newInternal("there is no next member");
                }
                RolapMember parent = parentIterator.previousMember();
                List<RolapMember> siblingList = new ArrayList<RolapMember>();
                reader.getMemberChildren(parent, siblingList);
                this.siblings = RolapUtil.toArray(siblingList);
                this.position = this.siblings.length - 1;
            }
            return this.siblings[this.position];
        }
    }

    public MemberBuilder getMemberBuilder() {
        return source.getMemberBuilder();
    }
}

// End SmartMemberReader.java
