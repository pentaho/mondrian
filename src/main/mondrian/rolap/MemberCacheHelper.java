/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Level;
import mondrian.olap.Util;
import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.spi.DataSourceChangeListener;
import mondrian.util.Pair;

import java.util.*;
import java.util.Map.Entry;

/**
 * Encapsulation of member caching.
 *
 * @author Will Gorman
 */
public class MemberCacheHelper implements MemberCache {

    private final SqlConstraintFactory sqlConstraintFactory =
        SqlConstraintFactory.instance();

    /** maps a parent member to a list of its children */
    final SmartMemberListCache<RolapMember, List<RolapMember>>
        mapMemberToChildren;

    /** a cache for all members to ensure uniqueness */
    SmartCache<Object, RolapMember> mapKeyToMember;
    RolapHierarchy rolapHierarchy;
    DataSourceChangeListener changeListener;

    /** maps a level to its members */
    final SmartMemberListCache<RolapLevel, List<RolapMember>>
        mapLevelToMembers;

    /**
     * Creates a MemberCacheHelper.
     *
     * @param rolapHierarchy Hierarchy
     */
    public MemberCacheHelper(RolapHierarchy rolapHierarchy) {
        this.rolapHierarchy = rolapHierarchy;
        this.mapLevelToMembers =
            new SmartMemberListCache<RolapLevel, List<RolapMember>>();
        this.mapKeyToMember =
            new SoftSmartCache<Object, RolapMember>();
        this.mapMemberToChildren =
            new SmartMemberListCache<RolapMember, List<RolapMember>>();

        if (rolapHierarchy != null) {
            changeListener =
                rolapHierarchy.getRolapSchema().getDataSourceChangeListener();
        } else {
            changeListener = null;
        }
    }

    public RolapMember getMember(
        Object key,
        boolean mustCheckCacheStatus)
    {
        if (mustCheckCacheStatus) {
            checkCacheStatus();
        }
        return mapKeyToMember.get(key);
    }


    // implement MemberCache
    public Object putMember(Object key, RolapMember value) {
        return mapKeyToMember.put(key, value);
    }

    // implement MemberCache
    public Object makeKey(RolapMember parent, Object key) {
        return new MemberKey(parent, key);
    }

    // implement MemberCache
    public RolapMember getMember(Object key) {
        return getMember(key, true);
    }

    public synchronized void checkCacheStatus() {
        if (changeListener != null) {
            if (changeListener.isHierarchyChanged(rolapHierarchy)) {
                flushCache();
            }
        }
    }

    /**
     * Deprecated in favor of
     * {@link #putChildren(RolapLevel, TupleConstraint, List)}
     */
    @Deprecated
    public void putLevelMembersInCache(
        RolapLevel level,
        TupleConstraint constraint,
        List<RolapMember> members)
    {
        putChildren(level, constraint, members);
    }

    public void putChildren(
        RolapLevel level,
        TupleConstraint constraint,
        List<RolapMember> members)
    {
        mapLevelToMembers.put(level, constraint, members);
    }

    public List<RolapMember> getChildrenFromCache(
        RolapMember member,
        MemberChildrenConstraint constraint)
    {
        if (constraint == null) {
            constraint =
                sqlConstraintFactory.getMemberChildrenConstraint(null);
        }
        return mapMemberToChildren.get(member, constraint);
    }

    public void putChildren(
        RolapMember member,
        MemberChildrenConstraint constraint,
        List<RolapMember> children)
    {
        if (constraint == null) {
            constraint =
                sqlConstraintFactory.getMemberChildrenConstraint(null);
        }
        mapMemberToChildren.put(member, constraint, children);
    }

    public List<RolapMember> getLevelMembersFromCache(
        RolapLevel level,
        TupleConstraint constraint)
    {
        if (constraint == null) {
            constraint = sqlConstraintFactory.getLevelMembersConstraint(null);
        }
        return mapLevelToMembers.get(level, constraint);
    }

    // Must sync here because we want the three maps to be modified together.
    public synchronized void flushCache() {
        mapMemberToChildren.clear();
        mapKeyToMember.clear();
        mapLevelToMembers.clear();
        // We also need to clear the approxRowCount of each level.
        for (Level level : rolapHierarchy.getLevels()) {
            ((RolapLevel)level).setApproxRowCount(Integer.MIN_VALUE);
        }
    }

    public DataSourceChangeListener getChangeListener() {
        return changeListener;
    }

    public void setChangeListener(DataSourceChangeListener listener) {
        changeListener = listener;
    }

    public boolean isMutable()
    {
        return true;
    }

    public synchronized RolapMember removeMember(Object key)
    {
        // Flush entries from the level-to-members map
        // for member's level and all child levels.
        // Important: Do this even if the member is apparently not in the cache.
        RolapLevel level = ((MemberKey) key).getLevel();
        if (level == null) {
            level = (RolapLevel) this.rolapHierarchy.getLevels()[0];
        }
        final RolapLevel levelRef = level;
        mapLevelToMembers.getCache().execute(
            new SmartCache.SmartCacheTask
                <Pair<RolapLevel, Object>, List<RolapMember>>()
            {
                public void execute(
                    Iterator<Entry<Pair
                        <RolapLevel, Object>, List<RolapMember>>> iterator)
                {
                    while (iterator.hasNext()) {
                        Map.Entry<Pair
                            <RolapLevel, Object>, List<RolapMember>> entry =
                            iterator.next();
                        final RolapLevel cacheLevel = entry.getKey().left;
                        if (cacheLevel.equals(levelRef)
                            || (cacheLevel.getHierarchy()
                            .equals(levelRef.getHierarchy())
                            && cacheLevel.getDepth()
                            >= levelRef.getDepth()))
                        {
                            iterator.remove();
                        }
                    }
                }
            });

        final RolapMember member = getMember(key);
        if (member == null) {
            // not in cache
            return null;
        }

        // Drop member from the member-to-children map, wherever it occurs as
        // a parent or as a child, regardless of the constraint.
        final RolapMember parent = member.getParentMember();
        mapMemberToChildren.cache.execute(
            new SmartCache.SmartCacheTask
                <Pair<RolapMember, Object>, List<RolapMember>>()
            {
                public void execute(
                    Iterator<Entry
                        <Pair<RolapMember, Object>, List<RolapMember>>> iter)
                {
                    while (iter.hasNext()) {
                        Map.Entry<Pair
                            <RolapMember, Object>, List<RolapMember>> entry =
                                iter.next();
                        final RolapMember member1 = entry.getKey().left;
                        final Object constraint = entry.getKey().right;

                        // Cache key is (member's parent, constraint);
                        // cache value is a list of member's siblings;
                        // If constraint is trivial remove member from list
                        // of siblings; otherwise it's safer to nuke the cache
                        // entry
                        if (Util.equals(member1, parent)) {
                            if (constraint
                                == DefaultMemberChildrenConstraint.instance())
                            {
                                List<RolapMember> siblings = entry.getValue();
                                boolean removedIt = siblings.remove(member);
                                Util.discard(removedIt);
                            } else {
                                iter.remove();
                            }
                        }

                        // cache is (member, some constraint);
                        // cache value is list of member's children;
                        // remove cache entry
                        if (Util.equals(member1, member)) {
                            iter.remove();
                        }
                    }
                }
            });

        // drop it from the lookup-cache
        return mapKeyToMember.put(key, null);
    }

    public RolapMember removeMemberAndDescendants(Object key) {
        // Can use mapMemberToChildren recursively. No need to update inferior
        // lists of children. Do need to update inferior lists of level-peers.
        return null; // STUB
    }
}

// End MemberCacheHelper.java

