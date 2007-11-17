/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/
package mondrian.rolap;

import java.util.List;

import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.spi.DataSourceChangeListener;

/**
 * This class encapsulates member caching
 * 
 * @author Will Gorman (wgorman@pentaho.org)
 *
 */
public class MemberCacheHelper implements MemberCache {
        
    private final SqlConstraintFactory sqlConstraintFactory =
        SqlConstraintFactory.instance();
    
    /** maps a parent member to a list of its children */
    final SmartMemberListCache<RolapMember, List<RolapMember>> 
                                                        mapMemberToChildren;

    /** a cache for alle members to ensure uniqueness */
    SmartCache<Object, RolapMember> mapKeyToMember;
    RolapHierarchy rolapHierarchy;
    DataSourceChangeListener changeListener;
    
    /** maps a level to its members */
    final SmartMemberListCache<RolapLevel, List<RolapMember>> 
                                                        mapLevelToMembers;
    
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
    
    /**
     * implement MemberCache.getMember()
     * synchronization: Must synchronize, because uses mapKeyToMember
     */ 
    public synchronized RolapMember getMember(
        Object key,
        boolean mustCheckCacheStatus)
    {
        if (mustCheckCacheStatus) {
            checkCacheStatus();
        }
        return mapKeyToMember.get(key);
    }
    
    /**
     * implement MemberCache.putMember()
     * synchronization: Must synchronize, because modifies mapKeyToMember
     */
    public synchronized Object putMember(Object key, RolapMember value) {
        return mapKeyToMember.put(key, value);
    }
    
    /** 
     * implement MemberCache.makeKey()
     */
    public Object makeKey(RolapMember parent, Object key) {
        return new MemberKey(parent, key);
    }

    /**
     * implement MemberCache.getMember()
     * synchronization: Must synchronize, because uses mapKeyToMember
     */
    public synchronized RolapMember getMember(Object key) {
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
     * calls to this method should be synchronized
     * 
     * @param level
     * @param constraint
     * @param members
     */
    public synchronized void putLevelMembersInCache(RolapLevel level, 
            TupleConstraint constraint, List<RolapMember> members) {
        mapLevelToMembers.put(level, constraint, members);
    }

    public synchronized List<RolapMember> getChildrenFromCache(
            RolapMember member,
            MemberChildrenConstraint constraint) {
        if (constraint == null) {
            constraint = 
                sqlConstraintFactory.getMemberChildrenConstraint(null);
        }
        return mapMemberToChildren.get(member, constraint);
    }
    
    public synchronized void putChildren(
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

    public synchronized List<RolapMember> getLevelMembersFromCache(
            RolapLevel level,
            TupleConstraint constraint) {
        if (constraint == null) {
            constraint = sqlConstraintFactory.getLevelMembersConstraint(null);
        }
        return mapLevelToMembers.get(level, constraint);
    }

    public synchronized void flushCache() {
        mapMemberToChildren.clear();
        mapKeyToMember.clear();
        mapLevelToMembers.clear();
    }
    
    public DataSourceChangeListener getChangeListener() {
        return changeListener;
    }

    public void setChangeListener(DataSourceChangeListener listener) {
        changeListener = listener;            
    }
}
