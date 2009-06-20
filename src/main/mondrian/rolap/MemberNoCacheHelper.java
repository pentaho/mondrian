/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008 TASecurity Group Spain
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.rolap;

import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.spi.DataSourceChangeListener;
import mondrian.olap.Util;

import java.util.*;

/**
 * Encapsulation of member caching for no caching.
 *
 * @author Luis F. Canals (lcanals@tasecurity.net)
 * @version 1.0
 */
public class MemberNoCacheHelper extends MemberCacheHelper {
    DataSourceChangeListener changeListener;

    public MemberNoCacheHelper() {
        super(null);
    }

    // implement MemberCache
    public RolapMember getMember(
        Object key,
        boolean mustCheckCacheStatus)
    {
        return null;
    }


    // implement MemberCache
    public Object putMember(Object key, RolapMember value) {
        return value;
    }

    // implement MemberCache
    public Object makeKey(RolapMember parent, Object key) {
        return new MemberKey(parent, key);
    }

    // implement MemberCache
    // synchronization: Must synchronize, because modifies mapKeyToMember
    public synchronized RolapMember getMember(Object key) {
        return getMember(key, true);
    }

    public void checkCacheStatus() {
    }

    /**
     * ???
     *
     * @param level
     * @param constraint
     * @param members
     */
    public void putLevelMembersInCache(
        RolapLevel level,
        TupleConstraint constraint,
        List<RolapMember> members)
    {
    }

    public List<RolapMember> getChildrenFromCache(
        RolapMember member,
        MemberChildrenConstraint constraint)
    {
        return null;
    }

    public void putChildren(
        RolapMember member,
        MemberChildrenConstraint constraint,
        List<RolapMember> children)
    {
    }

    public List<RolapMember> getLevelMembersFromCache(
        RolapLevel level,
        TupleConstraint constraint)
    {
        return null;
    }

    public DataSourceChangeListener getChangeListener() {
        return changeListener;
    }

    public void setChangeListener(DataSourceChangeListener listener) {
        changeListener = listener;
    }

    public boolean isMutable() {
        return true;
    }

    public synchronized RolapMember removeMember(Object key) {
        return null;
    }

    public synchronized RolapMember removeMemberAndDescendants(Object key) {
        return null;
    }
}

// End MemberNoCacheHelper.java
