/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap.sql;

import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents one of:
 * <ul>
 * <li>Level.Members:  member == null and level != null</li>
 * <li>Member.Children: member != null and level =
 *     member.getLevel().getChildLevel()</li>
 * <li>Member.Descendants: member != null and level == some level below
 *     member.getLevel()</li>
 * </ul>
 */
public class DescendantsCrossJoinArg implements CrossJoinArg {
    RolapMember member;
    RolapLevel level;

    public DescendantsCrossJoinArg(RolapLevel level, RolapMember member) {
        this.level = level;
        this.member = member;
    }

    public RolapLevel getLevel() {
        return level;
    }

    public List<RolapMember> getMembers() {
        if (member == null) {
            return null;
        }
        final List<RolapMember> list = new ArrayList<RolapMember>();
        list.add(member);
        return list;
    }

    public void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar)
    {
        if (member != null) {
            SqlConstraintUtils.addMemberConstraint(
                sqlQuery, baseCube, aggStar, member, true);
        }
    }

    public boolean isPreferInterpreter(boolean joinArg) {
        return false;
    }

    private boolean equals(Object o1, Object o2) {
        return o1 == null ? o2 == null : o1.equals(o2);
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof DescendantsCrossJoinArg)) {
            return false;
        }
        DescendantsCrossJoinArg that = (DescendantsCrossJoinArg) obj;
        if (!equals(this.level, that.level)) {
            return false;
        }
        return equals(this.member, that.member);
    }

    public int hashCode() {
        int c = 1;
        if (level != null) {
            c = level.hashCode();
        }
        if (member != null) {
            c = 31 * c + member.hashCode();
        }
        return c;
    }
}

// End DescendantsCrossJoinArg.java
