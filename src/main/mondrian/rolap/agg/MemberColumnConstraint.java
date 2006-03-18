/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapMember;

/**
 * Column constraint defined by a member.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 16, 2006
 */
public class MemberColumnConstraint extends ColumnConstraint {
    private final RolapMember member;

    public MemberColumnConstraint(RolapMember member) {
        super(member.getSqlKey());
        this.member = member;
        assert member != null;
    }

    public RolapMember getMember() {
        return member;
    }

    public boolean equals(Object other) {
        if (!(other instanceof MemberColumnConstraint)) {
            return false;
        }
        final MemberColumnConstraint that = (MemberColumnConstraint) other;
        return member.equals(that.getMember());
    }

    public int hashCode() {
        return member.hashCode();
    }

}

// End MemberColumnConstraint.java
