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
import mondrian.rolap.RolapStar;

/**
 * Column constraint defined by a member.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 16, 2006
 */
public class MemberColumnPredicate extends ValueColumnPredicate {
    private final RolapMember member;

    /**
     * Creates a MemberColumnPredicate
     *
     * @param column Constrained column
     * @param member Member to constrain column to; must not be null
     */
    public MemberColumnPredicate(RolapStar.Column column, RolapMember member) {
        super(column, member.getSqlKey());
        this.member = member;
    }

    // for debug
    public String toString() {
        return member.getUniqueName();
    }

    /**
     * Returns the <code>Member</code>.
     *
     * @return Returns the <code>Member</code>, not null.
     */
    public RolapMember getMember() {
        return member;
    }

    public boolean equals(Object other) {
        if (!(other instanceof MemberColumnPredicate)) {
            return false;
        }
        final MemberColumnPredicate that = (MemberColumnPredicate) other;
        return member.equals(that.getMember());
    }

    public int hashCode() {
        return member.hashCode();
    }

    public void describe(StringBuilder buf) {
        buf.append(member.getUniqueName());
    }
}

// End MemberColumnPredicate.java
