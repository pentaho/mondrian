/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.rolap.agg;

import mondrian.rolap.*;

import java.util.List;

/**
 * Column constraint defined by a member.
 *
 * @author jhyde
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
        super(column, member.getKey());
        this.member = member;
    }

    // for debug
    public String toString() {
        return member.getUniqueName();
    }

    public List<RolapStar.Column> getConstrainedColumnList() {
        return super.getConstrainedColumnList();
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

    public StarColumnPredicate cloneWithColumn(RolapStar.Column column) {
        return new MemberColumnPredicate(column, member);
    }
}

// End MemberColumnPredicate.java
