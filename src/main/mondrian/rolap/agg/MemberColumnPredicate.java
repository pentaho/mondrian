/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;

/**
 * Column constraint defined by a member.
 *
 * @see Util#deprecated(Object, boolean) No longer a column predicate;
 * TODO: rename this class.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 16, 2006
 */
public class MemberColumnPredicate
    extends ValueColumnPredicate
    implements MemberPredicate
{
    private final RolapMember member;

    /**
     * Creates a MemberColumnPredicate
     *
     * @param column Constrained column
     * @param member Member to constrain column to; must not be null
     */
    public MemberColumnPredicate(
        RolapSchema.PhysColumn column,
        RolapMember member)
    {
        super(column, member.getKey());
        this.member = member;
        assert column != null;
        assert column == member.getLevel().getAttribute().keyList.get(0);
        assert member.getLevel().getAttribute().keyList.size() == 1;
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
