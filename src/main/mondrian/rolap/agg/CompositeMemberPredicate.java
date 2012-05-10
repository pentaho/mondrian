/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.spi.Dialect;

import java.util.*;

/**
 * Constraint defined by a member.
 *
 * <p>Since a member's key may be composite, it is not necessarily a
 * single-column constraint.
 *
 * @author jhyde
 * @since Mar 16, 2006
 */
public class CompositeMemberPredicate
    implements MemberPredicate
{
    private final RolapMember member;
    private final List<PredicateColumn> columnList =
        new ArrayList<PredicateColumn>();

    /**
     * Creates a CompositeMemberPredicate.
     *
     * @param router Determines route to fact table
     * @param member Member
     */
    public CompositeMemberPredicate(
        RolapSchema.PhysRouter router,
        RolapMember member)
    {
        this.member = member;
        assert member != null;
        for (RolapSchema.PhysColumn physColumn
            : member.getLevel().getAttribute().getKeyList())
        {
            columnList.add(
                new PredicateColumn(
                    router,
                    physColumn));
        }
        assert columnList.size() > 1 : "use MemberColumnPredicate for non-comp";
    }

    // for debug
    public String toString() {
        return member.getUniqueName();
    }

    public List<PredicateColumn> getColumnList() {
        return columnList;
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
        if (!(other instanceof CompositeMemberPredicate)) {
            return false;
        }
        final CompositeMemberPredicate that = (CompositeMemberPredicate) other;
        return member.equals(that.getMember());
    }

    public int hashCode() {
        return member.hashCode();
    }

    public void describe(StringBuilder buf) {
        buf.append(member.getUniqueName());
    }

    public BitKey getConstrainedColumnBitKey() {
        throw new UnsupportedOperationException("TODO:");
    }

    public boolean evaluate(List<Object> valueList) {
        throw new UnsupportedOperationException("TODO:");
    }

    public boolean equalConstraint(StarPredicate that) {
        throw new UnsupportedOperationException("TODO:");
    }

    public StarPredicate minus(StarPredicate predicate) {
        throw new UnsupportedOperationException("TODO:");
    }

    public StarPredicate or(StarPredicate predicate) {
        throw new UnsupportedOperationException("TODO:");
    }

    public StarPredicate and(StarPredicate predicate) {
        throw new UnsupportedOperationException("TODO:");
    }

    public void toSql(Dialect dialect, StringBuilder buf) {
        throw new UnsupportedOperationException("TODO:");
    }
}

// End CompositeMemberPredicate.java
