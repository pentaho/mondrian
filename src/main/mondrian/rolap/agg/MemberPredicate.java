/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapMember;
import mondrian.rolap.StarPredicate;

/**
 * Constraint defined by a member.
 *
 * <p>If the key is non-composite it is implemented by
 * {@link MemberColumnPredicate}; otherwise by {@link MemberTuplePredicate}.
 *
 * @author jhyde
 * @since May 5, 2011
 */
public interface MemberPredicate extends StarPredicate {
    /**
     * Returns the <code>Member</code>.
     *
     * @return Returns the <code>Member</code>, not null.
     */
    public RolapMember getMember();
}

// End MemberPredicate.java
