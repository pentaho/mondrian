/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2010 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Member;

/**
 * A <code>RolapMember</code> is a member of a {@link RolapHierarchy}. There are
 * sub-classes for {@link RolapStoredMeasure}, {@link RolapCalculatedMember}.
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public interface RolapMember extends Member, RolapCalculation {
    Object getKey();
    RolapMember getParentMember();
    RolapHierarchy getHierarchy();
    RolapLevel getLevel();

    /** @deprecated will be removed in mondrian-4.0 */
    boolean isAllMember();
}

// End RolapMember.java
