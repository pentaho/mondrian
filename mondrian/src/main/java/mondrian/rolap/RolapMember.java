/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



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
