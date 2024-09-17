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

package mondrian.rolap.sql;

import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.List;

/**
 * "Light version" of a {@link mondrian.rolap.sql.TupleConstraint},
 * represents one of
 * member.children, level.members, member.descendants, {enumeration}.
 */
public interface CrossJoinArg {
    CrossJoinArg[] EMPTY_ARRAY = new CrossJoinArg[0];

    RolapLevel getLevel();

    List<RolapMember> getMembers();

    void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar);

    boolean isPreferInterpreter(boolean joinArg);
}

// End CrossJoinArg.java
