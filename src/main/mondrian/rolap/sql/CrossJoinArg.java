/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.rolap.*;

import java.util.List;

/**
 * "Light version" of a {@link mondrian.rolap.sql.TupleConstraint},
 * represents one of
 * member.children, level.members, member.descendants, {enumeration}.
 */
public interface CrossJoinArg {
    CrossJoinArg[] EMPTY_ARRAY = new CrossJoinArg[0];

    RolapCubeLevel getLevel();

    List<RolapMember> getMembers();

    void addConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet);

    boolean isPreferInterpreter(boolean joinArg);
}

// End CrossJoinArg.java
