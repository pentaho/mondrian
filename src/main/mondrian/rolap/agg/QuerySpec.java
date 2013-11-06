/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
//
// jhyde, 30 August, 2001
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.util.Pair;

import java.util.List;

/**
 * Contains the information necessary to generate a SQL statement to
 * retrieve a set of cells.
 *
 * @author jhyde
 * @author Richard M. Emberson
 */
public interface QuerySpec {
    RolapStar getStar();

    List<Pair<RolapStar.Measure, String>> getMeasures();

    List<Pair<RolapStar.Column, String>> getColumns();

    /**
     * Returns the predicate on the <code>i</code>th column.
     *
     * <p>If the column is unconstrained, returns
     * {@link LiteralStarPredicate}(true).
     *
     * @param i Column ordinal
     * @return Constraint on column
     */
    StarColumnPredicate getColumnPredicate(int i);

    Pair<String, List<SqlStatement.Type>> generateSqlQuery(String desc);
}

// End QuerySpec.java
