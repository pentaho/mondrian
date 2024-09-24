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
    int getMeasureCount();
    RolapStar.Measure getMeasure(int i);
    String getMeasureAlias(int i);
    RolapStar.Column[] getColumns();
    String getColumnAlias(int i);

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

    Pair<String, List<SqlStatement.Type>> generateSqlQuery();
}

// End QuerySpec.java
