/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.RolapSchema;

import java.util.Comparator;

/**
 * Use of a column in a predicate.
 *
 * <p>The column knows its route back to a fact table.</p>
 */
public class PredicateColumn {
    /**
     * Functor that computes the route to the fact table
     */
    public final RolapSchema.PhysRouter router;

    public final RolapSchema.PhysColumn physColumn;

    /**
     * Creates a PredicateColumn.
     *
     * @param router Determines path to fact table
     */
    public PredicateColumn(
        RolapSchema.PhysRouter router,
        RolapSchema.PhysColumn physColumn)
    {
        this.router = router;
        this.physColumn = physColumn;
        assert physColumn != null;
        assert router != null;
    }

    public static final Comparator<PredicateColumn> COMPARATOR =
        new Comparator<PredicateColumn>() {
            public int compare(
                PredicateColumn object1,
                PredicateColumn object2)
            {
                return Util.compare(
                    object1.physColumn.ordinal(),
                    object2.physColumn.ordinal());
            }
        };
}

// End PredicateColumn.java
