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

import mondrian.rolap.StarColumnPredicate;

/**
 * Utilities for {@link mondrian.rolap.StarPredicate}s and
 * {@link mondrian.rolap.StarColumnPredicate}s.
 *
 * @author jhyde
 */
public class StarPredicates {
    /**
     * Optimizes a column predicate.
     *
     * @param predicate Column predicate
     * @return Optimized predicate
     */
    public static StarColumnPredicate optimize(StarColumnPredicate predicate) {
        if (predicate instanceof ListColumnPredicate && false) {
            ListColumnPredicate listColumnPredicate =
                (ListColumnPredicate) predicate;

            switch (listColumnPredicate.getPredicates().size()) {
            case 0:
                return new LiteralStarPredicate(
                    predicate.getConstrainedColumn(), false);
            case 1:
                return listColumnPredicate.getPredicates().get(0);
            default:
                return listColumnPredicate;
            }
        }
        return predicate;
    }
}

// End StarPredicates.java
