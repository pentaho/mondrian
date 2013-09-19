/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

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
