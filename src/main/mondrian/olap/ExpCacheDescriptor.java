/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds information necessary to add an expression to the expression result
 * cache (see {@link Evaluator#getCachedResult(ExpCacheDescriptor}).
 *
 * @author jhyde
 * @since Aug 16, 2005
 * @version $Id$
 */
public class ExpCacheDescriptor {
    private final Exp exp;
    private int[] dependentDimensionOrdinals;

    /**
     * Creates a descriptor.
     */
    public ExpCacheDescriptor(Exp exp, Evaluator evaluator) {
        this.exp = exp;

        // Compute list of dependent dimensions.
        final List ordinalList = new ArrayList();
        final Member[] members = evaluator.getMembers();
        for (int i = 0; i < members.length; i++) {
            Dimension dimension = members[i].getDimension();
            if (exp.dependsOn(dimension)) {
                ordinalList.add(new Integer(i));
            }
        }
        dependentDimensionOrdinals = new int[ordinalList.size()];
        for (int i = 0; i < dependentDimensionOrdinals.length; i++) {
            dependentDimensionOrdinals[i] =
                    ((Integer) ordinalList.get(i)).intValue();
        }
    }

    public Exp getExp() {
        return exp;
    }

    /**
     * Returns the ordinals of the dimensions which this expression is
     * dependent upon. When the cache descriptor is used to generate a cache
     * key, the key will consist of a member from each of these dimensions.
     */
    public int[] getDependentDimensionOrdinals() {
        return dependentDimensionOrdinals;
    }
}

// End ExpCacheDescriptor.java
