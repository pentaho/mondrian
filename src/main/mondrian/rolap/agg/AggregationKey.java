/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mondrian.rolap.BitKey;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarPredicate;

/**
 * This class defines the column context that an Aggregation is computed for.
 * Column context has two components:
 *   -- the column constraints which define the dimentionality of an
 *   Aggregation
 *   -- an orthogonal context for which the measures are defined. This context
 *   is sometimes referred to as the compound member predicates, and usually of
 *   the shape:
 *      OR(AND(column predicates))
 *   Any column is only used in either column context or compound context, not
 *   both.
 * @author Rushan Chen
 * @version $Id$
 */
public class AggregationKey
{
    /**
     * This is needed because for a Virtual Cube: two CellRequests
     * could have the same BitKey but have different underlying
     * base cubes. Without this, one get the result in the
     * SegmentArrayQuerySpec addMeasure Util.assertTrue being
     * triggered (which is what happened).
     */
    private final RolapStar star;

    private final BitKey constrainedColumnsBitKey;

    /*
     * TODO: change this to SortedMap to speed up comparison.
     */
    private Map<BitKey, StarPredicate> compoundPredicateMap;

    public AggregationKey(CellRequest request) {
        this.constrainedColumnsBitKey = request.getConstrainedColumnsBitKey();
        this.star = request.getMeasure().getStar();
        compoundPredicateMap = request.getCompoundPredicateMap();
    }
    
    public int hashCode() {
        int retCode = 
            constrainedColumnsBitKey.hashCode() ^ star.hashCode();
        if (compoundPredicateMap != null) {
            for (BitKey bitKey : compoundPredicateMap.keySet()) {
                retCode ^= bitKey.hashCode();
            }
        }
        return retCode; 
    }

    public boolean hasSameCompoundPredicate(AggregationKey otherKey) {
        boolean isEqual = false;
        if (compoundPredicateMap.size() == 
                otherKey.compoundPredicateMap.size()) {
            isEqual = true;
            for (BitKey bitKey : compoundPredicateMap.keySet()) {
                StarPredicate thisPred = 
                    compoundPredicateMap.get(bitKey);
                StarPredicate otherPred = 
                    otherKey.compoundPredicateMap.get(bitKey);
                if (thisPred == null || otherPred == null ||
                    !thisPred.equalConstraint(otherPred)) {
                    isEqual = false;
                    break;
                }
            }
        }
        return isEqual;        
    }
    
    public boolean equals(Object other) {
        if (other instanceof AggregationKey) {
            AggregationKey otherKey = (AggregationKey) other;
            if (constrainedColumnsBitKey.equals(otherKey.constrainedColumnsBitKey) && 
                star.equals(otherKey.star) && 
                hasSameCompoundPredicate(otherKey)) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
        return 
            star.getFactTable().getTableName() + " " + 
            constrainedColumnsBitKey.toString() + "\n" +
            compoundPredicateMap.toString();
    }
    
    public BitKey getConstrainedColumnsBitKey() {
        return constrainedColumnsBitKey;
    }
    
    public RolapStar getStar() {
        return star;
    }
    
    /**
     * Get the set of compound predicates
     * @return list of predicates
     */
    public List<StarPredicate> getCompoundPredicateList() {
        return (new ArrayList<StarPredicate>(compoundPredicateMap.values()));
    }
    
}

// End AggregationKey.java
