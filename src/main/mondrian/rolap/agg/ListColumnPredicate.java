/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.StarColumnPredicate;

import java.util.*;

/**
 * Predicate which is the union of a list of predicates. It evaluates to
 * true if any of the predicates evaluates to true.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 2, 2006
 */
public class ListColumnPredicate extends AbstractColumnPredicate {
    /**
     * List of predicates.
     */
    private final List<StarColumnPredicate> children;

    public ListColumnPredicate(
        RolapStar.Column column, List<StarColumnPredicate> list) {
        super(column);
        this.children = list;
    }

    public List<StarColumnPredicate> getPredicates() {
        return children;
    }


    public int hashCode() {
        return children.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof ListColumnPredicate) {
            ListColumnPredicate that = (ListColumnPredicate) obj;
            return this.children.equals(that.children);
        } else {
            return false;
        }
    }

    public void values(Collection collection) {
        for (StarColumnPredicate child : children) {
            child.values(collection);
        }
    }

    public boolean evaluate(Object value) {
        // NOTE: If we know that every predicate in the list is a
        // ValueColumnPredicate, we could optimize the evaluate method by
        // building a value list at construction time. But it's a tradeoff,
        // considering the extra time and space required.
        for (StarColumnPredicate childPredicate : children) {
            if (childPredicate.evaluate(value)) {
                return true;
            }
        }
        return false;
    }

    public void describe(StringBuilder buf) {
        buf.append("={");
        for (int j = 0; j < children.size(); j++) {
            if (j > 0) {
                buf.append(", ");
            }
            buf.append(children.get(j));
        }
        buf.append('}');
    }

    public Overlap intersect(StarColumnPredicate predicate) {
        int matchCount = 0;
        for (StarColumnPredicate flushPredicate : children) {
            final Overlap r2 = flushPredicate.intersect(predicate);
            if (r2.matched) {
                // A hit!
                if (r2.remaining == null) {
                    // Total match.
                    return r2;
                } else {
                    // Partial match.
                    predicate = r2.remaining;
                    ++matchCount;
                }
            }
        }
        if (matchCount == 0) {
            return new Overlap(false, null, 0f);
        } else {
            float selectivity =
                (float) matchCount /
                    (float) children.size();
            return new Overlap(true, predicate, selectivity);
        }
    }

    public boolean mightIntersect(StarPredicate other) {
        if (other instanceof LiteralStarPredicate) {
            return ((LiteralStarPredicate) other).getValue();
        }
        if (other instanceof ValueColumnPredicate) {
            ValueColumnPredicate valueColumnPredicate =
                (ValueColumnPredicate) other;
            return evaluate(valueColumnPredicate.getValue());
        }
        if (other instanceof ListColumnPredicate) {
            final ArrayList thatSet = new ArrayList();
            ((ListColumnPredicate) other).values(thatSet);
            for (Object o : thatSet) {
                if (evaluate(o)) {
                    return true;
                }
            }
            return false;
        }
        throw Util.newInternal("unknown constraint type " + other);
    }

    public StarColumnPredicate minus(StarPredicate predicate) {
        assert predicate != null;
        if (predicate instanceof LiteralStarPredicate) {
            LiteralStarPredicate literalStarPredicate =
                (LiteralStarPredicate) predicate;
            if (literalStarPredicate.getValue()) {
                // X minus TRUE --> FALSE
                return LiteralStarPredicate.FALSE;
            } else {
                // X minus FALSE --> X
                return this;
            }
        }
        StarColumnPredicate columnPredicate = (StarColumnPredicate) predicate;
        List<StarColumnPredicate> newChildren =
            new ArrayList<StarColumnPredicate>(children);
        int changeCount = 0;
        final Iterator iterator = newChildren.iterator();
        while (iterator.hasNext()) {
            ValueColumnPredicate child =
                (ValueColumnPredicate) iterator.next();
            if (columnPredicate.evaluate(child.getValue())) {
                ++changeCount;
                iterator.remove();
            }
        }
        if (changeCount > 0) {
            return new ListColumnPredicate(getConstrainedColumn(), newChildren);
        } else {
            return this;
        }
    }
}

// End ListColumnPredicate.java
