/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.spi.Dialect;

import java.util.*;

/**
 * Predicate which is the union of a list of predicates, each of which applies
 * to the same, single column. It evaluates to
 * true if any of the predicates evaluates to true.
 *
 * @see mondrian.rolap.agg.ListColumnPredicate
 *
 * @author jhyde
 * @since Nov 2, 2006
 */
public class ListColumnPredicate extends AbstractColumnPredicate {
    /**
     * List of column predicates.
     */
    private final List<StarColumnPredicate> children;

    /**
     * Hash map of children predicates, keyed off of the hash code of each
     * child.  Each entry in the map is a list of predicates matching that
     * hash code.
     */
    private HashMap<Integer, List<StarColumnPredicate>> childrenHashMap;

    /**
     * Set of child values, if all child predicates are value predicates; null
     * otherwise.
     */
    private final Set<Object> values;

    /**
     * Pre-computed hash code for this list column predicate
     */
    private int hashValue;

    /**
     * Creates a ListColumnPredicate
     *
     * @param column Column being constrained
     * @param list List of child predicates
     */
    public ListColumnPredicate(
        PredicateColumn column,
        List<StarColumnPredicate> list)
    {
        super(column);
        this.children = list;
        childrenHashMap = null;
        hashValue = 0;
        values = createValues(list);
    }

    private static Set<Object> createValues(List<StarColumnPredicate> list) {
        final HashSet<Object> set = new HashSet<Object>();
        for (StarColumnPredicate predicate : list) {
            if (predicate instanceof ValueColumnPredicate) {
                set.add(((ValueColumnPredicate) predicate).getValue());
            } else {
                // One of the children is not a value predicate. We will have to
                // evaluate the predicate long-hand.
                return null;
            }
        }
        return set;
    }

    /**
     * Returns the list of child predicates.
     *
     * @return list of child predicates
     */
    public List<StarColumnPredicate> getPredicates() {
        return children;
    }

    public int hashCode() {
        // Don't use the default list hashcode because we want a hash code
        // that's not order dependent
        if (hashValue == 0) {
            hashValue = 37;
            for (StarColumnPredicate child : children) {
                int childHashCode = child.hashCode();
                if (childHashCode != 0) {
                    hashValue *= childHashCode;
                }
            }
            hashValue ^= children.size();
        }
        return hashValue;
    }

    public boolean equals(Object obj) {
        if (obj instanceof ListColumnPredicate) {
            ListColumnPredicate that = (ListColumnPredicate) obj;
            return this.children.equals(that.children);
        } else {
            return false;
        }
    }

    public void values(Collection<Object> collection) {
        if (values != null) {
            collection.addAll(values);
        } else {
            for (StarColumnPredicate child : children) {
                child.values(collection);
            }
        }
    }

    public boolean evaluate(Object value) {
        for (StarColumnPredicate childPredicate : children) {
            if (childPredicate.evaluate(value)) {
                return true;
            }
        }
        return false;
    }

    public boolean equalConstraint(StarPredicate that) {
        boolean isEqual =
            that instanceof ListColumnPredicate
            && getConstrainedColumnBitKey().equals(
                that.getConstrainedColumnBitKey());

        if (isEqual) {
            ListColumnPredicate thatPred = (ListColumnPredicate) that;
            if (getPredicates().size() != thatPred.getPredicates().size()) {
                isEqual = false;
            } else {
                // Create a hash map of the children predicates, if not
                // already done
                if (childrenHashMap == null) {
                    childrenHashMap =
                        new HashMap<Integer, List<StarColumnPredicate>>();
                    for (StarColumnPredicate thisChild : getPredicates()) {
                        Integer key = thisChild.hashCode();
                        List<StarColumnPredicate> predList =
                            childrenHashMap.get(key);
                        if (predList == null) {
                            predList = new ArrayList<StarColumnPredicate>();
                            childrenHashMap.put(key, predList);
                        }
                        predList.add(thisChild);
                    }
                }

                // Loop through thatPred's children predicates.  There needs
                // to be a matching entry in the hash map for each child
                // predicate.
                for (StarColumnPredicate thatChild : thatPred.getPredicates()) {
                    List<StarColumnPredicate> predList =
                        childrenHashMap.get(thatChild.hashCode());
                    if (predList == null) {
                        isEqual = false;
                        break;
                    }
                    boolean foundMatch = false;
                    for (StarColumnPredicate pred : predList) {
                        if (thatChild.equalConstraint(pred)) {
                            foundMatch = true;
                            break;
                        }
                    }
                    if (!foundMatch) {
                        isEqual = false;
                        break;
                    }
                }
            }
        }
        return isEqual;
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
                (float) matchCount
                / (float) children.size();
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
            final List<Object> thatSet = new ArrayList<Object>();
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
                return Predicates.wildcard(constrainedColumn, false);
            } else {
                // X minus FALSE --> X
                return this;
            }
        }
        if (predicate instanceof LiteralColumnPredicate) {
            LiteralColumnPredicate literalColumnPredicate =
                (LiteralColumnPredicate) predicate;
            if (literalColumnPredicate.getValue()) {
                // X minus TRUE --> FALSE
                return Predicates.wildcard(constrainedColumn, false);
            } else {
                // X minus FALSE --> X
                return this;
            }
        }
        StarColumnPredicate columnPredicate = (StarColumnPredicate) predicate;
        List<StarColumnPredicate> newChildren =
            new ArrayList<StarColumnPredicate>(children);
        int changeCount = 0;
        final Iterator<StarColumnPredicate> iterator = newChildren.iterator();
        while (iterator.hasNext()) {
            ValueColumnPredicate child =
                (ValueColumnPredicate) iterator.next();
            if (columnPredicate.evaluate(child.getValue())) {
                ++changeCount;
                iterator.remove();
            }
        }
        if (changeCount > 0) {
            return new ListColumnPredicate(getColumn(), newChildren);
        } else {
            return this;
        }
    }

    public StarColumnPredicate orColumn(StarColumnPredicate predicate) {
        assert predicate.getColumn() == getColumn();
        if (predicate instanceof ListColumnPredicate) {
            ListColumnPredicate that = (ListColumnPredicate) predicate;
            final List<StarColumnPredicate> list =
                new ArrayList<StarColumnPredicate>(children);
            list.addAll(that.children);
            return new ListColumnPredicate(getColumn(), list);
        } else {
            final List<StarColumnPredicate> list =
                new ArrayList<StarColumnPredicate>(children);
            list.add(predicate);
            return new ListColumnPredicate(getColumn(), list);
        }
    }

    public void toSql(Dialect dialect, StringBuilder buf) {
        List<StarColumnPredicate> predicates = getPredicates();
        if (predicates.size() == 1) {
            predicates.get(0).toSql(dialect, buf);
            return;
        }

        int notNullCount = 0;
        final RolapSchema.PhysColumn column = getColumn().physColumn;
        final String expr = column.toSql();
        final int marker = buf.length(); // to allow backtrack later
        buf.append(expr);
        ValueColumnPredicate firstNotNull = null;
        buf.append(" in (");
        for (StarColumnPredicate predicate1 : predicates) {
            final ValueColumnPredicate predicate2 =
                (ValueColumnPredicate) predicate1;
            Object key = predicate2.getValue();
            if (key == RolapUtil.sqlNullValue) {
                continue;
            }
            if (notNullCount > 0) {
                buf.append(", ");
            } else {
                firstNotNull = predicate2;
            }
            ++notNullCount;
            dialect.quote(buf, key, column.getDatatype());
        }
        buf.append(')');

        // If all of the predicates were non-null, return what we've got, for
        // example, "x in (1, 2, 3)".
        if (notNullCount >= predicates.size()) {
            return;
        }

        // There was at least one null. Reset the buffer to how we
        // originally found it, and generate a more concise expression.
        switch (notNullCount) {
        case 0:
            // Special case -- there were no values besides null.
            // Return, for example, "x is null".
            buf.setLength(marker);
            buf.append(expr);
            buf.append(" is null");
            break;

        case 1:
            // Special case -- one not-null value, and null, for
            // example "(x = 1 or x is null)".
            assert firstNotNull != null;
            buf.setLength(marker);
            buf.append('(');
            buf.append(expr);
            buf.append(" = ");
            dialect.quote(
                buf,
                firstNotNull.getValue(),
                column.getDatatype());
            buf.append(" or ");
            buf.append(expr);
            buf.append(" is null)");
            break;

        default:
            // Nulls and values, for example,
            // "(x in (1, 2) or x IS NULL)".
            String save = buf.substring(marker);
            buf.setLength(marker); // backtrack
            buf.append('(');
            buf.append(save);
            buf.append(" or ");
            buf.append(expr);
            buf.append(" is null)");
            break;
        }
    }
}

// End ListColumnPredicate.java
