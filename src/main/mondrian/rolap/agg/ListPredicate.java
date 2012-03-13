/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;

/**
 * Base class for {@link AndPredicate} and {@link OrPredicate}.
 *
 * @see mondrian.rolap.agg.ListColumnPredicate
 *
 * @author jhyde
 */
public abstract class ListPredicate implements StarPredicate {
    protected final List<StarPredicate> children =
        new ArrayList<StarPredicate>();

    /**
     * Hash map of children predicates, keyed off of the hash code of each
     * child.  Each entry in the map is a list of predicates matching that
     * hash code.
     */
    private HashMap<Integer, List<StarPredicate>> childrenHashMap;

    /**
     * Pre-computed hash code for this list column predicate
     */
    private int hashValue;

    protected final List<RolapStar.Column> columns;

    private BitKey columnBitKey = null;

    protected ListPredicate(List<StarPredicate> predicateList) {
        childrenHashMap = null;
        hashValue = 0;
        // Ensure that columns are sorted by bit-key, for determinacy.
        final SortedSet<RolapStar.Column> columnSet =
            new TreeSet<RolapStar.Column>(RolapStar.Column.COMPARATOR);
        for (StarPredicate predicate : predicateList) {
            children.add(predicate);
            columnSet.addAll(predicate.getConstrainedColumnList());
        }
        columns = new ArrayList<RolapStar.Column>(columnSet);
    }

    public List<RolapStar.Column> getConstrainedColumnList() {
        return columns;
    }

    public BitKey getConstrainedColumnBitKey() {
        if (columnBitKey == null) {
            for (StarPredicate predicate : children) {
                if (columnBitKey == null) {
                    columnBitKey =
                        predicate.getConstrainedColumnBitKey().copy();
                } else {
                    columnBitKey =
                        columnBitKey.or(predicate.getConstrainedColumnBitKey());
                }
            }
        }
        return columnBitKey;
    }

    public List<StarPredicate> getChildren() {
        return children;
    }

    public int hashCode() {
        // Don't use the default list hashcode because we want a hash code
        // that's not order dependent
        if (hashValue == 0) {
            hashValue = 37;
            for (StarPredicate child : children) {
                int childHashCode = child.hashCode();
                if (childHashCode != 0) {
                    hashValue *= childHashCode;
                }
            }
            hashValue ^= children.size();
        }
        return hashValue;
    }

    public boolean equalConstraint(StarPredicate that) {
        boolean isEqual =
            that instanceof ListPredicate
            && getConstrainedColumnBitKey().equals(
                that.getConstrainedColumnBitKey());

        if (isEqual) {
            ListPredicate thatPred = (ListPredicate) that;
            if (getOp() != thatPred.getOp()
                || getChildren().size() != thatPred.getChildren().size())
            {
                isEqual = false;
            }

            if (isEqual) {
                // Create a hash map of the children predicates, if not
                // already done
                if (childrenHashMap == null) {
                    childrenHashMap =
                        new HashMap<Integer, List<StarPredicate>>();
                    for (StarPredicate thisChild : getChildren()) {
                        Integer key = new Integer(thisChild.hashCode());
                        List<StarPredicate> predList = childrenHashMap.get(key);
                        if (predList == null) {
                            predList = new ArrayList<StarPredicate>();
                        }
                        predList.add(thisChild);
                        childrenHashMap.put(key, predList);
                    }
                }

                // Loop through thatPred's children predicates.  There needs
                // to be a matching entry in the hash map for each child
                // predicate.
                for (StarPredicate thatChild : thatPred.getChildren()) {
                    List<StarPredicate> predList =
                        childrenHashMap.get(thatChild.hashCode());
                    if (predList == null) {
                        isEqual = false;
                        break;
                    }
                    boolean foundMatch = false;
                    for (StarPredicate pred : predList) {
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

    public StarPredicate minus(StarPredicate predicate) {
        throw Util.needToImplement(this);
    }

    public void toSql(SqlQuery sqlQuery, StringBuilder buf) {
        if (children.size() == 1) {
            children.get(0).toSql(sqlQuery, buf);
        } else {
            int k = 0;
            buf.append("(");
            for (StarPredicate child : children) {
                if (k++ > 0) {
                    buf.append(" ").append(getOp()).append(" ");
                }
                child.toSql(sqlQuery, buf);
            }
            buf.append(")");
        }
    }

    protected abstract String getOp();

    public void describe(StringBuilder buf) {
        buf.append(getOp()).append("(");
        int k = 0;
        for (StarPredicate child : children) {
            if (k++ > 0) {
                buf.append(", ");
            }
            buf.append(child);
        }
        buf.append(')');
    }


    public String toString() {
        final StringBuilder buf = new StringBuilder();
        describe(buf);
        return buf.toString();
    }
}

// End ListPredicate.java
