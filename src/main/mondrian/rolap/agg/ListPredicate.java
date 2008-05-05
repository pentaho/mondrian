/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.StarPredicate;
import mondrian.rolap.RolapStar;
import mondrian.rolap.BitKey;
import mondrian.rolap.sql.SqlQuery;
import mondrian.olap.Util;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Base class for {@link AndPredicate} and {@link OrPredicate}.
 *
 * @see mondrian.rolap.agg.ListColumnPredicate
 *
 * @author jhyde
 * @version $Id$
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
    
    protected final List<RolapStar.Column> columns =
        new ArrayList<RolapStar.Column>();

    protected BitKey columnBitKey;
    
    protected ListPredicate(List<StarPredicate> predicateList) {
        columnBitKey = null;
        childrenHashMap = null;
        for (StarPredicate predicate : predicateList) {
            if (columnBitKey == null) {
                columnBitKey =
                    predicate.getConstrainedColumnBitKey().copy();
            } else {
                columnBitKey =
                    columnBitKey.or(predicate.getConstrainedColumnBitKey());
            }
            children.add(predicate);
            for (RolapStar.Column column :
                predicate.getConstrainedColumnList()) {
                if (!columns.contains(column)) {
                    columns.add(column);
                }
            }
        }
    }

    public List<RolapStar.Column> getConstrainedColumnList() {
        return columns;
    }

    public BitKey getConstrainedColumnBitKey() {
        return columnBitKey;
    }

    public List<StarPredicate> getChildren() {
        return children;
    }
    
    public int hashCode() {
        // Don't use the default list hashcode because we want a hash code
        // that's not order dependent
        int hashCode = 1;
        for (StarPredicate child : children) {
            hashCode *= child.hashCode();
        }
        hashCode ^= children.size();
        return hashCode;
    }
    
    public boolean equalConstraint(StarPredicate that) {
        boolean isEqual = 
            that instanceof ListPredicate &&
            getConstrainedColumnBitKey().equals(
                that.getConstrainedColumnBitKey());
        
        if (isEqual) {        
            ListPredicate thatPred = (ListPredicate) that;
            if (getOp() != thatPred.getOp() ||
                getChildren().size() != thatPred.getChildren().size()) {
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
