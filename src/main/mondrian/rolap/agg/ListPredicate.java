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

    protected final List<RolapStar.Column> columns =
        new ArrayList<RolapStar.Column>();

    protected BitKey columnBitKey;
    
    protected ListPredicate(List<StarPredicate> predicateList) {
        columnBitKey = null; 
        for (StarPredicate predicate : predicateList) {
            if (columnBitKey == null) {
                columnBitKey =
                    predicate.getConstrainedColumnBitKey().copy();
            } else {
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
                for (StarPredicate thisChild : getChildren()) {
                    boolean foundMatch = false;
                    for (StarPredicate thatChild: thatPred.getChildren()) {
                        if (thisChild.equalConstraint(thatChild)) {
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

    public abstract boolean inListPossible();
    public abstract void toInListSql(SqlQuery sqlQuery, StringBuilder buf);
    
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
