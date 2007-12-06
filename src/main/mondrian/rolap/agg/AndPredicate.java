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

import mondrian.rolap.RolapUtil;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.BitKey;
import mondrian.rolap.sql.SqlQuery;

import java.util.List;
import java.util.ArrayList;

/**
 * Predicate which is the intersection of a list of predicates. It evaluates to
 * true if all of the predicates evaluate to true.
 *
 * @see OrPredicate
 *
 * @author jhyde
 * @version $Id$
 */
public class AndPredicate extends ListPredicate {

    public AndPredicate(List<StarPredicate> predicateList) {
        super(predicateList);
    }

    public boolean evaluate(List<Object> valueList) {
        // NOTE: If we know that every predicate in the list is a
        // ValueColumnPredicate, we could optimize the evaluate method by
        // building a value list at construction time. But it's a tradeoff,
        // considering the extra time and space required.
        for (StarPredicate childPredicate : children) {
            if (childPredicate.evaluate(valueList)) {
                return true;
            }
        }
        return false;
    }

    public StarPredicate and(StarPredicate predicate) {
        if (predicate instanceof AndPredicate) {
            ListPredicate that = (ListPredicate) predicate;
            final List<StarPredicate> list =
                new ArrayList<StarPredicate>(children);
            list.addAll(that.children);
            return new AndPredicate(list);
        } else {
            final List<StarPredicate> list =
                new ArrayList<StarPredicate>(children);
            list.add(predicate);
            return new AndPredicate(list);
        }
    }


    public StarPredicate or(StarPredicate predicate) {
        List<StarPredicate> list = new ArrayList<StarPredicate>();
        list.add(this);
        list.add(predicate);
        return new OrPredicate(list);
    }

    public boolean inListPossible() {
        /*
         * Check the children are all ValueColumnPredicate type
         * and the columnBitKey conforms to the set of columns referenced
         */
        boolean inListPossible = true;
        for (StarPredicate predicate : children) {
            if ((predicate instanceof ValueColumnPredicate) &&
                ((ValueColumnPredicate) predicate).getValue() != RolapUtil.sqlNullValue) {
                continue;
            } else {
                inListPossible = false;
                break;                
            }
        }
        return inListPossible;
    }
    
    public boolean inListPossible(BitKey inListLHS) {
        return (super.columnBitKey.equals(inListLHS) && inListPossible());
    }
    
    /*
     * Generate value list for this predicate to be used in an IN-list
     * sql predicate.
     */
    public void toInListSql(SqlQuery sqlQuery, StringBuilder buf) {
        assert (inListPossible());
        boolean firstValue = true;
        buf.append("(");
        for (StarPredicate predicate : children) {
            if (firstValue) {
                firstValue = false;
            } else {
                buf.append(",");
            }
            assert (predicate instanceof ValueColumnPredicate);
            ValueColumnPredicate pred = (ValueColumnPredicate)predicate;
            sqlQuery.getDialect().quote(
                buf, pred.getValue(), 
                pred.getConstrainedColumn().getDatatype());            
        }
        buf.append(")");
    }
        
    protected String getOp() {
        return "and";
    }
}

// End AndPredicate.java
