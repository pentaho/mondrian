/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.spi.Dialect;

import java.util.*;

/**
 * A <code>AbstractColumnPredicate</code> is an abstract implementation for
 * {@link mondrian.rolap.StarColumnPredicate}.
 */
public abstract class AbstractColumnPredicate implements StarColumnPredicate {
    protected final PredicateColumn constrainedColumn;
    private BitKey constrainedColumnBitKey;

    /**
     * Creates an AbstractColumnPredicate.
     *
     * @param constrainedColumn Constrained column
     */
    protected AbstractColumnPredicate(
        PredicateColumn constrainedColumn)
    {
        this.constrainedColumn = constrainedColumn;
        assert constrainedColumn != null;
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append(constrainedColumn.physColumn.toSql());
        describe(buf);
        return buf.toString();
    }

    public final PredicateColumn getColumn() {
        return constrainedColumn;
    }

    public List<PredicateColumn> getColumnList() {
        return Collections.singletonList(constrainedColumn);
    }

    public BitKey getConstrainedColumnBitKey() {
        if (constrainedColumnBitKey == null) {
            constrainedColumnBitKey =
                BitKey.Factory.makeBitKey(
                    constrainedColumn.physColumn.relation.getSchema()
                        .getColumnCount());
            constrainedColumnBitKey.set(constrainedColumn.physColumn.ordinal());
        }
        return constrainedColumnBitKey;
    }

    public boolean evaluate(List<Object> valueList) {
        assert valueList.size() == 1;
        return evaluate(valueList.get(0));
    }

    public boolean equalConstraint(StarPredicate that) {
        return false;
    }

    public StarPredicate or(StarPredicate predicate) {
        if (predicate instanceof StarColumnPredicate) {
            StarColumnPredicate starColumnPredicate =
                (StarColumnPredicate) predicate;
            if (starColumnPredicate.getColumn()
                == getColumn())
            {
                return orColumn(starColumnPredicate);
            }
        }
        return Predicates.or(Arrays.asList(this, predicate));
    }

    public StarColumnPredicate orColumn(StarColumnPredicate predicate) {
        assert predicate.getColumn() == getColumn();
        if (predicate instanceof ListColumnPredicate) {
            ListColumnPredicate that = (ListColumnPredicate) predicate;
            final List<StarColumnPredicate> list =
                new ArrayList<StarColumnPredicate>();
            list.add(this);
            list.addAll(that.getPredicates());
            return new ListColumnPredicate(
                getColumn(),
                list);
        } else {
            final List<StarColumnPredicate> list =
                new ArrayList<StarColumnPredicate>(2);
            list.add(this);
            list.add(predicate);
            return new ListColumnPredicate(
                getColumn(),
                list);
        }
    }

    public StarPredicate and(StarPredicate predicate) {
        return new AndPredicate(Arrays.asList(this, predicate));
    }

    public void toSql(Dialect dialect, StringBuilder buf) {
        throw Util.needToImplement(this);
    }
}

// End AbstractColumnPredicate.java
