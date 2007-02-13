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

import mondrian.rolap.RolapStar;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.StarColumnPredicate;

import java.util.Collection;

/**
 * A constraint which requires a column to have a particular value.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 2, 2006
 */
public class ValueColumnPredicate
    extends AbstractColumnPredicate
    implements Comparable
{
    private final Object value;

    /**
     * Creates a column constraint.
     *
     * @param value Value to constraint the column to. (We require that it is
     *   {@link Comparable} because we will sort the values in order to
     *   generate deterministic SQL.)
     */
    public ValueColumnPredicate(
        RolapStar.Column constrainedColumn,
        Object value)
    {
        super(constrainedColumn);
        assert value != null;
        assert ! (value instanceof StarColumnPredicate);
        this.value = value;
    }

    /**
     * Returns the value which the column is compared to.
     */
    public Object getValue() {
        return value;
    }

    public String toString() {
        return String.valueOf(value);
    }

    public boolean equalConstraint(StarPredicate that) {
        return that instanceof ValueColumnPredicate &&
            this.value.equals(((ValueColumnPredicate) that).value);
    }

    public int compareTo(Object o) {
        ValueColumnPredicate that = (ValueColumnPredicate) o;
        if (this.value instanceof Comparable &&
                that.value instanceof Comparable &&
                this.value.getClass() == that.value.getClass()) {
            return ((Comparable) this.value).compareTo(that.value);
        } else {
            String thisComp = String.valueOf(this.value);
            String thatComp = String.valueOf(that.value);
            return thisComp.compareTo(thatComp);
        }
    }

    public boolean equals(Object other) {
        if (!(other instanceof ValueColumnPredicate)) {
            return false;
        }
        final ValueColumnPredicate that = (ValueColumnPredicate) other;
        if (value != null) {
            return value.equals(that.getValue());
        } else {
            return null == that.getValue();
        }
    }

    public int hashCode() {
        if (value != null) {
            return value.hashCode();
        }
        return 0;
    }

    public void values(Collection collection) {
        collection.add(value);
    }

    public boolean evaluate(Object value) {
        return this.value.equals(value);
    }

    public void describe(StringBuilder buf) {
        buf.append(value);
    }

    public Overlap intersect(StarColumnPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    public boolean mightIntersect(StarPredicate other) {
        return ((StarColumnPredicate) other).evaluate(value);
    }

    public StarColumnPredicate minus(StarPredicate predicate) {
        assert predicate != null;
        if (((StarColumnPredicate) predicate).evaluate(value)) {
            return LiteralStarPredicate.FALSE;
        } else {
            return this;
        }
    }
}

// End ValueColumnPredicate.java
