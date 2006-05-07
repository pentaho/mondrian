/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapMember;

/**
 * A <code>ColumnConstraint</code> is an Object to constraining a
 * column (WHERE-Clause) when a segment is loaded.
 *
 * @version $Id$
 */
public class ColumnConstraint implements Comparable {

    private final Object value;

    /**
     * Creates a column constraint.
     *
     * @param value Value to constraint the column to. (We require that it is
     *   {@link Comparable} because we will sort the values in order to
     *   generate deterministic SQL.)
     */
    public ColumnConstraint(Object value) {
        assert value != null;
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public RolapMember getMember() {
        return null;
    }

    public int compareTo(Object o) {
        ColumnConstraint that = (ColumnConstraint) o;
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

    /**
     * Returns whether this constraint has the same constraining effect as the
     * other constraint. This is weaker than {@link #equals(Object)} -- it is
     * possible for two different members to constraint the same column in the
     * same way.
     */
    public boolean equalConstraint(ColumnConstraint that) {
        return this.value.equals(that.value);
    }

    public boolean equals(Object other) {
        if (!(other instanceof ColumnConstraint)) {
            return false;
        }
        final ColumnConstraint that = (ColumnConstraint) other;
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
}

// End ColumnConstraint.java
