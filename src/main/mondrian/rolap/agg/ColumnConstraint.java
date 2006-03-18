/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
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
public class ColumnConstraint {

    private final Object value;

    public ColumnConstraint(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public RolapMember getMember() {
        return null;
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
