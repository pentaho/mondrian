/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.Property;

/**
 * A <code>RolapCalculatedMember</code> is a member based upon a
 * {@link Formula}.
 *
 * <p>It is created before the formula has been resolved; the formula is
 * responsible for setting the "format_string" property.
 *
 * @author jhyde
 * @since 26 August, 2001
 * @version $Id$
 **/
class RolapCalculatedMember extends RolapMember {
    private final Formula formula;

    RolapCalculatedMember(
            RolapMember parentMember, RolapLevel level, String name,
            Formula formula) {
        super(parentMember, level, name);
        this.formula = formula;
    }

    // override RolapMember
    public int getSolveOrder() {
        return formula.getSolveOrder();
    }

    public Object getPropertyValue(String name) {
        if (name.equals(Property.FORMULA.name)) {
            return formula;
        } else if (name.equals(Property.CHILDREN_CARDINALITY.name)) {
            // Looking up children is unnecessary for calculated member.
            // If do that, SQLException will be thrown.
            return new Integer(0);
        } else {
            return super.getPropertyValue(name);
        }
    }

    public boolean isCalculated() {
        return true;
    }

    public boolean isCalculatedInQuery() {
        return true;
    }

    public Exp getExpression() {
        return formula.getExpression();
    }

}


// End RolapCalculatedMember.java
