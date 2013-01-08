/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;


/**
 * A <code>RolapCalculatedMember</code> is a member based upon a
 * {@link Formula}.
 *
 * <p>It is created before the formula has been resolved; the formula is
 * responsible for setting the "format_string" property.
 *
 * @author jhyde
 * @since 26 August, 2001
 */
public class RolapCalculatedMember
    extends RolapMemberBase
    implements CalculatedMember
{
    private final Formula formula;
    private Larder larder;

    /**
     * Creates a RolapCalculatedMember.
     *
     * @param parentMember Parent member
     * @param level Level
     * @param name Name
     * @param formula Formula
     */
    RolapCalculatedMember(
        RolapMember parentMember,
        RolapLevel level,
        String name,
        Formula formula)
    {
        // A calculated measure has MemberType.FORMULA because FORMULA
        // overrides MEASURE.
        super(parentMember, level, null, name, MemberType.FORMULA);
        this.formula = formula;
        this.larder = Larders.EMPTY;
    }

    // override RolapMember
    public int getSolveOrder() {
        final Number solveOrder = formula.getSolveOrder();
        return solveOrder == null ? 0 : solveOrder.intValue();
    }

    public Object getPropertyValue(String propertyName, boolean matchCase) {
        if (Util.equal(propertyName, Property.FORMULA.name, matchCase)) {
            return formula;
        } else if (Util.equal(
                propertyName, Property.CHILDREN_CARDINALITY.name, matchCase))
        {
            // Looking up children is unnecessary for calculated member.
            // If do that, SQLException will be thrown.
            return 0;
        } else {
            return super.getPropertyValue(propertyName, matchCase);
        }
    }

    protected boolean computeCalculated(final MemberType memberType) {
        return true;
    }

    public boolean isCalculatedInQuery() {
        final String memberScope =
            (String) getPropertyValue(Property.MEMBER_SCOPE.name);
        return memberScope == null
            || memberScope.equals("QUERY");
    }

    public Exp getExpression() {
        return formula.getExpression();
    }

    public Formula getFormula() {
        return formula;
    }

    @Override
    public Larder getLarder() {
        return larder;
    }

    void setLarder(Larder larder) {
        assert larder != null;
        this.larder = larder;
    }
}

// End RolapCalculatedMember.java
