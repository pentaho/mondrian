/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.*;

import java.util.Map;
import java.util.Collections;

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
 */
public class RolapCalculatedMember extends RolapMember {
    private final Formula formula;
    private Map<String, Annotation> annotationMap;

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
        super(parentMember, level, name, null, MemberType.FORMULA);
        this.formula = formula;
        this.annotationMap = Collections.emptyMap();
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
    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    void setAnnotationMap(Map<String, Annotation> annotationMap) {
        assert annotationMap != null;
        this.annotationMap = annotationMap;
    }
}

// End RolapCalculatedMember.java
