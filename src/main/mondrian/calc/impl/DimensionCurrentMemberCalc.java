/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.type.MemberType;
import mondrian.calc.DummyExp;
import mondrian.calc.Calc;

/**
 * Expression which returns the current member of a given dimension.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 26, 2005
 */
public class DimensionCurrentMemberCalc extends AbstractMemberCalc {
    private final Dimension dimension;

    public DimensionCurrentMemberCalc(Dimension dimension) {
        super(
                new DummyExp(
                        MemberType.forHierarchy(dimension.getHierarchy())),
                new Calc[0]);
        this.dimension = dimension;
    }

    public Member evaluateMember(Evaluator evaluator) {
        return evaluator.getContext(dimension);
    }

    public boolean dependsOn(Dimension dimension) {
        return dimension == this.dimension;
    }
}

// End DimensionCurrentMemberCalc.java
