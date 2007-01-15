/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.calc.*;

import java.util.List;

/**
 * Adapter which computes an expression and converts it to any required type.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 26, 2005
 */
public abstract class GenericCalc
        extends AbstractCalc
        implements ListCalc, IterCalc,
            StringCalc, IntegerCalc, DoubleCalc, BooleanCalc,
            VoidCalc, MemberCalc, LevelCalc, HierarchyCalc, DimensionCalc
{

    protected GenericCalc(Exp exp) {
        super(exp);
    }

    public List evaluateList(Evaluator evaluator) {
        return (List) evaluate(evaluator);
    }

    public Iterable evaluateIterable(Evaluator evaluator) {
        return (Iterable) evaluate(evaluator);
    }

    public String evaluateString(Evaluator evaluator) {
        return (String) evaluate(evaluator);
    }

    public int evaluateInteger(Evaluator evaluator) {
        return ((Number) evaluate(evaluator)).intValue();
    }

    public double evaluateDouble(Evaluator evaluator) {
        final Object o = evaluate(evaluator);
        final Number number = (Number) o;
        return number == null ?
                FunUtil.DoubleNull :
                number.doubleValue();
    }

    public boolean evaluateBoolean(Evaluator evaluator) {
        return (Boolean) evaluate(evaluator);
    }

    public void evaluateVoid(Evaluator evaluator) {
        final Object result = evaluate(evaluator);
        assert result == null;
    }

    public Member evaluateMember(Evaluator evaluator) {
        return (Member) evaluate(evaluator);
    }

    public Level evaluateLevel(Evaluator evaluator) {
        return (Level) evaluate(evaluator);
    }

    public Hierarchy evaluateHierarchy(Evaluator evaluator) {
        return (Hierarchy) evaluate(evaluator);
    }

    public Dimension evaluateDimension(Evaluator evaluator) {
        return (Dimension) evaluate(evaluator);
    }
}

// End GenericCalc.java
