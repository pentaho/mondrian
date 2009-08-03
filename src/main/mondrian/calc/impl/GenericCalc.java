/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.calc.*;

import java.util.*;

/**
 * Adapter which computes a scalar or tuple expression and converts it to any
 * required type.
 *
 * @see mondrian.calc.impl.GenericIterCalc
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 26, 2005
 */
public abstract class GenericCalc
    extends AbstractCalc
    implements TupleCalc,
    StringCalc, IntegerCalc, DoubleCalc, BooleanCalc, DateTimeCalc,
    VoidCalc, MemberCalc, LevelCalc, HierarchyCalc, DimensionCalc
{
    protected GenericCalc(Exp exp) {
        super(exp);
    }

    public Member[] evaluateTuple(Evaluator evaluator) {
        return (Member[]) evaluate(evaluator);
    }

    public String evaluateString(Evaluator evaluator) {
        return (String) evaluate(evaluator);
    }

    public int evaluateInteger(Evaluator evaluator) {
        Object o = evaluate(evaluator);
        final Number number = (Number) o;
        return number == null ?
                FunUtil.IntegerNull :
                number.intValue();
    }

    public double evaluateDouble(Evaluator evaluator) {
        final Object o = evaluate(evaluator);
        final Number number = (Number) o;
        return numberToDouble(number);
    }

    public static double numberToDouble(Number number) {
        return number == null ?
                FunUtil.DoubleNull :
                number.doubleValue();
    }

    public boolean evaluateBoolean(Evaluator evaluator) {
        return (Boolean) evaluate(evaluator);
    }

    public Date evaluateDateTime(Evaluator evaluator) {
        return (Date) evaluate(evaluator);
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
