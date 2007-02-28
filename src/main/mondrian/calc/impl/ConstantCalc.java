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
import mondrian.olap.type.*;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.LevelType;
import mondrian.calc.*;

/**
 * Calculator which always returns the same value.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public class ConstantCalc extends GenericCalc {
    private final Object o;
    private final int i;
    private final double d;

    public ConstantCalc(Type type, Object o) {
        super(new DummyExp(type));
        this.o = o;
        if (o == null) {
            this.i = mondrian.olap.fun.FunUtil.IntegerNull;
            this.d = mondrian.olap.fun.FunUtil.DoubleNull;
        } else {
            this.i = (o instanceof Number) ? ((Number) o).intValue() : 0;
            this.d = (o instanceof Number) ? ((Number) o).doubleValue() : 0;
        }
    }

    public void accept(CalcWriter calcWriter) {
        calcWriter.getWriter().print(o);
    }

    public Object evaluate(Evaluator evaluator) {
        return o;
    }

    public int evaluateInteger(Evaluator evaluator) {
        return i;
    }

    public double evaluateDouble(Evaluator evaluator) {
        return d;
    }

    public boolean dependsOn(Dimension dimension) {
        // A constant -- including a catalog element -- will evaluate to the
        // same result regardless of the evaluation context. For example, the
        // member [Gender].[M] does not 'depend on' the [Gender] dimension.
        return false;
    }

    public Calc[] getCalcs() {
        return new Calc[0];
    }

    /**
     * Creates an expression which evaluates to an integer.
     */
    public static ConstantCalc constantInteger(int i) {
        return new ConstantCalc(new NumericType(), i);
    }

    /**
     * Creates an expression which evaluates to a string.
     */
    public static StringCalc constantString(String s) {
        return new ConstantCalc(new StringType(), s);
    }

    /**
     * Creates an expression which evaluates to null.
     */
    public static ConstantCalc constantNull(Type type) {
        return new ConstantCalc(type, null);
    }

    /**
     * Creates an expression which evaluates to a member.
     */
    public static Calc constantMember(Member member) {
        return new ConstantCalc(
                MemberType.forMember(member),
                member);
    }

    /**
     * Creates an expression which evaluates to a level.
     */
    public static Calc constantLevel(Level level) {
        return new ConstantCalc(
                LevelType.forLevel(level),
                level);
    }

    /**
     * Creates an expression which evaluates to a hierarchy.
     */
    public static Calc constantHierarchy(Hierarchy hierarchy) {
        return new ConstantCalc(
                HierarchyType.forHierarchy(hierarchy),
                hierarchy);
    }

    /**
     * Creates an expression which evaluates to a dimension.
     */
    public static Calc constantDimension(Dimension dimension) {
        return new ConstantCalc(
                DimensionType.forDimension(dimension),
                dimension);
    }
}

// End ConstantCalc.java
