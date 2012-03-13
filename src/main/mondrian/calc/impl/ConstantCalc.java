/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.calc.impl;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.type.*;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.LevelType;

import java.util.Map;

/**
 * Calculator which always returns the same value.
 *
 * @author jhyde
 * @since Sep 27, 2005
 */
public class ConstantCalc extends GenericCalc {
    private final Object o;
    private final int i;
    private final double d;

    public ConstantCalc(Type type, Object o) {
        super(new DummyExp(type));
        this.o = o;
        this.i = initializeInteger(o);
        this.d = initializeDouble(o);
    }

    @Override
    protected String getName() {
        return "Literal";
    }

    public ResultStyle getResultStyle() {
        return o == null
            ? ResultStyle.VALUE
            : ResultStyle.VALUE_NOT_NULL;
    }

    private double initializeDouble(Object o) {
        double value;
        if (o instanceof Number) {
            value = ((Number) o).doubleValue();
        } else {
            if (o == null) {
                value = FunUtil.DoubleNull;
            } else {
                value = 0;
            }
        }
        return value;
    }

    private int initializeInteger(Object o) {
        int value;
        if (o instanceof Number) {
            value = ((Number) o).intValue();
        } else {
            if (o == null) {
                value = FunUtil.IntegerNull;
            } else {
                value = 0;
            }
        }
        return value;
    }

    @Override
    public void collectArguments(Map<String, Object> arguments) {
        super.collectArguments(arguments);
        arguments.put("value", o);
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

    public boolean dependsOn(Hierarchy hierarchy) {
        // A constant -- including a catalog element -- will evaluate to the
        // same result regardless of the evaluation context. For example, the
        // member [Gender].[M] does not 'depend on' the [Gender] dimension.
        return false;
    }

    public Calc[] getCalcs() {
        return new Calc[0];
    }

    /**
     * Creates an expression which evaluates to a given integer.
     *
     * @param i Integer value
     * @return Constant integer expression
     */
    public static ConstantCalc constantInteger(int i) {
        return new ConstantCalc(new DecimalType(Integer.MAX_VALUE, 0), i);
    }

    /**
     * Creates an expression which evaluates to a given double.
     *
     * @param v Double value
     * @return Constant double expression
     */
    public static DoubleCalc constantDouble(double v) {
        return new ConstantCalc(new NumericType(), v);
    }

    /**
     * Creates an expression which evaluates to a given string.
     *
     * @param s String value
     * @return Constant string expression
     */
    public static StringCalc constantString(String s) {
        return new ConstantCalc(new StringType(), s);
    }

    /**
     * Creates an expression which evaluates to a given boolean.
     *
     * @param b Boolean value
     * @return Constant boolean expression
     */
    public static BooleanCalc constantBoolean(boolean b) {
        return new ConstantCalc(new BooleanType(), b);
    }

    /**
     * Creates an expression which evaluates to null.
     *
     * @param type Type
     * @return Constant null expression
     */
    public static ConstantCalc constantNull(Type type) {
        return new ConstantCalc(type, null);
    }

    /**
     * Creates an expression which evaluates to a given member.
     *
     * @param member Member
     * @return Constant member expression
     */
    public static Calc constantMember(Member member) {
        return new ConstantCalc(
            MemberType.forMember(member),
            member);
    }

    /**
     * Creates an expression which evaluates to a given level.
     *
     * @param level Level
     * @return Constant level expression
     */
    public static Calc constantLevel(Level level) {
        return new ConstantCalc(
            LevelType.forLevel(level),
            level);
    }

    /**
     * Creates an expression which evaluates to a given hierarchy.
     *
     * @param hierarchy Hierarchy
     * @return Constant hierarchy expression
     */
    public static Calc constantHierarchy(Hierarchy hierarchy) {
        return new ConstantCalc(
            HierarchyType.forHierarchy(hierarchy),
            hierarchy);
    }

    /**
     * Creates an expression which evaluates to a given dimension.
     *
     * @param dimension Dimension
     * @return Constant dimension expression
     */
    public static Calc constantDimension(Dimension dimension) {
        return new ConstantCalc(
            DimensionType.forDimension(dimension),
            dimension);
    }
}

// End ConstantCalc.java
