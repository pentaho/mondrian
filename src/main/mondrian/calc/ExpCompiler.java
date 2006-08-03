/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.*;

/**
 * Mediates the compilation of an expression ({@link mondrian.olap.Exp})
 * into a compiled expression ({@link Calc}).
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 28, 2005
 */
public interface ExpCompiler {
    /**
     * Returns the evaluator to be used for evaluating expressions during the
     * compilation process.
     */
    Evaluator getEvaluator();
    Validator getValidator();

    Calc compile(Exp exp);

    /**
     * Compiles an expression which yields a {@link Member} result.
     */
    MemberCalc compileMember(Exp exp);

    /**
     * Compiles an expression which yields a {@link Level} result.
     */
    LevelCalc compileLevel(Exp exp);

    /**
     * Compiles an expression which yields a {@link Dimension} result.
     */
    DimensionCalc compileDimension(Exp exp);

    /**
     * Compiles an expression which yields a {@link Hierarchy} result.
     */
    HierarchyCalc compileHierarchy(Exp exp);

    /**
     * Compiles an expression which yields an <code>int</code> result.
     * The expression is implicitly converted into a scalar.
     */
    IntegerCalc compileInteger(Exp exp);

    /**
     * Compiles an expression which yields a {@link String} result.
     * The expression is implicitly converted into a scalar.
     */
    StringCalc compileString(Exp exp);

    /**
     * Compiles an expression which yields a {@link java.util.List} result.
     * Such an expression is generally a list of {@link Member} objects or a
     * list of tuples (each represented by a {@link Member} array).
     */
    ListCalc compileList(Exp exp);

    /**
     * Compiles an expression which yields a <code>boolean</code> result.
     */
    BooleanCalc compileBoolean(Exp exp);

    /**
     * Compiles an expression which yields a <code>double</code> result.
     */
    DoubleCalc compileDouble(Exp exp);

    /**
     * Compiles an expression which yields a tuple result.
     */
    TupleCalc compileTuple(Exp exp);

    /**
     * Compiles an expression to yield a scalar result.
     *
     * <p>If the expression yields a member or tuple, the calculator will
     * automatically apply that member or tuple to the current dimensional
     * context and return the value of the current measure.
     *
     * @param exp Expression
     * @param convert
     * @return Calculation which returns the scalar value of the expression
     */
    Calc compileScalar(Exp exp, boolean convert);

    /**
     * Implements a parameter, returning a unique slot which will hold the
     * parameter's value.
     *
     * @param parameter Parameter
     * @return Slot
     */
    ParameterSlot registerParameter(Parameter parameter);
}

// End ExpCompiler.java
