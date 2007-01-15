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

    /**
     * Compiles an expression.
     *
     * @param exp Expression
     * @return Compiled expression
     */
    Calc compile(Exp exp);

    /**
     * Compiles an expression to a given result type.
     *
     * @param exp Expression
     * @param preferredResultTypes List of result types, in descending order
     *   of preference. Never null.
     * @return Compiled expression, or null if none can satisfy
     */
    Calc compile(Exp exp, ResultStyle[] preferredResultTypes);

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
     * Compiles an expression which yields an immutable {@link java.util.List}
     * result.
     *
     * <p>Always equivalent to <code>{@link #compileList}(exp, false)</code>.
     */
    ListCalc compileList(Exp exp);

    /**
     * Compiles an expression which yields {@link java.util.List} result.
     *
     * <p>Such an expression is generally a list of {@link Member} objects or a
     * list of tuples (each represented by a {@link Member} array).
     *
     * <p>See {@link #compileList(mondrian.olap.Exp)}.
     *
     * @param exp Expression
     * @param mutable Whether resulting list is mutable
     */
    ListCalc compileList(Exp exp, boolean mutable);

    /**
     * Compiles an expression which yields an immutable {@
     * link * java.lang.Iterable} * result.
     */
    IterCalc compileIter(Exp exp);

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

    /**
     * Returns a list of the {@link mondrian.calc.ExpCompiler.ResultStyle}s
     * acceptable to the caller.
     */
    ResultStyle[] getAcceptableResultStyles();

    /**
     * Enumeration of ways that a compiled expression can return its result to
     * its caller.
     *
     * <p>In future, we may have an "ITERABLE" result style, which allows us
     * to handle large lists without holding them in memory.
     */
    enum ResultStyle {
        /**
         * Indicates that caller will accept any applicable style.
         */
        ANY,

        /**
         * Indicates that the expression returns its result as a list which may
         * safely be modified by the caller.
         */
        MUTABLE_LIST,

        /**
         * Indicates that the expression returns its result as a list which must
         * not be modified by the caller.
         */
        LIST,

        /**
         * Indicates that the expression returns its result as an Iterable
         * which must not be modified by the caller.
         */
        ITERABLE,

        /**
         * Indicates that the expression results its result as an immutable
         * value. This is typical for expressions which return string and
         * numeric values.
         */
        VALUE
    }

    ResultStyle[] ANY_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.ANY 
        };
    ResultStyle[] ITERABLE_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.ITERABLE 
        };
    ResultStyle[] MUTABLE_LIST_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.MUTABLE_LIST 
        };
    ResultStyle[] LIST_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.LIST 
        };

    ResultStyle[] ITERABLE_ANY_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.ITERABLE,
            ResultStyle.ANY
        };
    ResultStyle[] ITERABLE_LIST_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.ITERABLE,
            ResultStyle.LIST
        };
    ResultStyle[] ITERABLE_MUTABLE_LIST_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.ITERABLE,
            ResultStyle.MUTABLE_LIST
        };
    ResultStyle[] ITERABLE_LIST_MUTABLE_LIST_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.ITERABLE,
            ResultStyle.LIST,
            ResultStyle.MUTABLE_LIST
        };
    ResultStyle[] LIST_MUTABLE_LIST_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.LIST,
            ResultStyle.MUTABLE_LIST
        };
    ResultStyle[] MUTABLE_LIST_LIST_RESULT_STYLE_ARRAY = 
        new ResultStyle[] { 
            ResultStyle.MUTABLE_LIST,
            ResultStyle.LIST
        };
}

// End ExpCompiler.java
