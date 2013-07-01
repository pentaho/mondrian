/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.calc;

import mondrian.calc.impl.BetterExpCompiler;
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.util.CreationException;
import mondrian.util.ObjectFactory;

import org.eigenbase.util.property.StringProperty;

import java.util.List;

/**
 * Mediates the compilation of an expression ({@link mondrian.olap.Exp})
 * into a compiled expression ({@link Calc}).
 *
 * @author jhyde
 * @since Sep 28, 2005
 */
public interface ExpCompiler {

    /**
     * Returns the evaluator to be used for evaluating expressions during the
     * compilation process.
     */
    Evaluator getEvaluator();

    /**
     * Returns the validator which was used to validate this expression.
     *
     * @return validator
     */
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
     * <p>If <code>resultType</code> is not null, casts the expression to that
     * type. Throws an exception if that conversion is not allowed by the
     * type system.
     *
     * <p>The <code>preferredResultStyles</code> parameter specifies a list
     * of desired result styles. It must not be null, but may be empty.
     *
     * @param exp Expression
     *
     * @param resultType Desired result type, or null to use expression's
     *                   current type
     *
     * @param preferredResultStyles List of result types, in descending order
     *                   of preference. Never null.
     *
     * @return Compiled expression, or null if none can satisfy
     */
    Calc compileAs(
        Exp exp,
        Type resultType,
        List<ResultStyle> preferredResultStyles);

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
     * Compiles an expression which yields a {@link java.util.Date} result.
     * The expression is implicitly converted into a scalar.
     */
    DateTimeCalc compileDateTime(Exp exp);

    /**
     * Compiles an expression which yields an immutable {@link TupleList}
     * result.
     *
     * <p>Always equivalent to <code>{@link #compileList}(exp, false)</code>.
     */
    ListCalc compileList(Exp exp);

    /**
     * Compiles an expression which yields {@link TupleList} result.
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
     * Compiles an expression which yields an immutable {@link Iterable} result.
     *
     * @param exp Expression
     * @return Calculator which yields an Iterable
     */
    IterCalc compileIter(Exp exp);

    /**
     * Compiles an expression which yields a <code>boolean</code> result.
     *
     * @param exp Expression
     * @return Calculator which yields a boolean
     */
    BooleanCalc compileBoolean(Exp exp);

    /**
     * Compiles an expression which yields a <code>double</code> result.
     *
     * @param exp Expression
     * @return Calculator which yields a double
     */
    DoubleCalc compileDouble(Exp exp);

    /**
     * Compiles an expression which yields a tuple result.
     *
     * @param exp Expression
     * @return Calculator which yields a tuple
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
     * @param specific Whether to try to use the specific compile method for
     *   scalar types. For example, if <code>specific</code> is true and
     *   <code>exp</code> is a string expression, calls
     *   {@link #compileString(mondrian.olap.Exp)}
     * @return Calculation which returns the scalar value of the expression
     */
    Calc compileScalar(Exp exp, boolean specific);

    /**
     * Implements a parameter, returning a unique slot which will hold the
     * parameter's value.
     *
     * @param parameter Parameter
     * @return Slot
     */
    ParameterSlot registerParameter(Parameter parameter);

    /**
     * Returns a list of the {@link ResultStyle}s
     * acceptable to the caller.
     */
    List<ResultStyle> getAcceptableResultStyles();

    /**
     * The <code>ExpCompiler.Factory</code> is used to access
     * <code>ExpCompiler</code> implementations. Each call returns
     * a new instance. This factory supports overriding the default
     * instance by use of a <code>ThreadLocal</code> and by defining a
     * <code>System</code> property with the <code>ExpCompiler</code>
     * class name.
     */
    public static final class Factory extends ObjectFactory<ExpCompiler> {
        private static final Factory factory;
        private static final Class[] CLASS_ARRAY;
        static {
            factory = new Factory();
            CLASS_ARRAY = new Class[] {
                Evaluator.class,
                Validator.class,
                ResultStyle[].class,
            };
        }

        /**
         * Create a <code>ExpCompiler</code> instance, each call returns a
         * new compiler.
         *
         * @param evaluator the <code>Evaluator</code> to use with the compiler
         * @param validator the <code>Validator</code> to use with the compiler
         * @return the new <code>ExpCompiler</code> compiler
         * @throws CreationException if the compiler can not be created
         */
        public static ExpCompiler getExpCompiler(
            final Evaluator evaluator,
            final Validator validator)
            throws CreationException
        {
            return getExpCompiler(evaluator, validator, ResultStyle.ANY_LIST);
        }

        /**
         *
         *
         * @param evaluator the <code>Evaluator</code> to use with the compiler
         * @param validator the <code>Validator</code> to use with the compiler
         * @param resultStyles the initial <code>ResultStyle</code> array
         * for the compiler
         * @return the new <code>ExpCompiler</code> compiler
         * @throws CreationException if the compiler can not be created
         */
        public static ExpCompiler getExpCompiler(
            final Evaluator evaluator,
            final Validator validator,
            final List<ResultStyle> resultStyles)
                throws CreationException
        {
            return factory.getObject(
                CLASS_ARRAY,
                new Object[] {
                    evaluator,
                    validator,
                    resultStyles
                });
        }

        /**
         * <code>ThreadLocal</code> used to hold the class name of an
         * <code>ExpCompiler</code> implementation.
         * Generally, this should only be used for testing.
         */
        private static final ThreadLocal<String> ClassName =
            new ThreadLocal<String>();

        /**
         * Get the class name of a <code>ExpCompiler</code> implementation
         * or null.
         *
         * @return the class name or null.
        */
        public static String getThreadLocalClassName() {
            return ClassName.get();
        }
        /**
         * Sets the class name of a  <code>ExpCompiler</code> implementation.
         * This should be called (obviously) before calling the
         * <code>ExpCompiler.Factory</code> <code>getExpCompiler</code>
         * method to get the <code>ExpCompiler</code> implementation.
         * Generally, this is only used for testing.
         *
         * @param className Class name
         */
        public static void setThreadLocalClassName(String className) {
            ClassName.set(className);
        }
        /**
         * Clears the class name (regardless of whether a class name was set).
         * When a class name is set using <code>setThreadLocalClassName</code>,
         * the setting whould be done in a try-block and a call to this
         * clear method should be in the finally-clause of that try-block.
         */
        public static void clearThreadLocalClassName() {
            ClassName.set(null);
        }

        /**
         * The constructor for the <code>ExpCompiler.Factory</code>.
         * This passes the <code>ExpCompiler</code> class to the
         * <code>ObjectFactory</code> base class.
        */
        private Factory() {
            super(ExpCompiler.class);
        }
        /**
         * Get the class name set in the <code>ThreadLocal</code> or null.
         *
         * @return class name or null.
         */
        protected String getClassName() {
            return getThreadLocalClassName();
        }

        /**
         * Return the <code>ExpCompiler.Factory</code property name.
         *
         * @return <code>ExpCompiler.Factory</code> property name
         */
        protected StringProperty getStringProperty() {
            return MondrianProperties.instance().ExpCompilerClass;
        }
        /**
         * The <code>ExpCompiler.Factory</code>'s implementation of the
         * <code>ObjectFactory</code>'s abstract method which returns
         * the default <code>ExpCompiler</code> instance.
         *
         * @param parameterTypes array of classes: Evaluator, Validator and
         *  ResultStyle
         * @param parameterValues  the Evaluator, Validator and ResultStyle
         * values
         * @return <code>ExpCompiler</code> instance
         * @throws CreationException if the <code>ExpCompiler</code> can not be
         * created.
         */
        protected ExpCompiler getDefault(
            final Class[] parameterTypes,
            final Object[] parameterValues)
            throws CreationException
        {
            // Strong typed above so don't need to check here
            Evaluator evaluator = (Evaluator) parameterValues[0];
            Validator validator = (Validator) parameterValues[1];
            List<ResultStyle> resultStyles =
                (List<ResultStyle>) parameterValues[2];

            // Here there is bleed-through from the "calc.impl" implementation
            // directory into the "calc" interface definition directory.
            // This can be avoided if we were to use reflection to
            // create this the default ExpCompiler implementation.
            return new BetterExpCompiler(
                evaluator, validator, resultStyles);
        }

        /**
         * Get the underlying Factory object.
         * <p>
         * This is for testing only.
         *
         * @return the <code>ExpCompiler.Factory</code> object
         */
        public static Factory getFactory() {
            return factory;
        }

        /**
         * Get the current override contect.
         * <p>
         * This is for testing only.
         *
         * @return the override context object.
         */
        public Object removeContext() {
            return new Context();
        }

        /**
         * Restore the current overrides.
         * <p>
         * This is for testing only.
         *
         * @param context the current override object.
         */
        public void restoreContext(final Object context) {
            if (context instanceof Context) {
                ((Context) context).restore();
            }
        }

        /**
         * The <code>ExpCompiler</code> only has two override mechanisms: the
         * <code>ThreadLocal</code> and <code>System</code>
         * <code>Properties</code>. This class captures and clears the current
         * values for both in the constructor and then replaces them
         * in the <code>restore</code> method.
         * <p>
         * This is for testing only.
         */
        public static class Context implements ObjectFactory.Context {
            private final String threadLocalClassName;
            private final String systemPropertyClassName;

            /**
             * Creates a Context.
             */
            Context() {
                this.threadLocalClassName =
                        ExpCompiler.Factory.getThreadLocalClassName();
                if (this.threadLocalClassName != null) {
                    ExpCompiler.Factory.clearThreadLocalClassName();
                }

                this.systemPropertyClassName =
                        System.getProperty(ExpCompiler.class.getName());
                if (this.systemPropertyClassName != null) {
                    System.getProperties().remove(ExpCompiler.class.getName());
                }
            }

            /**
             * Restores the previous context.
             */
            private void restore() {
                if (this.threadLocalClassName != null) {
                    ExpCompiler.Factory.setThreadLocalClassName(
                        this.threadLocalClassName);
                }
                if (this.systemPropertyClassName != null) {
                    System.setProperty(
                        ExpCompiler.class.getName(),
                        this.systemPropertyClassName);
                }
            }
        }
    }
}

// End ExpCompiler.java
