/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho
// All Rights Reserved.
*/

package mondrian.spi;

import mondrian.olap.Evaluator;
import mondrian.olap.Syntax;
import mondrian.olap.type.Type;

import java.util.List;

/**
 * Definition of a user-defined function.
 *
 * <p>The class must have a public, zero-arguments constructor, be on
 * Mondrian's runtime class-path, and be referenced from the schema file:
 *
 * <blockquote><code>
 * &lt;Schema&gt;<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;....<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&lt;UserDefinedFunction name="MyFun"
 * class="com.acme.MyFun"&gt;<br/>
 * &lt;/Schema&gt;</code></blockquote>
 *
 * @author jhyde
  */
public interface UserDefinedFunction {
    /**
     * Returns the name with which the user-defined function will be used
     * from within MDX expressions.
     */
    public String getName();

    /**
     * Returns a description of the user-defined function.
     */
    public String getDescription();

    /**
     * Returns the syntactic type of the user-defined function.
     * Usually {@link Syntax#Function}.
     */
    public Syntax getSyntax();

    /**
     * Returns an array of the types of the parameters of this function.
     */
    public Type[] getParameterTypes();

    /**
     * Returns the return-type of this function.
     *
     * @param parameterTypes Parameter types
     * @return Return type
     */
    public Type getReturnType(Type[] parameterTypes);

    /**
     * Applies this function to a set of arguments, and returns a result.
     *
     * @param evaluator Evaluator containts the runtime context, in particular
     *   the current member of each dimension.
     * @param arguments Expressions which yield the arguments of this function.
     *   Most user-defined functions will evaluate all arguments before using
     *   them. Functions such as <code>IIf</code> do not evaluate all
     *   arguments; this technique is called <dfn>lazy evaluation</dfn>.
     * @return The result value.
     */
    public Object execute(Evaluator evaluator, Argument[] arguments);

    /**
     * Returns a list of reserved words used by this function.
     * May return an empty array or null if this function does not require
     * any reserved words.
     */
    public String[] getReservedWords();

    interface Argument {
        /**
         * Returns the type of the argument.
         *
         * @return Argument type
         */
        Type getType();

        /**
         * Evaluates the argument as a scalar expression.
         *
         * <p>The effect is the same as
         * {@link #evaluate(mondrian.olap.Evaluator)} except if the argument
         * evaluates to a member or tuple. This method will set the context
         * to the member or tuple and evaluate the current measure, whereas
         * {@code evaluate} would return the member or tuple.
         *
         * <p>The effect is similar to creating a calculated member in an MDX
         * query:</p>
         *
         * <blockquote>WITH MEMBER [Measures].[Previous Period] AS<br/>
         * &nbsp;&nbsp;([Measures].[Unit Sales], [Time].[Time].PrevMember)<br/>
         * SELECT {[Measures].[Unit Sales],<br/>
         * &nbsp;&nbsp;&nbsp;&nbsp;[Measures].[Previous Period]} on 0,<br/>
         * &nbsp;&nbsp;[Time].[Time].Children on 1<br/>
         * FROM [Sales]</blockquote>
         *
         * <p>Note how {@code [Measures].[Previous Period]} is defined as a
         * tuple, but evaluates to a number.</p>
         *
         * @param evaluator Evaluation context
         * @return Scalar expression at the given member or tuple
         */
        Object evaluateScalar(Evaluator evaluator);

        /**
         * Evaluates the argument.
         *
         * <p>If the argument is a set of members or tuples, this method may
         * return either a {@link List} or an {@link Iterable}. It is not safe
         * to blindly cast to {@code List}. For guaranteed type, call
         * {@link #evaluateList(mondrian.olap.Evaluator)} or
         * {@link #evaluateIterable(mondrian.olap.Evaluator)}.
         *
         * @param evaluator Evaluation context
         * @return Result of evaluating the argument
         */
        Object evaluate(Evaluator evaluator);

        /**
         * Evaluates the argument to a list of members or tuples.
         *
         * @param eval Evaluation context
         * @return List of members or tuples.
         */
        List evaluateList(Evaluator eval);

        /**
         * Evaluates the argument to an iterable over members or tuples.
         *
         * @param eval Evaluation context
         * @return Iterable over members or tuples.
         */
        Iterable evaluateIterable(Evaluator eval);
    }
}

// End UserDefinedFunction.java
