/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 April, 1999
*/

package mondrian.olap;

import java.io.PrintWriter;

/**
 * <code>FunDef</code> is the definition of an MDX function. See also {@link
 * FunTable}.
 **/
public interface FunDef {
    /**
     * Returns the syntactic type of the function. */
    Syntax getSyntax();

    /**
     * Returns the name of this function.
     **/
    String getName();

    /**
     * Returns the description of this function.
     **/
    String getDescription();

    /**
     * Returns the type of value returned by this function. Values are the same
     * as those returned by {@link Exp#getType}.
     **/
    int getReturnType();

    /**
     * Returns the types of the arguments of this function. Values are the same
     * as those returned by {@link Exp#getType}. The 0<sup>th</sup>
     * argument of methods and properties are the object they are applied
     * to. Infix operators have two arguments, and prefix operators have one
     * argument.
     **/
    int[] getParameterTypes();

    /**
     * Returns the hierarchy of the result of applying this function to
     * <code>args</code>, or null if no dimension is defined. Only applicable
     * to functions which return a {@link Category#Set} or
     * {@link Category#Tuple}.
     *
     * @see Exp#getHierarchy
     **/
    Hierarchy getHierarchy(Exp[] args);

    /**
     * Returns an English description of the signature of the function, for
     * example "&lt;Numeric Expression&gt; / &lt;Numeric Expression&gt;".
     **/
    String getSignature();

    /**
     * Converts a function call into source code.
     **/
    void unparse(Exp[] args, PrintWriter pw);

    /**
     * Applies this function to a set of arguments in the context provided
     * by an evaluator, and returns the result.
     **/
    Object evaluate(Evaluator evaluator, Exp[] args);

    /**
     * Computes how the result of the function depends on members
     * of the dimension. For example, the add operation "+" has two
     * arguments. If one argument depends on Customers and the other
     * depends on Products, the result will depend on both (union of
     * dependencies).
     * <p>
     * For tupel, filter and some others this is not true. They must
     * compute the intersection. TopCount has to omit its Count
     * argument etc.
     */
    boolean dependsOn(Exp[] args, Dimension dimension);
}

// End FunDef.java
