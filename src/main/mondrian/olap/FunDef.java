/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 April, 1999
*/

package mondrian.olap;
import mondrian.test.Testable;

import java.io.PrintWriter;

/**
 * <code>FunDef</code> is the definition of an MDX function. See also {@link
 * FunTable}.
 **/
public interface FunDef extends Testable {
	/**
	 * Returns the syntactic type of the function. One of the following:<dl>
	 * <dt>{@link #TypeProperty}</dt><dd>invoked
	 *     <code>object.PROPERTY</code>;</dd>
	 * <dt>{@link #TypeMethod}</dt><dd>invoked <code>object.METHOD()</code> or
	 *     <code>object.METHOD(args)</code>;</dd>
	 * <dt>{@link #TypeFunction}</dt><dd>invoked <code>FUNCTION()</code> or
	 *     <code>FUNCTION(args)</code>;</dd>
	 * <dt>{@link #TypeInfix}</dt><dd>invoked <code>arg OPERATOR
	 *     arg</code> (like '+' or 'AND');</dd>
	 * <dt>{@link #TypePrefix}</dt><dd>invoked <code>OPERATOR arg</code> (like
	 *     unary '-');</dd>
	 * <dt>{@link #TypeBraces}</dt><dd>invoked <code>{ARG,...}</code>, that is,
	 *     the set construction operator;</dd>
	 * <dt>{@link #TypeParentheses}</dt><dd>invoked <code>(ARG)</code> or
	 *      <code>(ARG,...)</code>; that is, parentheses for grouping
	 *     expressions, and the tuple construction operator.</dd>
	 * <dt>{@link #TypeCase}</dt><dd>invoked <code>CASE ... END</code>.</dd>
	 * </dl>
	 **/
	int getSyntacticType();

	/** @see #getSyntacticType **/
	int TypeFunction = 0;
	/** @see #getSyntacticType **/
	int TypeProperty = 1;
	/** @see #getSyntacticType **/
	int TypeMethod = 2;
	/** @see #getSyntacticType **/
	int TypeInfix = 3;
	/** @see #getSyntacticType **/
	int TypePrefix = 4;
	/** @see #getSyntacticType **/
	int TypeBraces = 5;
	/** @see #getSyntacticType **/
	int TypeParentheses = 6;
	/** @see #getSyntacticType **/
	int TypeCase = 7;

	int TypeMask = 0xFF;
	int TypePropertyQuoted = TypeProperty | 0x100;
	int TypePropertyAmpQuoted = TypeProperty | 0x200;

	/** Returns <code>true</code> if this function is invoked using the syntax
	 * 'FUNCTION(args)' or 'FUNCTION()' **/
	boolean isFunction();

	/** Returns <code>true</code> if this function is invoked using the syntax
	 * 'object.METHOD(args)' or 'object.METHOD()' **/
	boolean isMethod();

	/** Returns <code>true</code> if this function is invoked using the syntax
	 * 'object.PROPERTY' **/
	boolean isProperty();

	/** Returns <code>true</code> if this function is an infix operator, such
	 * '+'. **/
	boolean isInfix();

	/** Returns <code>true</code> if this function is a prefix operator, such
	 * as unary '-'. **/
	boolean isPrefix();

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
     * to functions which return a {@link Exp#CatSet} or {@link Exp#CatTuple}.
	 *
	 * @see Exp#getHierarchy
	 **/
	Hierarchy getHierarchy(Exp[] args);

	/**
	 * Returns an English description of the signature of the function, for
	 * example "<Numeric Expression> / <Numeric Expression>".
	 **/
	String getSignature();

	/**
	 * Converts a function call into source code.
	 **/
	void unparse(Exp[] args, PrintWriter pw, ElementCallback callback);

	/**
	 * Applies this function to a set of arguments in the context provided
	 * by an evaluator, and returns the result.
	 **/
	Object evaluate(Evaluator evaluator, Exp[] args);
}

// End FunDef.java
