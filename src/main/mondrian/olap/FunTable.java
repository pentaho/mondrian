/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
 */
package mondrian.olap;
import mondrian.olap.fun.BuiltinFunTable;

import java.util.ArrayList;

/**
 * A <code>FunTable</code> resolves a function call, using a particular syntax
 * (see {@link FunDef#getSyntax}) and set of arguments to a function
 * definition (<code>class {@link FunDef}</code>).
 *
 * <p> It is a singleton class. The default implementation is {@link
 * mondrian.olap.fun.BuiltinFunTable}.
 **/
public abstract class FunTable {
	/** the singleton **/
	private static FunTable instance;
	/** used during initialization **/
	protected ArrayList resolvers;

	/** Returns (creating if necessary) the singleton. **/
	public static FunTable instance() {
		if (instance == null) {
			instance = new BuiltinFunTable();
		}
		return instance;
	}

	/**
	 * Resolves a function call to a particular function. If the function is
	 * overloaded, returns as precise a match to the argument types as
	 * possible.
	 **/
	public abstract FunDef getDef(FunCall call, Exp.Resolver resolver);

	/**
	 * Adds a casting function, if necessary, to ensure that an expression is
	 * of a given type. Throws an error if conversion is not possible.
	 */
	public abstract Exp convert(Exp fromExp, int to, Exp.Resolver resolver);

	/**
	 * This method is called from the constructor, to define the set of
	 * functions recognized. Derived class can override this method to add more
	 * functions. Each function is declared by calling {@link #define}.
	 **/
	protected void defineFunctions() {
	}

	protected abstract void define(FunDef funDef);

	/**
	 * Returns whether a string is a reserved word.
	 */
	public abstract boolean isReserved(String s);

    /**
     * Returns whether the <code>k</code>th argument to a function call
     * has to be an expression.
     */ 
    public abstract boolean requiresExpression(FunCall funCall, int k,
            Exp.Resolver resolver);
}

// End FunTable.java
