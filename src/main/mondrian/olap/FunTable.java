/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
 */
package mondrian.olap;
import java.util.*;

/**
 * List of all MDX functions.
 *
 * A function table can resolve a function call, using a particular
 * {@link Syntax} and set of arguments, to a
 * function definition ({@link FunDef}).
 */
public interface FunTable {
    /**
     * Resolves a function call to a particular function. If the function is
     * overloaded, returns as precise a match to the argument types as
     * possible.
     **/
    FunDef getDef(FunCall call, Validator resolver);

    /**
     * Adds a casting function, if necessary, to ensure that an expression is
     * of a given type. Throws an error if conversion is not possible.
     */
    Exp convert(Exp fromExp, int to, Validator resolver);

    /**
     * Returns whether a string is a reserved word.
     */
    boolean isReserved(String s);

    /**
     * Returns whether a string is a property-style (postfix)
     * operator. This is used during parsing to disambiguate
     * functions from unquoted member names.
     */
    boolean isProperty(String s);

    /**
     * Returns whether the <code>k</code>th argument to a function call
     * has to be an expression.
     */
    boolean requiresExpression(
            FunCall funCall,
            int k,
            Validator resolver);

    /**
     * Returns a list of words ({@link String}) which may not be used as
     * identifiers.
     */
    List getReservedWords();

    /**
     * Returns a list of {@link mondrian.olap.fun.Resolver} objects.
     */
    List getResolvers();

    /**
     * Returns a list of {@link mondrian.olap.fun.FunInfo} objects.
     */
    List getFunInfoList();
}

// End FunTable.java
