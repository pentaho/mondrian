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
package mondrian.olap.fun;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Syntax;

/**
 * A <code>Resolver</code> converts a function name, invocation type, and set
 * of arguments into a {@link FunDef}.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
interface Resolver {
    /**
     * Returns the name of the function or operator.
     */
    String getName();

    /**         
     * Returns the description of the function or operator.
     */             
    String getDescription();

    /**
     * Returns the syntax with which the function or operator was invoked.
     */
    Syntax getSyntax();

    /**
     * Given a particular set of arguments the function is applied to, returns
     * the correct overloaded form of the function.
     *
     * <p>The method must increment <code>conversionCount</code> argument every
     * time it performs an implicit type-conversion. If there are several
     * candidate functions with the same signature, the validator will choose
     * the one which used the fewest implicit conversions.
     *
     * @param args Expressions which this function call is applied to.
     *
     * @param conversionCount This argument must be an  <code>int</code> array
     *   with a single element; in effect, it is an in/out parameter. It
     *   The method increments the count every time it performs a conversion.
     *
     * @return
     */
    FunDef resolve(Exp[] args, int[] conversionCount);

    /**
     * Returns whether a particular argument must be a scalar expression.
     * Returns <code>false</code> if any of the variants of this resolver
     * allows a set as its <code>k</code>th argument; true otherwise.
     */
    boolean requiresExpression(int k);

    /**
     * Returns an array of symbolic constants which can appear as arguments
     * to this function.
     *
     * <p>For example, the <code>DrilldownMember</code> may take the symbol
     * <code>RECURSIVE</code> as an argument. Most functions do not define
     * any symbolic constants.
     *
     * @return An array of the names of the symbolic constants
     */
    String[] getReservedWords();
}


// End Resolver.java
