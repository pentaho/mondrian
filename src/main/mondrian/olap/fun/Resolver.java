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
package mondrian.olap.fun;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Syntax;

/**
 * A <code>Resolver</code> converts a function name, invokation type, and set
 * of arguments into a {@link FunDef}.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
interface Resolver
{
	String getName();
    Syntax getSyntax();
	FunDef resolve(Exp[] args, int[] conversionCount);

    /**
     * Returns whether a particular argument must be a scalar expression.
     * Returns <code>false</code> if any of the variants of this resolver
     * allows a set as its <code>k</code>th argument; true otherwise.
     */
    boolean requiresExpression(int k);
}


// End Resolver.java
