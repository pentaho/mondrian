/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.test.Testable;

/**
 * A <code>Resolver</code> converts a function name, invokation type, and set
 * of arguments into a {@link FunDef}.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
interface Resolver extends Testable
{
	String getName();
	FunDef resolve(int syntacticType, Exp[] args, int[] conversionCount);
}


// End Resolver.java
