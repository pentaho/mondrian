/*
// $Id$
// (C) Copyright 2002-2005 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 February, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;

/**
 * A <code>Funk</code> is a code-fragment: it can be evaluated, given an
 * environment and a list of arguments. It is like a
 * {@link mondrian.olap.FunDef}, but not so self-aware.
 *
 * @author jhyde
 * @since 28 February, 2002
 * @version $Id$
 **/
interface Funk {
    /**
     * Evaluates this function with the given set of expressions. The
     * implementation will often evaluate the expressions first. For example,
     * <code>TopCount([Promotion Media].members, 2 + 3, [Measures].[Unit
     * Sales])</code> would evaluate the first and second arguments, but not
     * the third.
     **/
    Object evaluate(Evaluator evaluator, Exp[] args);
}

// End Funk.java
