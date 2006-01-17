/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2006 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * A <code>FunCall</code> is a function applied to a list of operands.
 *
 * <p>The parser creates function calls as an
 * {@link mondrian.mdx.UnresolvedFunCall unresolved  function call}.
 * The validator converts it to a
 * {@link  mondrian.mdx.ResolvedFunCall resolved function call},
 * which has a {@link FunDef function definition} and extra type information.
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 6, 2006
 */
public interface FunCall extends Exp {
    /**
     * Returns the <code>index</code><sup>th</sup> argument to this function
     * call.
     *
     * @param index Ordinal of the argument
     * @return <code>index</code><sup>th</sup> argument to this function call
     */
    Exp getArg(int index);

    /**
     * Returns the arguments to this function.
     *
     * @return array of arguments
     */
    Exp[] getArgs();

    /**
     * Returns the number of arguments to this function.
     *
     * @return number of arguments
     */
    int getArgCount();

    /**
     * Returns the name of the function.
     */
    String getFunName();

    /**
     * Returns the syntax of the call.
     */
    Syntax getSyntax();
}

// End FunCall.java
