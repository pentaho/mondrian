/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
