/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;

import mondrian.olap.type.Type;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;

import java.io.PrintWriter;

/**
 * An <code>Exp</code> is an MDX expression.
 *
 * @author jhyde
 * @since 1.0
 * @version $Id$
 */
public interface Exp {

    Object clone();

    /**
     * Returns the {@link Category} of the expression.
     * @post Category.instance().isValid(return)
     **/
    int getCategory();

    /**
     * Returns the type of this expression. Never null.
     */
    Type getType();

    /**
     * Writes the MDX representation of this expression to a print writer.
     * Sub-expressions are invoked recursively.
     *
     * @param pw PrintWriter
     */
    void unparse(PrintWriter pw);

    /**
     * Validates this expression.
     *
     * The validator acts in the role of 'visitor' (see Gang of Four
     * 'visitor pattern'), and an expression in the role of 'visitee'.
     *
     * @param validator Validator contains validation context
     *
     * @return The validated expression; often but not always the same as
     *   this expression
     */
    Exp accept(Validator validator);

    /**
     * Converts this expression into an a tree of expressions which can be
     * efficiently evaluated.
     *
     * @param compiler
     * @return A compiled expression
     */
    Calc accept(ExpCompiler compiler);

    Object evaluate(Evaluator evaluator);

    Object evaluateScalar(Evaluator evaluator);
}

// End Exp.java
