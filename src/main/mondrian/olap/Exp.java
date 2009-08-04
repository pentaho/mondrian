/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 1999-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;

import mondrian.olap.type.Type;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MdxVisitor;

import java.io.PrintWriter;

/**
 * An <code>Exp</code> is an MDX expression.
 *
 * @author jhyde
 * @since 1.0
 * @version $Id$
 */
public interface Exp {

    Exp clone();

    /**
     * Returns the {@link Category} of the expression.
     *
     * @post Category.instance().isValid(return)
     */
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

    /**
     * Accepts a visitor to this Exp.
     * The implementation should generally dispatches to the
     * {@link MdxVisitor#visit} method appropriate to the type of expression.
     *
     * @param visitor Visitor
     */
    Object accept(MdxVisitor visitor);
}

// End Exp.java
