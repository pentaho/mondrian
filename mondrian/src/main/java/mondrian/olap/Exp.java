/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2006 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.MdxVisitor;
import mondrian.olap.type.Type;

import java.io.PrintWriter;

/**
 * An <code>Exp</code> is an MDX expression.
 *
 * @author jhyde, 20 January, 1999
 * @since 1.0
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
