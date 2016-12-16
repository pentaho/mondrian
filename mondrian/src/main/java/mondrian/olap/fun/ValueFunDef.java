/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2006 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;

import java.io.PrintWriter;

/**
 * A <code>ValueFunDef</code> is a pseudo-function to evaluate a member or
 * a tuple. Similar to {@link TupleFunDef}.
 *
 * @author jhyde
 * @since Jun 14, 2002
 */
class ValueFunDef extends FunDefBase {
    private final int[] argTypes;

    ValueFunDef(int[] argTypes) {
        super(
            "_Value()",
            "_Value([<Member>, ...])",
            "Pseudo-function which evaluates a tuple.",
            Syntax.Parentheses,
            Category.Numeric,
            argTypes);
        this.argTypes = argTypes;
    }

    public int getReturnCategory() {
        return Category.Tuple;
    }

    public int[] getParameterCategories() {
        return argTypes;
    }

    public void unparse(Exp[] args, PrintWriter pw) {
        ExpBase.unparseList(pw, args, "(", ", ", ")");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        return null;
    }

}

// End ValueFunDef.java
