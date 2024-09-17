/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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
