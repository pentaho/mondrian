/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.ResolvedFunCall;

import java.io.PrintWriter;

/**
 * <code>ParenthesesFunDef</code> implements the parentheses operator as if it
 * were a function.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 */
class ParenthesesFunDef extends FunDefBase {
    private final int argType;
    ParenthesesFunDef(int argType) {
        super(
            "()",
            "(<Expression>)",
            "Parenthesis enclose an expression and indicate precedence.",
            Syntax.Parentheses,
            argType,
            new int[] {argType});
        this.argType = argType;
    }
    public void unparse(Exp[] args, PrintWriter pw) {
        if (args.length != 1) {
            ExpBase.unparseList(pw, args, "(", ",", ")");
        } else {
            // Don't use parentheses unless necessary. We add parentheses around
            // expressions because we're not sure of operator precedence, so if
            // we're not careful, the parentheses tend to multiply ad infinitum.
            args[0].unparse(pw);
        }
    }

    public Type getResultType(Validator validator, Exp[] args) {
        Util.assertTrue(args.length == 1);
        return args[0].getType();
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        return compiler.compile(call.getArg(0));
    }
}

// End ParenthesesFunDef.java
