/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Jun 14, 2002
*/

package mondrian.olap.fun;

import mondrian.olap.*;

import java.io.PrintWriter;

/**
 * A <code>ValueFunDef</code> is a pseudo-function to evaluate a member or
 * a tuple. Similar to {@link TupleFunDef}.
 *
 * @author jhyde
 * @since Jun 14, 2002
 * @version $Id$
 **/
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
    public int getReturnType() {
        return Category.Tuple;
    }
    public int[] getParameterTypes() {
        return argTypes;
    }
    public void unparse(Exp[] args, PrintWriter pw) {
        ExpBase.unparseList(pw, args, "(", ", ", ")");
    }
    public Hierarchy getHierarchy(Exp[] args) {
        return null;
    }
    public Object evaluate(Evaluator evaluator, Exp[] args) {
        Member[] members = new Member[args.length];
        for (int i = 0; i < args.length; i++) {
            members[i] = getMemberArg(evaluator, args, i, true);
        }
        Evaluator evaluator2 = evaluator.push(members);
        return evaluator2.evaluateCurrent();
    }
}

// End ValueFunDef.java
