/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TupleType;

import java.io.PrintWriter;

/**
 * <code>TupleFunDef</code> implements the '( ... )' operator which builds
 * tuples, as in <code>([Time].CurrentMember,
 * [Stores].[USA].[California])</code>.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class TupleFunDef extends FunDefBase {
    private final int[] argTypes;

    TupleFunDef(int[] argTypes) {
        super(
            "()",
            "(<Member> [, <Member>]...)",
            "Parenthesis operator constructs a tuple.  If there is only one member, the expression is equivalent to the member expression.",
            Syntax.Parentheses,
            Category.Tuple,
            argTypes);
        this.argTypes = argTypes;
    }
    public int getReturnCategory() {
        return Category.Tuple;
    }
    public int[] getParameterTypes() {
        return argTypes;
    }
    public void unparse(Exp[] args, PrintWriter pw) {
        ExpBase.unparseList(pw, args, "(", ", ", ")");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        // _Tuple(<Member1>[,<MemberI>]...), which is written
        // (<Member1>[,<MemberI>]...), has type [Hie1] x ... x [HieN].
        //
        // If there is only one member, it merely represents a parenthesized
        // expression, whose Hierarchy is that of the member.
        if (args.length == 1) {
            return args[0].getTypeX();
        } else {
            Type[] types = new Type[args.length];
            for (int i = 0; i < args.length; i++) {
                Exp arg = args[i];
                types[i] = arg.getTypeX();
            }
            return new TupleType(types);
        }
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        Member[] members = new Member[args.length];
        for (int i = 0; i < args.length; i++) {
            members[i] = getMemberArg(evaluator, args, i, true);
        }
        return members;
    }
    public boolean dependsOn(Exp[] args, Dimension dimension) {
        return dependsOnIntersection(args, dimension);
    }
}

// End TupleFunDef.java
