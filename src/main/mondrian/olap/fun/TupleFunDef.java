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
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractTupleCalc;

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
public class TupleFunDef extends FunDefBase {
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

    public int[] getParameterCategories() {
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
            return TypeUtil.toMemberType(args[0].getType());
        } else {
            MemberType[] types = new MemberType[args.length];
            for (int i = 0; i < args.length; i++) {
                Exp arg = args[i];
                types[i] = TypeUtil.toMemberType(arg.getType());
            }
            checkDimensions(types);
            return new TupleType(types);
        }
    }

    public Calc compileCall(FunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        final MemberCalc[] memberCalcs = new MemberCalc[args.length];
        for (int i = 0; i < args.length; i++) {
            memberCalcs[i] = compiler.compileMember(args[i]);
        }
        return new CalcImpl(call, memberCalcs);
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        Member[] members = new Member[args.length];
        for (int i = 0; i < args.length; i++) {
            Member member = getMemberArg(evaluator, args, i, true);
            if (member == null ||
                    member.isNull()) {
                return null;
            }
            members[i] = member;
        }
        checkDimensions(members);
        return members;
    }

    private void checkDimensions(MemberType[] memberTypes) {
        for (int i = 0; i < memberTypes.length; i++) {
            MemberType memberType = memberTypes[i];
            for (int j = 0; j < i; j++) {
                MemberType member1 = memberTypes[j];
                final Dimension dimension = memberType.getDimension();
                final Dimension dimension1 = member1.getDimension();
                if (dimension == dimension1) {
                    throw MondrianResource.instance().DupDimensionsInTuple.ex(
                            dimension.getUniqueName());
                }
            }
        }
    }

    private void checkDimensions(Member[] members) {
        for (int i = 0; i < members.length; i++) {
            Member member = members[i];
            for (int j = 0; j < i; j++) {
                Member member1 = members[j];
                if (member.getDimension() == member1.getDimension()) {
                    throw MondrianResource.instance().DupDimensionsInTuple.ex(
                            member.getDimension().getUniqueName());
                }
            }
        }
    }

    public static class CalcImpl extends AbstractTupleCalc {
        private final MemberCalc[] memberCalcs;

        public CalcImpl(FunCall call, MemberCalc[] memberCalcs) {
            super(call, memberCalcs);
            this.memberCalcs = memberCalcs;
        }

        public Member[] evaluateTuple(Evaluator evaluator) {
            final Member[] members = new Member[memberCalcs.length];
            for (int i = 0; i < members.length; i++) {
                final Member member = members[i]
                        = memberCalcs[i].evaluateMember(evaluator);
                if (member == null || member.isNull()) {
                    return null;
                }
            }
            return members;
        }

        public MemberCalc[] getMemberCalcs() {
            return memberCalcs;
        }
    }
}

// End TupleFunDef.java
