/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractTupleCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;

import java.io.PrintWriter;
import java.util.List;

/**
 * <code>TupleFunDef</code> implements the '(...)' operator which builds
 * tuples, as in <code>([Time].CurrentMember,
 * [Stores].[USA].[California])</code>.
 *
 * @author jhyde
 * @since 3 March, 2002
 */
public class TupleFunDef extends FunDefBase {
    private final int[] argTypes;
    static final ResolverImpl Resolver = new ResolverImpl();

    private TupleFunDef(int[] argTypes) {
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
            TupleType.checkHierarchies(types);
            return new TupleType(types);
        }
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        final MemberCalc[] memberCalcs = new MemberCalc[args.length];
        for (int i = 0; i < args.length; i++) {
            memberCalcs[i] = compiler.compileMember(args[i]);
        }
        return new CalcImpl(call, memberCalcs);
    }

    public static class CalcImpl extends AbstractTupleCalc {
        private final MemberCalc[] memberCalcs;

        public CalcImpl(ResolvedFunCall call, MemberCalc[] memberCalcs) {
            super(call, memberCalcs);
            this.memberCalcs = memberCalcs;
        }

        public Member[] evaluateTuple(Evaluator evaluator) {
            final Member[] members = new Member[memberCalcs.length];
            for (int i = 0; i < members.length; i++) {
                final Member member =
                    members[i] =
                    memberCalcs[i].evaluateMember(evaluator);
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

    private static class ResolverImpl extends ResolverBase {
        public ResolverImpl() {
            super("()", null, null, Syntax.Parentheses);
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            // Compare with TupleFunDef.getReturnCategory().  For example,
            //   ([Gender].members) is a set,
            //   ([Gender].[M]) is a member,
            //   (1 + 2) is a numeric,
            // but
            //   ([Gender].[M], [Marital Status].[S]) is a tuple.
            if (args.length == 1) {
                return new ParenthesesFunDef(args[0].getCategory());
            } else {
                final int[] argTypes = new int[args.length];
                for (int i = 0; i < args.length; i++) {
                    // Arg must be a member:
                    //  OK: ([Gender].[S], [Time].[1997])   (member, member)
                    //  OK: ([Gender], [Time])           (dimension, dimension)
                    // Not OK:
                    //  ([Gender].[S], [Store].[Store City]) (member, level)
                    if (!validator.canConvert(
                            i, args[i], Category.Member, conversions))
                    {
                        return null;
                    }
                    argTypes[i] = Category.Member;
                }
                return new TupleFunDef(argTypes);
            }
        }
    }
}

// End TupleFunDef.java
