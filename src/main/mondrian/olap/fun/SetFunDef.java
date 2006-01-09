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

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.AbstractVoidCalc;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.mdx.ResolvedFunCall;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * <code>SetFunDef</code> implements the 'set' function (whose syntax is the
 * brace operator, <code>{ ... }</code>).
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class SetFunDef extends FunDefBase {
    SetFunDef(Resolver resolver, int[] argTypes) {
        super(resolver, Category.Set, argTypes);
    }

    public void unparse(Exp[] args, PrintWriter pw) {
        ExpBase.unparseList(pw, args, "{", ", ", "}");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        // All of the members in {<Member1>[,<MemberI>]...} must have the same
        // Hierarchy.  But if there are no members, we can't derive a
        // hierarchy.
        Type type0 = null;
        if (args.length == 0) {
            // No members to go on, so we can't guess the hierarchy.
            type0 = MemberType.Unknown;
        } else {
            for (int i = 0; i < args.length; i++) {
                Exp arg = args[i];
                Type type = arg.getType();
                type = TypeUtil.stripSetType(type);
                if (i == 0) {
                    type0 = type;
                } else {
                    if (!TypeUtil.isUnionCompatible(type0, type)) {
                        throw MondrianResource.instance().ArgsMustHaveSameHierarchy.ex(getName());
                    }
                }
            }
        }
        return new SetType(type0);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        return new SetCalc(call, args, compiler);
    }

    /**
     * Compiled expression to implement the MDX set function, <code>{ ...
     * }</code>.
     *
     * <p>The set function can contain expressions which yield sets together
     * with expressions which yield individual members/tuples, provided that
     * they all have the same type. It automatically removes null members
     * or partially-null tuples from the list.
     *
     * <p>The implementation uses {@link VoidCalc} objects with side-effects
     * to avoid generating lots of intermediate lists.
     */
    public static class SetCalc extends AbstractListCalc {
        private final List result = new ArrayList();
        private final VoidCalc[] voidCalcs;

        public SetCalc(Exp exp, Exp[] args, ExpCompiler compiler) {
            super(exp, null);
            voidCalcs = compileSelf(args, compiler);
        }

        public Calc[] getCalcs() {
            return voidCalcs;
        }

        private VoidCalc[] compileSelf(Exp[] args, ExpCompiler compiler) {
            VoidCalc[] voidCalcs = new VoidCalc[args.length];
            for (int i = 0; i < args.length; i++) {
                voidCalcs[i] = createCalc(args[i], compiler);
            }
            return voidCalcs;
        }

        private VoidCalc createCalc(Exp arg, ExpCompiler compiler) {
            final Type type = arg.getType();
            if (type instanceof SetType) {
                final ListCalc listCalc = compiler.compileList(arg);
                final Type elementType = ((SetType) type).getElementType();
                if (elementType instanceof MemberType) {
                    return new AbstractVoidCalc(arg, new Calc[] {listCalc}) {
                        public void evaluateVoid(Evaluator evaluator) {
                            List list = listCalc.evaluateList(evaluator);
                            // Add only members which are not null.
                            for (int i = 0; i < list.size(); i++) {
                                Member member = (Member) list.get(i);
                                if (member == null || member.isNull()) {
                                    continue;
                                }
                                result.add(member);
                            }
                        }

                        protected String getName() {
                            return "Sublist";
                        }
                    };
                } else {
                    return new AbstractVoidCalc(arg, new Calc[] {listCalc}) {
                        public void evaluateVoid(Evaluator evaluator) {
                            List list = listCalc.evaluateList(evaluator);
                            // Add only tuples which are not null. Tuples with
                            // any null members are considered null.
                            list:
                            for (int i = 0; i < list.size(); i++) {
                                Member[] members = (Member[]) list.get(i);
                                for (int j = 0; j < members.length; j++) {
                                    Member member = members[j];
                                    if (member == null || member.isNull()) {
                                        continue list;
                                    }
                                }
                            }
                            result.addAll(list);
                        }

                        protected String getName() {
                            return "Sublist";
                        }
                    };
                }
            } else if (TypeUtil.couldBeMember(type)) {
                final MemberCalc listCalc = compiler.compileMember(arg);
                return new AbstractVoidCalc(arg, new Calc[] {listCalc}) {
                    public void evaluateVoid(Evaluator evaluator) {
                        Member member = listCalc.evaluateMember(evaluator);
                        if (member == null || member.isNull()) {
                            return;
                        }
                        result.add(member);
                    }

                    protected String getName() {
                        return "Sublist";
                    }
                };
            } else {
                final TupleCalc tupleCalc = compiler.compileTuple(arg);
                return new AbstractVoidCalc(arg, new Calc[] {tupleCalc}) {
                    public void evaluateVoid(Evaluator evaluator) {
                        // Don't add null or partially null tuple to result.
                        Member[] members = tupleCalc.evaluateTuple(evaluator);
                        if (members == null) {
                            return;
                        }
                        assert !tupleContainsNullMember(members);

                        result.add(members);
                    }
                };
            }
        }

        public List evaluateList(Evaluator evaluator) {
            result.clear();
            for (int i = 0; i < voidCalcs.length; i++) {
                voidCalcs[i].evaluateVoid(evaluator);
            }
            return new ArrayList(result);
        }
    }
}

// End SetFunDef.java
