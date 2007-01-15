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

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.AbstractIterCalc;
import mondrian.calc.impl.AbstractVoidCalc;
import mondrian.calc.ExpCompiler.ResultStyle;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.mdx.ResolvedFunCall;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <code>SetFunDef</code> implements the 'set' function (whose syntax is the
 * brace operator, <code>{ ... }</code>).
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 */
public class SetFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

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
                type = TypeUtil.toMemberOrTupleType(type);
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
        return new ListSetCalc(call, args, compiler,
                    ExpCompiler.LIST_MUTABLE_LIST_RESULT_STYLE_ARRAY);
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
    public static class ListSetCalc extends AbstractListCalc {
        private List result = new ArrayList();
        private final VoidCalc[] voidCalcs;

        public ListSetCalc(Exp exp, Exp[] args, ExpCompiler compiler,
                ResultStyle[] resultStyles) {
            super(exp, null);
            voidCalcs = compileSelf(args, compiler, resultStyles);
        }

        public Calc[] getCalcs() {
            return voidCalcs;
        }

        private VoidCalc[] compileSelf(Exp[] args, 
                ExpCompiler compiler,
                ResultStyle[] resultStyles) {
            VoidCalc[] voidCalcs = new VoidCalc[args.length];
            for (int i = 0; i < args.length; i++) {
                voidCalcs[i] = createCalc(args[i], compiler, resultStyles);
            }
            return voidCalcs;
        }

        private VoidCalc createCalc(Exp arg, 
                ExpCompiler compiler,
                ResultStyle[] resultStyles) {
            final Type type = arg.getType();
            if (type instanceof SetType) {
                // TODO use resultStyles
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
                                result.add(members);
                            }
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
    public static class IterSetCalc extends AbstractIterCalc {
        private final IterCalc[] iterCalcs;

        public IterSetCalc(Exp exp, Exp[] args, ExpCompiler compiler,
                ResultStyle[] resultStyles) {
            super(exp, null);
            iterCalcs = compileSelf(args, compiler, resultStyles);
        }

        public Calc[] getCalcs() {
            return iterCalcs;
        }

        private IterCalc[] compileSelf(Exp[] args, 
                ExpCompiler compiler,
                ResultStyle[] resultStyles) {
            IterCalc[] iterCalcs = new IterCalc[args.length];
            for (int i = 0; i < args.length; i++) {
                iterCalcs[i] = createCalc(args[i], compiler, resultStyles);
            }
            return iterCalcs;
        }

        private IterCalc createCalc(Exp arg, 
                ExpCompiler compiler,
                ResultStyle[] resultStyles) {

            final Type type = arg.getType();
            if (type instanceof SetType) {
                final Calc calc = compiler.compile(arg, resultStyles);
                final Type elementType = ((SetType) type).getElementType();
                if (elementType instanceof MemberType) {
                    switch (calc.getResultStyle()) {
                    case ITERABLE :
                    return new AbstractIterCalc(arg, new Calc[] {calc}) {
                        private final IterCalc iterCalc = (IterCalc) calc;
                        public Iterable evaluateIterable(Evaluator evaluator) {
                            Iterable iterable = 
                                iterCalc.evaluateIterable(evaluator);
                            return iterable;
                        }
                        protected String getName() {
                            return "Sublist";
                        }
                    };
                    case LIST :
                    case MUTABLE_LIST :
                    return new AbstractIterCalc(arg, new Calc[] {calc}) {
                        private final ListCalc listCalc = (ListCalc) calc;
                        public Iterable evaluateIterable(Evaluator evaluator) {
                            List result = new ArrayList();
                            List list = listCalc.evaluateList(evaluator);
                            // Add only members which are not null.
                            for (int i = 0; i < list.size(); i++) {
                                Member member = (Member) list.get(i);
                                if (member == null || member.isNull()) {
                                    continue;
                                }
                                result.add(member);
                            }
                            return result;
                        }
                        protected String getName() {
                            return "Sublist";
                        }
                    };
                    }
                    throw ResultStyleException.generateBadType(
                        new ResultStyle[] {
                            ResultStyle.ITERABLE,
                            ResultStyle.LIST,
                            ResultStyle.MUTABLE_LIST
                        }, calc.getResultStyle());
                } else {
                    switch (calc.getResultStyle()) {
                    case ITERABLE :
                    return new AbstractIterCalc(arg, new Calc[] {calc}) {
                        private final IterCalc iterCalc = (IterCalc) calc;
                        public Iterable evaluateIterable(Evaluator evaluator) {
                            Iterable iterable = 
                                iterCalc.evaluateIterable(evaluator);
                            return iterable;
                        }
                        protected String getName() {
                            return "Sublist";
                        }
                    };
                    case LIST :
                    case MUTABLE_LIST :
                    return new AbstractIterCalc(arg, new Calc[] {calc}) {
                        private final ListCalc listCalc = (ListCalc) calc;
                        public Iterable evaluateIterable(Evaluator evaluator) {
                            List result = new ArrayList();
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
                                result.add(members);
                            }
                            return result;
                        }

                        protected String getName() {
                            return "Sublist";
                        }
                    };
                    }
                    throw ResultStyleException.generateBadType(
                        new ResultStyle[] {
                            ResultStyle.ITERABLE,
                            ResultStyle.LIST,
                            ResultStyle.MUTABLE_LIST
                        },
                        calc.getResultStyle());

                }
            } else if (TypeUtil.couldBeMember(type)) {
                final MemberCalc memberCalc = compiler.compileMember(arg);
                return new AbstractIterCalc(arg, new Calc[] {memberCalc}) {
                    public Iterable evaluateIterable(Evaluator evaluator) {
                        final Member member = 
                            memberCalc.evaluateMember(evaluator);
                        Iterable<Member> iterable = new Iterable<Member>() {
                            public Iterator<Member> iterator() { 
                                return new Iterator<Member>() {
                                    private Member m = null;
                                    public boolean hasNext() {
                                        if (m == null) {
                                            m = member;
                                            return true;
                                        } else {
                                            return false;
                                        }
                                    }
                                    public Member next() {
                                        return m;
                                    }
                                    public void remove() {
                                        throw new UnsupportedOperationException("remove");
                                    }
                                };
                            }
                        };
                        return iterable;
                    }
                    protected String getName() {
                        return "Sublist";
                    }
                };
            } else {
                final TupleCalc tupleCalc = compiler.compileTuple(arg);

                return new AbstractIterCalc(arg, new Calc[] {tupleCalc}) {
                    public Iterable evaluateIterable(Evaluator evaluator) {
                        final Member[] members = tupleCalc.evaluateTuple(evaluator);
                        Iterable<Member[]> iterable = new Iterable<Member[]>() {
                            public Iterator<Member[]> iterator() { 
                                return new Iterator<Member[]>() {
                                    private Member[] m = null;
                                    public boolean hasNext() {
                                        if (m == null) {
                                            m = members;
                                            return true;
                                        } else {
                                            return false;
                                        }
                                    }
                                    public Member[] next() {
                                        return m;
                                    }
                                    public void remove() {
                                        throw new UnsupportedOperationException("remove");
                                    }
                                };
                            }
                        };
                        return iterable;
                    }
                    protected String getName() {
                        return "Sublist";
                    }
                };
            }
        }

        public Iterable evaluateIterable(final Evaluator evaluator) {
            Iterable result = new Iterable<Member>() {
                public Iterator<Member> iterator() {
                    return new Iterator<Member>() {
                        int index = 0;
                        Iterator<Member> currentIterator = null;
                        Member member = null;
                        public boolean hasNext() {
                            if (currentIterator == null) {
                                if (index >= iterCalcs.length) {
                                    return false;
                                }
                                IterCalc iterCalc = iterCalcs[index++];
                                Iterable iter = 
                                    iterCalc.evaluateIterable(evaluator);
                                currentIterator = iter.iterator();
                            }
                            while(true) {
                                boolean b = currentIterator.hasNext();
                                while (! b) {
                                    if (index >= iterCalcs.length) {
                                        return false;
                                    }
                                    IterCalc iterCalc = iterCalcs[index++];
                                    Iterable iter = 
                                        iterCalc.evaluateIterable(evaluator);
                                    currentIterator = iter.iterator();
                                    b = currentIterator.hasNext();
                                }
                                member = currentIterator.next();
                                if (member != null) {
                                    break;
                                }
                            }
                            return true;
                        }
                        public Member next() {
                            return member;
                        }
                        public void remove() {
                            throw new UnsupportedOperationException("remove");
                        }
                    };
                }
            };
            return result;
        }
    }

    private static class ResolverImpl extends ResolverBase {
        public ResolverImpl() {
            super(
                    "{}",
                    "{<Member> [, <Member>...]}",
                    "Brace operator constructs a set.",
                    Syntax.Braces);
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount) {
            int[] parameterTypes = new int[args.length];
            for (int i = 0; i < args.length; i++) {
                if (validator.canConvert(
                        args[i], Category.Member, conversionCount)) {
                    parameterTypes[i] = Category.Member;
                    continue;
                }
                if (validator.canConvert(
                        args[i], Category.Set, conversionCount)) {
                    parameterTypes[i] = Category.Set;
                    continue;
                }
                if (validator.canConvert(
                        args[i], Category.Tuple, conversionCount)) {
                    parameterTypes[i] = Category.Tuple;
                    continue;
                }
                return null;
            }
            return new SetFunDef(this, parameterTypes);
        }
    }
}

// End SetFunDef.java
