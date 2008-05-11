/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.util.ConcatenableList;
import mondrian.util.FilteredIterableList;

import java.io.PrintWriter;
import java.util.*;

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
        return new ListSetCalc(
            call, args, compiler,
            ResultStyle.LIST_MUTABLELIST);
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
        private List result = new ConcatenableList();
        private final VoidCalc[] voidCalcs;

        public ListSetCalc(
            Exp exp, Exp[] args, ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
            super(exp, null);
            voidCalcs = compileSelf(args, compiler, resultStyles);
        }

        public Calc[] getCalcs() {
            return voidCalcs;
        }

        private VoidCalc[] compileSelf(Exp[] args,
                ExpCompiler compiler,
                List<ResultStyle> resultStyles) {
            VoidCalc[] voidCalcs = new VoidCalc[args.length];
            for (int i = 0; i < args.length; i++) {
                voidCalcs[i] = createCalc(args[i], compiler, resultStyles);
            }
            return voidCalcs;
        }

        private VoidCalc createCalc(
            Exp arg,
            ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
            final Type type = arg.getType();
            if (type instanceof SetType) {
                // TODO use resultStyles
                final ListCalc listCalc = compiler.compileList(arg);
                final Type elementType = ((SetType) type).getElementType();
                if (elementType instanceof MemberType) {
                    final MemberListCalc memberListCalc =
                        (MemberListCalc) listCalc;
                    return new AbstractVoidCalc(arg, new Calc[] {listCalc}) {
                        public void evaluateVoid(Evaluator evaluator) {
                            final List<Member> memberList =
                                memberListCalc.evaluateMemberList(evaluator);
                            final List<Member> list =
                                new FilteredIterableList<Member>(
                                    memberList,
                                    new FilteredIterableList.Filter<Member>() {
                                        public boolean accept(Member m) {
                                            return m != null && !m.isNull();
                                        }
                                    }
                                );
                            result.addAll(list);
                        }

                        protected String getName() {
                            return "Sublist";
                        }
                    };
                } else {
                    final TupleListCalc tupleListCalc =
                        (TupleListCalc) listCalc;
                    return new AbstractVoidCalc(arg, new Calc[] {listCalc}) {
                        public void evaluateVoid(Evaluator evaluator) {
                            List<Member[]> list =
                                tupleListCalc.evaluateTupleList(evaluator);
                            // Add only tuples which are not null. Tuples with
                            // any null members are considered null.
                            outer:
                            for (Member[] members : list) {
                                for (Member member : members) {
                                    if (member == null || member.isNull()) {
                                        continue outer;
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

        public List evaluateList(final Evaluator evaluator) {
            this.result = new ConcatenableList();
            for (VoidCalc voidCalc : voidCalcs) {
                voidCalc.evaluateVoid(evaluator);
            }
            // REVIEW: What the heck is this code doing? Why is it OK to
            // ignore IndexOutOfBoundsException and NullPointerException?
            try {
                if (result.get(0) instanceof Member) {
                    if (!((Member)result.get(0)).getDimension()
                            .isHighCardinality()) {
                        result.toArray();
                    }
                }
            } catch (IndexOutOfBoundsException iooe) {
            } catch (NullPointerException npe) {
            }
            return result;
        }
    }

    public static class IterSetCalc extends AbstractIterCalc {
        private final IterCalc[] iterCalcs;

        public IterSetCalc(
            Exp exp,
            Exp[] args,
            ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
            super(exp, null);
            iterCalcs = compileSelf(args, compiler, resultStyles);
        }

        public Calc[] getCalcs() {
            return iterCalcs;
        }

        private IterCalc[] compileSelf(
            Exp[] args,
            ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
            IterCalc[] iterCalcs = new IterCalc[args.length];
            for (int i = 0; i < args.length; i++) {
                iterCalcs[i] = createCalc(args[i], compiler, resultStyles);
            }
            return iterCalcs;
        }

        private IterCalc createCalc(
            Exp arg,
            ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
            final Type type = arg.getType();
            if (type instanceof SetType) {
                final Calc calc = compiler.compileAs(arg, null, resultStyles);
                final Type elementType = ((SetType) type).getElementType();
                if (elementType instanceof MemberType) {
                    switch (calc.getResultStyle()) {
                    case ITERABLE:
                        return new AbstractIterCalc(arg, new Calc[] {calc}) {
                            private final IterCalc iterCalc = (IterCalc) calc;
                            public Iterable evaluateIterable(Evaluator evaluator) {
                                return iterCalc.evaluateIterable(evaluator);
                            }
                            protected String getName() {
                                return "Sublist";
                            }
                        };
                    case LIST:
                    case MUTABLE_LIST:
                        return new AbstractIterCalc(arg, new Calc[] {calc}) {
                            private final MemberListCalc memberListCalc =
                                (MemberListCalc) calc;
                            public Iterable evaluateIterable(Evaluator evaluator) {
                                List<Member> result = new ArrayList<Member>();
                                List<Member> list =
                                    memberListCalc.evaluateMemberList(evaluator);
                                // Add only members which are not null.
                                for (Member member : list) {
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
                        ResultStyle.ITERABLE_LIST_MUTABLELIST,
                        calc.getResultStyle());
                } else {
                    switch (calc.getResultStyle()) {
                    case ITERABLE:
                        return new AbstractIterCalc(arg, new Calc[] {calc}) {
                            private final IterCalc iterCalc = (IterCalc) calc;
                            public Iterable evaluateIterable(Evaluator evaluator) {
                                return iterCalc.evaluateIterable(evaluator);
                            }
                            protected String getName() {
                                return "Sublist";
                            }
                        };
                    case LIST:
                    case MUTABLE_LIST:
                        return new AbstractIterCalc(arg, new Calc[] {calc}) {
                            private final TupleListCalc tupleListCalc =
                                (TupleListCalc) calc;

                            public Iterable evaluateIterable(Evaluator evaluator) {
                                return evaluateTupleIterable(evaluator);
                            }

                            public Iterable<Member[]> evaluateTupleIterable(
                                Evaluator evaluator)
                            {
                                List<Member[]> result =
                                    new ArrayList<Member[]>();
                                List<Member[]> list =
                                    tupleListCalc.evaluateTupleList(evaluator);
                                // Add only tuples which are not null. Tuples with
                                // any null members are considered null.
                                list:
                                for (Member[] members : list) {
                                    for (Member member : members) {
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
                        ResultStyle.ITERABLE_LIST_MUTABLELIST,
                        calc.getResultStyle());
                }
            } else if (TypeUtil.couldBeMember(type)) {
                final MemberCalc memberCalc = compiler.compileMember(arg);
                final ResolvedFunCall call = wrapAsSet(arg);
                return new AbstractIterCalc(call, new Calc[] {memberCalc}) {
                    public Iterable evaluateIterable(Evaluator evaluator) {
                        final Member member =
                            memberCalc.evaluateMember(evaluator);
                        return new Iterable<Member>() {
                            public Iterator<Member> iterator() {
                                return new Iterator<Member>() {
                                    private Member m = member;
                                    public boolean hasNext() {
                                        return (m != null);
                                    }
                                    public Member next() {
                                        try {
                                            return m;
                                        } finally {
                                            m = null;
                                        }
                                    }
                                    public void remove() {
                                        throw new UnsupportedOperationException("remove");
                                    }
                                };
                            }
                        };
                    }
                    protected String getName() {
                        return "Sublist";
                    }
                };
            } else {
                final TupleCalc tupleCalc = compiler.compileTuple(arg);
                final ResolvedFunCall call = wrapAsSet(arg);
                return new AbstractIterCalc(call, new Calc[] {tupleCalc}) {
                    public Iterable evaluateIterable(Evaluator evaluator) {
                        final Member[] members = tupleCalc.evaluateTuple(evaluator);
                        return new Iterable<Member[]>() {
                            public Iterator<Member[]> iterator() {
                                return new Iterator<Member[]>() {
                                    private Member[] m = members;
                                    public boolean hasNext() {
                                        return (m != null);
                                    }
                                    public Member[] next() {
                                        try {
                                            return m;
                                        } finally {
                                            m = null;
                                        }
                                    }
                                    public void remove() {
                                        throw new UnsupportedOperationException("remove");
                                    }
                                };
                            }
                        };
                    }
                    protected String getName() {
                        return "Sublist";
                    }
                };
            }
        }

        private ResolvedFunCall wrapAsSet(Exp arg) {
            return new ResolvedFunCall(
                    new SetFunDef(Resolver, new int[] {arg.getCategory()}),
                    new Exp[] {arg},
                    new SetType(arg.getType()));
        }

        public Iterable evaluateIterable(final Evaluator evaluator) {
            return new Iterable<Member>() {
                public Iterator<Member> iterator() {
                    return new Iterator<Member>() {
                        int index = 0;
                        Iterator<Member> currentIterator = null;
                        Member member = null;
                        public boolean hasNext() {
                            if (member != null) {
                                return true;
                            }
                            if (currentIterator == null) {
                                if (index >= iterCalcs.length) {
                                    return false;
                                }
                                IterCalc iterCalc = iterCalcs[index++];
                                Iterable<Member> iter =
                                    iterCalc.evaluateMemberIterable(evaluator);
                                currentIterator = iter.iterator();
                            }
                            while(true) {
                                boolean b = currentIterator.hasNext();
                                while (! b) {
                                    if (index >= iterCalcs.length) {
                                        return false;
                                    }
                                    IterCalc iterCalc = iterCalcs[index++];
                                    Iterable<Member> iter =
                                        iterCalc.evaluateMemberIterable(evaluator);
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
                            try {
                                return member;
                            } finally {
                                member = null;
                            }
                        }
                        public void remove() {
                            throw new UnsupportedOperationException("remove");
                        }
                    };
                }
            };
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
