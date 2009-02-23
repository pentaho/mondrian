/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2009 Julian Hyde and others
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
        if (args.length == 1
            && args[0].getType() instanceof SetType)
        {
            // Optimized case when there is only one argument. This occurs quite
            // often, because people write '{Foo.Children} on 1' when they could
            // write 'Foo.Children on 1'.
            return args[0].accept(compiler);
        }
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
                if (((SetType) type).getArity() == 1) {
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

    private static List<Calc> compileSelf(
        Exp[] args,
        ExpCompiler compiler,
        List<ResultStyle> resultStyles)
    {
        List<Calc> calcs = new ArrayList<Calc>(args.length);
        for (Exp arg : args) {
            calcs.add(createCalc(arg, compiler, resultStyles));
        }
        return calcs;
    }

    private static IterCalc createCalc(
        Exp arg,
        ExpCompiler compiler,
        List<ResultStyle> resultStyles)
    {
        final Type type = arg.getType();
        if (type instanceof SetType) {
            final Calc calc = compiler.compileAs(arg, null, resultStyles);
            if (((SetType) type).getArity() == 1) {
                switch (calc.getResultStyle()) {
                case ITERABLE:
                    final MemberIterCalc iterCalc = (MemberIterCalc) calc;
                    return new AbstractMemberIterCalc(arg, new Calc[] {calc}) {
                        public Iterable<Member> evaluateMemberIterable(Evaluator evaluator) {
                            return iterCalc.evaluateMemberIterable(evaluator);
                        }

                        protected String getName() {
                            return "Sublist";
                        }
                    };
                case LIST:
                case MUTABLE_LIST:
                    final MemberListCalc memberListCalc =
                        (MemberListCalc) calc;
                    return new AbstractMemberIterCalc(arg, new Calc[] {calc}) {
                        public Iterable<Member> evaluateMemberIterable(
                            Evaluator evaluator)
                        {
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
                    final TupleIterCalc iterCalc = (TupleIterCalc) calc;
                    return new AbstractTupleIterCalc(arg, new Calc[] {calc}) {
                        public Iterable<Member[]> evaluateTupleIterable(
                            Evaluator evaluator)
                        {
                            return iterCalc.evaluateTupleIterable(evaluator);
                        }

                        protected String getName() {
                            return "Sublist";
                        }
                    };
                case LIST:
                case MUTABLE_LIST:
                    final TupleListCalc tupleListCalc = (TupleListCalc) calc;
                    return new AbstractTupleIterCalc(arg, new Calc[] {calc}) {
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
            return new AbstractMemberIterCalc(call, new Calc[] {memberCalc}) {
                public Iterable<Member> evaluateMemberIterable(Evaluator evaluator) {
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
            return new AbstractTupleIterCalc(call, new Calc[] {tupleCalc}) {
                public Iterable<Member[]> evaluateTupleIterable(Evaluator evaluator) {
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

    /**
     * Creates a call to the set operator with a given collection of
     * expressions. There must be at least one expression.
     *
     * @param args Expressions
     * @return Call to set operator
     */
    public static ResolvedFunCall wrapAsSet(Exp... args) {
        assert args.length > 0;
        final int[] categories = new int[args.length];
        Type type = null;
        for (int i = 0; i < args.length; i++) {
            final Exp arg = args[i];
            categories[i] = arg.getCategory();
            final Type argType = arg.getType();
            if (argType instanceof SetType) {
                type = ((SetType) argType).getElementType();
            }
        }
        return new ResolvedFunCall(
            new SetFunDef(Resolver, categories),
            args,
            new SetType(type));
    }

    /**
     * Compiled expression that evaluates one or more expressions, each of which
     * yields a member or a set of members, and returns the result as an member
     * iterator.
     */
    public static class ExprMemberIterCalc extends AbstractMemberIterCalc {
        private final MemberIterCalc[] iterCalcs;

        public ExprMemberIterCalc(
            Exp exp,
            Exp[] args,
            ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
            super(exp, null);
            final List<Calc> calcList = compileSelf(args, compiler, resultStyles);
            iterCalcs = calcList.toArray(new MemberIterCalc[calcList.size()]);
        }

        public Calc[] getCalcs() {
            return iterCalcs;
        }

        public Iterable<Member> evaluateMemberIterable(final Evaluator evaluator) {
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
                                MemberIterCalc iterCalc = iterCalcs[index++];
                                Iterable<Member> iter =
                                    iterCalc.evaluateMemberIterable(evaluator);
                                currentIterator = iter.iterator();
                            }
                            while (true) {
                                boolean b = currentIterator.hasNext();
                                while (! b) {
                                    if (index >= iterCalcs.length) {
                                        return false;
                                    }
                                    MemberIterCalc iterCalc = iterCalcs[index++];
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
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            int[] parameterTypes = new int[args.length];
            for (int i = 0; i < args.length; i++) {
                if (validator.canConvert(
                        args[i], Category.Member, conversions)) {
                    parameterTypes[i] = Category.Member;
                    continue;
                }
                if (validator.canConvert(
                        args[i], Category.Set, conversions)) {
                    parameterTypes[i] = Category.Set;
                    continue;
                }
                if (validator.canConvert(
                        args[i], Category.Tuple, conversions)) {
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
