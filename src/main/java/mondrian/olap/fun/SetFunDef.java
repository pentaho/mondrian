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
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;

import java.io.PrintWriter;
import java.util.*;

/**
 * <code>SetFunDef</code> implements the 'set' function (whose syntax is the
 * brace operator, <code>{ ... }</code>).
 *
 * @author jhyde
 * @since 3 March, 2002
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
                        throw MondrianResource.instance()
                            .ArgsMustHaveSameHierarchy.ex(getName());
                    }
                }
            }
        }
        return new SetType(type0);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        if (args.length == 0) {
            // Special treatment for empty set, because we don't know whether it
            // is a set of members or tuples, and so we need it to implement
            // both MemberListCalc and ListCalc.
            return new EmptyListCalc(call);
        }
        if (args.length == 1
            && args[0].getType() instanceof SetType)
        {
            // Optimized case when there is only one argument. This occurs quite
            // often, because people write '{Foo.Children} on 1' when they could
            // write 'Foo.Children on 1'.
            return args[0].accept(compiler);
        }
        return new SetListCalc(
            call, args, compiler, ResultStyle.LIST_MUTABLELIST);
    }

    /**
     * Compiled expression to implement the MDX set function, <code>{ ...
     * }</code>, applied to a set of tuples, as a list.
     *
     * <p>The set function can contain expressions which yield sets together
     * with expressions which yield individual tuples, provided that
     * they all have the same type. It automatically removes null
     * or partially-null tuples from the list.
     *
     * <p>Also, does not process high-cardinality dimensions specially.
     */
    public static class SetListCalc extends AbstractListCalc {
        private TupleList result;
        private final VoidCalc[] voidCalcs;

        public SetListCalc(
            Exp exp,
            Exp[] args,
            ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
            super(exp, null);
            voidCalcs = compileSelf(args, compiler, resultStyles);
            result = TupleCollections.createList(getType().getArity());
        }

        public Calc[] getCalcs() {
            return voidCalcs;
        }

        private VoidCalc[] compileSelf(
            Exp[] args,
            ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
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
                return new AbstractVoidCalc(arg, new Calc[] {listCalc}) {
                    public void evaluateVoid(Evaluator evaluator) {
                        TupleList list =
                            listCalc.evaluateList(evaluator);
                        // Add only tuples which are not null. Tuples with
                        // any null members are considered null.
                        outer:
                        for (List<Member> members : list) {
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
            } else if (type.getArity() == 1) {
                final MemberCalc memberCalc = compiler.compileMember(arg);
                return new AbstractVoidCalc(arg, new Calc[]{memberCalc}) {
                    final Member[] members = {null};
                    public void evaluateVoid(Evaluator evaluator) {
                        // Don't add null or partially null tuple to result.
                        Member member = memberCalc.evaluateMember(evaluator);
                        if (member == null || member.isNull()) {
                            return;
                        }
                        members[0] = member;
                        result.addTuple(members);
                    }
                };
            } else {
                final TupleCalc tupleCalc = compiler.compileTuple(arg);
                return new AbstractVoidCalc(arg, new Calc[]{tupleCalc}) {
                    public void evaluateVoid(Evaluator evaluator) {
                        // Don't add null or partially null tuple to result.
                        Member[] members = tupleCalc.evaluateTuple(evaluator);
                        if (members == null
                            || tupleContainsNullMember(members))
                        {
                            return;
                        }
                        result.addTuple(members);
                    }
                };
            }
        }

        public TupleList evaluateList(final Evaluator evaluator) {
            result.clear();
            for (VoidCalc voidCalc : voidCalcs) {
                voidCalc.evaluateVoid(evaluator);
            }
            return result.cloneList(-1);
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
            switch (calc.getResultStyle()) {
            case ITERABLE:
                final IterCalc iterCalc = (IterCalc) calc;
                return new AbstractIterCalc(arg, new Calc[]{calc}) {
                    public TupleIterable evaluateIterable(
                        Evaluator evaluator)
                    {
                        return iterCalc.evaluateIterable(evaluator);
                    }

                    protected String getName() {
                        return "Sublist";
                    }
                };
            case LIST:
            case MUTABLE_LIST:
                final ListCalc listCalc = (ListCalc) calc;
                return new AbstractIterCalc(arg, new Calc[]{calc}) {
                    public TupleIterable evaluateIterable(
                        Evaluator evaluator)
                    {
                        TupleList list = listCalc.evaluateList(
                            evaluator);
                        TupleList result = list.cloneList(list.size());
                        // Add only tuples which are not null. Tuples with
                        // any null members are considered null.
                        list:
                        for (List<Member> members : list) {
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
        } else if (TypeUtil.couldBeMember(type)) {
            final MemberCalc memberCalc = compiler.compileMember(arg);
            final ResolvedFunCall call = wrapAsSet(arg);
            return new AbstractIterCalc(call, new Calc[] {memberCalc}) {
                public TupleIterable evaluateIterable(
                    Evaluator evaluator)
                {
                    final Member member =
                        memberCalc.evaluateMember(evaluator);
                    return member == null
                        ? TupleCollections.createList(1)
                        : new UnaryTupleList(Collections.singletonList(member));
                }

                protected String getName() {
                    return "Sublist";
                }
            };
        } else {
            final TupleCalc tupleCalc = compiler.compileTuple(arg);
            final ResolvedFunCall call = wrapAsSet(arg);
            return new AbstractIterCalc(call, new Calc[] {tupleCalc}) {
                public TupleIterable evaluateIterable(
                    Evaluator evaluator)
                {
                    final Member[] members = tupleCalc.evaluateTuple(evaluator);
                    return new ListTupleList(
                        tupleCalc.getType().getArity(),
                        Arrays.asList(members));
                }

                protected String getName() {
                    return "Sublist";
                }
            };
        }
    }

    /**
     * Creates a call to the set operator with a given collection of
     * expressions.
     *
     * <p>There must be at least one expression. Each expression may be a set of
     * members/tuples, or may be a member/tuple, but method assumes that
     * expressions have compatible types.
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
            } else {
                type = argType;
            }
        }
        return new ResolvedFunCall(
            new SetFunDef(Resolver, categories),
            args,
            new SetType(type));
    }

    /**
     * Compiled expression that evaluates one or more expressions, each of which
     * yields a tuple or a set of tuples, and returns the result as a tuple
     * iterator.
     */
    public static class ExprIterCalc extends AbstractIterCalc {
        private final IterCalc[] iterCalcs;

        public ExprIterCalc(
            Exp exp,
            Exp[] args,
            ExpCompiler compiler,
            List<ResultStyle> resultStyles)
        {
            super(exp, null);
            final List<Calc> calcList =
                compileSelf(args, compiler, resultStyles);
            iterCalcs = calcList.toArray(new IterCalc[calcList.size()]);
        }

        // override return type
        public IterCalc[] getCalcs() {
            return iterCalcs;
        }

        public TupleIterable evaluateIterable(
            final Evaluator evaluator)
        {
            return new AbstractTupleIterable(getType().getArity()) {
                public TupleCursor tupleCursor() {
                    return new AbstractTupleCursor(arity) {
                        Iterator<IterCalc> calcIterator =
                            Arrays.asList(iterCalcs).iterator();
                        TupleCursor currentCursor =
                            TupleCollections.emptyList(1).tupleCursor();

                        public boolean forward() {
                            while (true) {
                                if (currentCursor.forward()) {
                                    return true;
                                }
                                if (!calcIterator.hasNext()) {
                                    return false;
                                }
                                currentCursor =
                                    calcIterator.next()
                                        .evaluateIterable(evaluator)
                                        .tupleCursor();
                            }
                        }

                        public List<Member> current() {
                            return currentCursor.current();
                        }

                        @Override
                        public void setContext(Evaluator evaluator) {
                            currentCursor.setContext(evaluator);
                        }

                        @Override
                        public void currentToArray(
                            Member[] members, int offset)
                        {
                            currentCursor.currentToArray(members, offset);
                        }

                        @Override
                        public Member member(int column) {
                            return currentCursor.member(column);
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
                        i, args[i], Category.Member, conversions))
                {
                    parameterTypes[i] = Category.Member;
                    continue;
                }
                if (validator.canConvert(
                        i, args[i], Category.Tuple, conversions))
                {
                    parameterTypes[i] = Category.Tuple;
                    continue;
                }
                if (validator.canConvert(
                        i, args[i], Category.Set, conversions))
                {
                    parameterTypes[i] = Category.Set;
                    continue;
                }
                return null;
            }
            return new SetFunDef(this, parameterTypes);
        }
    }

    /**
     * Compiled expression that returns an empty list of members or tuples.
     */
    private static class EmptyListCalc extends AbstractListCalc {
        private final TupleList list;

        /**
         * Creates an EmptyListCalc.
         *
         * @param call Expression which was compiled
         */
        EmptyListCalc(ResolvedFunCall call) {
            super(call, new Calc[0]);

            list = TupleCollections.emptyList(call.getType().getArity());
        }

        public TupleList evaluateList(Evaluator evaluator) {
            return list;
        }
    }
}

// End SetFunDef.java
