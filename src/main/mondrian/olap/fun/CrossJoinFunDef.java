/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2003-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.ExpCompiler.ResultStyle;
import mondrian.calc.impl.AbstractIterCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.util.Bug;
import mondrian.util.UnsupportedList;

import java.util.*;

/**
 * Definition of the <code>CrossJoin</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class CrossJoinFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "Crossjoin",
            "Crossjoin(<Set1>, <Set2>)",
            "Returns the cross product of two sets.",
            new String[]{"fxxx"},
            CrossJoinFunDef.class);

    static final StarCrossJoinResolver StarResolver = new StarCrossJoinResolver();

    private static int counterTag = 0;

    // used to tell the difference between crossjoin expressions.
    private final int ctag = counterTag++;

    public CrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        // CROSSJOIN(<Set1>,<Set2>) has type [Hie1] x [Hie2].
        List<Type> list = new ArrayList<Type>();
        for (Exp arg : args) {
            final Type type = arg.getType();
            if (type instanceof SetType) {
                addTypes(type, list);
            } else if (getName().equals("*")) {
                // The "*" form of CrossJoin is lenient: args can be either
                // members/tuples or sets.
                addTypes(type, list);
            } else {
                throw Util.newInternal("arg to crossjoin must be a set");
            }
        }
        final Type[] types = list.toArray(new Type[list.size()]);
        final TupleType tupleType = new TupleType(types);
        return new SetType(tupleType);
    }

    /**
     * Adds a type to a list of types. If type is a {@link TupleType}, does so
     * recursively.
     */
    private static void addTypes(final Type type, List<Type> list) {
        if (type instanceof SetType) {
            SetType setType = (SetType) type;
            addTypes(setType.getElementType(), list);
        } else if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            for (Type elementType : tupleType.elementTypes) {
                addTypes(elementType, list);
            }
        } else {
            list.add(type);
        }
    }
    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        ResultStyle[] rs = compiler.getAcceptableResultStyles();
        // What is the desired return type?
        for (ResultStyle r : rs) {
            switch (r) {
            case ITERABLE:
            case ANY:
                // Consumer wants ITERABLE or ANY
                    return compileCallIterable(call, compiler);
            case LIST:
                // Consumer wants (immutable) LIST
                return compileCallImmutableList(call, compiler);
            case MUTABLE_LIST:
                // Consumer MUTABLE_LIST
                return compileCallMutableList(call, compiler);
            }
        }
        throw ResultStyleException.generate(
            new ResultStyle[] {
                ResultStyle.ITERABLE,
                ResultStyle.LIST,
                ResultStyle.MUTABLE_LIST,
                ResultStyle.ANY
            },
            rs
        );
    }

    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    // Iterable
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    protected IterCalc compileCallIterable(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        final Calc calc1 = toIter(compiler, call.getArg(0));
        final Calc calc2 = toIter(compiler, call.getArg(1));
        Calc[] calcs = new Calc[] {calc1, calc2};
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: ITERABLE, LIST or MUTABLE_LIST, but
        // LIST and MUTABLE_LIST are treated the same; so
        // there are 16 possible combinations - sweet.

        // Check returned calc ResultStyles
        checkIterListResultStyles(calc1);
        checkIterListResultStyles(calc2);

        if (isMemberType(calc1)) {
            // Member
            if (isMemberType(calc2)) {
                // Member
                if (calc1.getResultStyle() == ResultStyle.ITERABLE) {
                    if (calc2.getResultStyle() == ResultStyle.ITERABLE) {
                        return new IterMemberIterMemberIterCalc(call, calcs);
                    } else {
                        return new IterMemberListMemberIterCalc(call, calcs);
                    }
                } else {
                    if (calc2.getResultStyle() == ResultStyle.ITERABLE) {
                        return new ListMemberIterMemberIterCalc(call, calcs);
                    } else {
                        return new ListMemberListMemberIterCalc(call, calcs);
                    }
                }
            } else {
                // Member[]
                if (calc1.getResultStyle() == ResultStyle.ITERABLE) {
                    if (calc2.getResultStyle() == ResultStyle.ITERABLE) {
                        return new IterMemberIterMemberArrayIterCalc(call, calcs);
                    } else {
                        return new IterMemberListMemberArrayIterCalc(call, calcs);
                    }
                } else {
                    if (calc2.getResultStyle() == ResultStyle.ITERABLE) {
                        return new ListMemberIterMemberArrayIterCalc(call, calcs);
                    } else {
                        return new ListMemberListMemberArrayIterCalc(call, calcs);
                    }
                }
            }
        } else {
            // Member[]
            if (isMemberType(calc2)) {
                // Member
                if (calc1.getResultStyle() == ResultStyle.ITERABLE) {
                    if (calc2.getResultStyle() == ResultStyle.ITERABLE) {
                        return new IterMemberArrayIterMemberIterCalc(call, calcs);
                    } else {
                        return new IterMemberArrayListMemberIterCalc(call, calcs);
                    }
                } else {
                    if (calc2.getResultStyle() == ResultStyle.ITERABLE) {
                        return new ListMemberArrayIterMemberIterCalc(call, calcs);
                    } else {
                        return new ListMemberArrayListMemberIterCalc(call, calcs);
                    }
                }
            } else {
                // Member[]
                if (calc1.getResultStyle() == ResultStyle.ITERABLE) {
                    if (calc2.getResultStyle() == ResultStyle.ITERABLE) {
                        return new IterMemberArrayIterMemberArrayIterCalc(call, calcs);
                    } else {
                        return new IterMemberArrayListMemberArrayIterCalc(call, calcs);
                    }
                } else {
                    if (calc2.getResultStyle() == ResultStyle.ITERABLE) {
                        return new ListMemberArrayIterMemberArrayIterCalc(call, calcs);
                    } else {
                        return new ListMemberArrayListMemberArrayIterCalc(call, calcs);
                    }
                }
            }
        }
    }
    private Calc toIter(ExpCompiler compiler, final Exp exp) {
        // Want iterable, immutable list or mutable list in that order
        // It is assumed that an immutable list is easier to get than
        // a mutable list.
        final Type type = exp.getType();
        if (type instanceof SetType) {
            // this can return an IterCalc or ListCalc
            return compiler.compile(exp,
                ExpCompiler.ITERABLE_LIST_MUTABLE_LIST_RESULT_STYLE_ARRAY);
        } else {
            // this always returns an IterCalc
            return new SetFunDef.IterSetCalc(
                new DummyExp(new SetType(type)),
                new Exp[] {exp},
                compiler,
                ExpCompiler.ITERABLE_LIST_MUTABLE_LIST_RESULT_STYLE_ARRAY);
        }
    }
    private abstract class BaseIterCalc extends AbstractIterCalc {
        protected BaseIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        public Iterable evaluateIterable(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                schemaReader.getNativeSetEvaluator(
                    call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (Iterable) nativeEvaluator.execute(
                            ResultStyle.ITERABLE);
            }

            Calc[] calcs = getCalcs();
            Calc calc1 = calcs[0];
            Calc calc2 = calcs[1];

            Evaluator oldEval = null;
            assert (oldEval = evaluator.push()) != null;

            Object o1 = calc1.evaluate(evaluator);
            assert oldEval.equals(evaluator) : "calc1 changed context";

            if (o1 instanceof List) {
                List l1 = (List) o1;
                //l1 = checkList(evaluator, l1);
                l1 = nonEmptyOptimizeList(evaluator, l1, call);
                if (l1.isEmpty()) {
                    return Collections.EMPTY_LIST;
                }
                o1 = l1;
            }

            Object o2 = calc2.evaluate(evaluator);
            assert oldEval.equals(evaluator) : "calc2 changed context";

            if (o2 instanceof List) {
                List l2 = (List) o2;
                //l2 = checkList(evaluator, l2);
                l2 = nonEmptyOptimizeList(evaluator, l2, call);
                if (l2.isEmpty()) {
                    return Collections.EMPTY_LIST;
                }
                o2 = l2;
            }

            return makeIterable(o1, o2);
        }

        /**
         * Derived classes implement and create Iterable&lt;Member[]&gt;
         * based upon the types of the parameters:
         * List&lt;Member&gt;,
         * List&lt;Member[]&gt;,
         * Iterable&lt;Member&gt;, or
         * Iterable&lt;Member[]&gt;.
         *
         * @param o1 List or Iterable of Member or Member[]
         * @param o2 List or Iterable of Member or Member[]
         * @return Iterable&lt;Member[]&gt; over contents of o1 and o2
         */
        protected abstract Iterable<Member[]> makeIterable(Object o1, Object o2);

        /**
         * Derived classes implement depending upon the types of parameter
         * o1 and o2.
         *
         * @param o1 Member or Member[]
         * @param o2 Member or Member[]
         * @return combining o1 and o2 into Member[]
         */
        protected abstract Member[] makeNext(Object o1, Object o2);

        protected Iterable<Member[]> makeIterableIterable(
            final Iterable it1,
            final Iterable it2)
        {
            // There is no knowledge about how large either it1 ore it2
            // are or how many null members they might have, so all
            // one can do is iterate across them:
            // iterate across it1 and for each member iterate across it2
            Iterable<Member[]> iterable = new Iterable<Member[]>() {
                public Iterator<Member[]> iterator() {
                    return new Iterator<Member[]>() {
                        Iterator i1 = it1.iterator();
                        Object o1 = null;
                        Iterator i2 = it2.iterator();
                        Object o2 = null;
                        public boolean hasNext() {
                            if (o2 != null) {
                                return true;
                            }
                            if (! hasNextO1()) {
                                return false;
                            }
                            if (! hasNextO2()) {
                                 o1 = null;
                                // got to end of i2, get next m1
                                if (! hasNextO1()) {
                                    return false;
                                }
                                // reset i2
                                i2 = it2.iterator();
                                if (! hasNextO2()) {
                                    return false;
                                }
                            }
                            return true;
                        }
                        public Member[] next() {
                            try {
                                return makeNext(o1, o2);
                            } finally {
                                o2 = null;
                            }
                        }
                        public void remove() {
                            throw new UnsupportedOperationException("remove");
                        }

                        private boolean hasNextO1() {
                            while (o1 == null) {
                                if (! i1.hasNext()) {
                                    return false;
                                }
                                o1 = i1.next();
                            }
                            return true;
                        }
                        private boolean hasNextO2() {
                            o2 = null;
                            while (o2 == null) {
                                if (! i2.hasNext()) {
                                    return false;
                                }
                                o2 = i2.next();
                            }
                            return true;
                        }
                    };
                }
            };

            return iterable;
        }

        protected Iterable<Member[]> makeIterableList(
            final Iterable it1,
            final List l2)
        {
            Iterable<Member[]> iterable = new Iterable<Member[]>() {
                public Iterator<Member[]> iterator() {
                    return new Iterator<Member[]>() {
                        Iterator i1 = it1.iterator();
                        Object o1 = null;
                        int index2 = 0;
                        Object o2 = null;
                        public boolean hasNext() {
                            if (o2 != null) {
                                return true;
                            }
                            if (! hasNextO1()) {
                                return false;
                            }
                            if (! hasNextO2()) {
                                 o1 = null;
                                // got to end of l2, get next m1
                                if (! hasNextO1()) {
                                    return false;
                                }
                                // reset l2
                                index2 = 0;
                                if (! hasNextO2()) {
                                    return false;
                                }
                            }
                            return true;
                        }
                        public Member[] next() {
                            try {
                                return makeNext(o1, o2);
                            } finally {
                                o2 = null;
                            }
                        }
                        public void remove() {
                            throw new UnsupportedOperationException("remove");
                        }

                        private boolean hasNextO1() {
                            while (o1 == null) {
                                if (! i1.hasNext()) {
                                    return false;
                                }
                                o1 = i1.next();
                            }
                            return true;
                        }
                        private boolean hasNextO2() {
                            o2 = null;
                            while (o2 == null) {
                                if (index2 == l2.size()) {
                                    return false;
                                }
                                o2 = l2.get(index2++);
                            }
                            return true;
                        }
                    };
                }
            };
            return iterable;
        }

        protected Iterable<Member[]> makeListIterable(
            final List l1,
            final Iterable it2)
        {
            Iterable<Member[]> iterable = new Iterable<Member[]>() {
                public Iterator<Member[]> iterator() {
                    return new Iterator<Member[]>() {
                        int index1 = 0;
                        Object o1 = null;
                        Iterator i2 = it2.iterator();
                        Object o2 = null;
                        public boolean hasNext() {
                            if (o2 != null) {
                                return true;
                            }
                            if (! hasNextO1()) {
                                return false;
                            }
                            if (! hasNextO2()) {
                                 o1 = null;
                                // got to end of i2, get next o1
                                if (! hasNextO1()) {
                                    return false;
                                }
                                // reset i2
                                i2 = it2.iterator();
                                if (! hasNextO2()) {
                                    return false;
                                }
                            }
                            return true;
                        }
                        public Member[] next() {
                            try {
                                return makeNext(o1, o2);
                            } finally {
                                o2 = null;
                            }
                        }
                        public void remove() {
                            throw new UnsupportedOperationException("remove");
                        }

                        private boolean hasNextO1() {
                            while (o1 == null) {
                                if (index1 == l1.size()) {
                                    return false;
                                }
                                o1 = l1.get(index1++);
                            }
                            return true;
                        }
                        private boolean hasNextO2() {
                            o2 = null;
                            while (o2 == null) {
                                if (! i2.hasNext()) {
                                    return false;
                                }
                                o2 = i2.next();
                            }
                            return true;
                        }
                    };
                }
            };

            return iterable;
        }

        protected Iterable<Member[]> makeListList(
            final List l1,
            final List l2)
        {
            Iterable<Member[]> iterable = new Iterable<Member[]>() {
                public Iterator<Member[]> iterator() {
                    return new Iterator<Member[]>() {
                        int index1 = 0;
                        Object o1 = null;
                        int index2 = 0;
                        Object o2 = null;
                        public boolean hasNext() {
                            if (o2 != null) {
                                return true;
                            }
                            if (! hasNextO1()) {
                                return false;
                            }
                            if (! hasNextO2()) {
                                 o1 = null;
                                // got to end of i2, get next o1
                                if (! hasNextO1()) {
                                    return false;
                                }
                                // reset i2
                                index2 = 0;
                                if (! hasNextO2()) {
                                    return false;
                                }
                            }
                            return true;
                        }
                        public Member[] next() {
                            try {
                                return makeNext(o1, o2);
                            } finally {
                                o2 = null;
                            }
                        }
                        public void remove() {
                            throw new UnsupportedOperationException("remove");
                        }

                        private boolean hasNextO1() {
                            while (o1 == null) {
                                if (index1 == l1.size()) {
                                    return false;
                                }
                                o1 = l1.get(index1++);
                            }
                            return true;
                        }
                        private boolean hasNextO2() {
                            o2 = null;
                            while (o2 == null) {
                                if (index2 == l2.size()) {
                                    return false;
                                }
                                o2 = l2.get(index2++);
                            }
                            return true;
                        }
                    };
                }
            };
            return iterable;
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    // Member Member
    abstract class BaseMemberMemberIterCalc
            extends BaseIterCalc {
        BaseMemberMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Member[] makeNext(Object o1, Object o2) {
            return new Member[] {(Member) o1, (Member) o2};
        }
    }

    // Member Member[]
    abstract class BaseMemberMemberArrayIterCalc
                    extends BaseIterCalc {
        BaseMemberMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Member[] makeNext(Object o1, Object o2) {
            Member m1 = (Member) o1;
            Member[] ma2 = (Member[]) o2;
            Member[] ma = new Member[ma2.length+1];
            ma[0] = m1;
            System.arraycopy(ma2, 0, ma, 1, ma2.length);
            return ma;
        }
    }

    // Member[] Member
    abstract class BaseMemberArrayMemberIterCalc
                    extends BaseIterCalc {
        BaseMemberArrayMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Member[] makeNext(Object o1, Object o2) {
            Member[] ma1 = (Member[]) o1;
            Member m2 = (Member) o2;
            Member[] ma = new Member[ma1.length+1];
            System.arraycopy(ma1, 0, ma, 0, ma1.length);
            ma[ma1.length] = m2;
            return ma;
        }
    }

    // Member[] Member[]
    abstract class BaseMemberArrayMemberArrayIterCalc
                    extends BaseIterCalc {
        BaseMemberArrayMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Member[] makeNext(Object o1, Object o2) {
            Member[] ma1 = (Member[]) o1;
            Member[] ma2 = (Member[]) o2;
            Member[] ma = new Member[ma1.length+ma2.length];
            System.arraycopy(ma1, 0, ma, 0, ma1.length);
            System.arraycopy(ma2, 0, ma, ma1.length, ma2.length);
            return ma;
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    // ITERABLE Member ITERABLE Member
    class IterMemberIterMemberIterCalc
            extends BaseMemberMemberIterCalc {
        IterMemberIterMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            Iterable<Member> it1 = (Iterable<Member>) o1;
            Iterable<Member> it2 = (Iterable<Member>) o2;
            return makeIterableIterable(it1, it2);
        }
    }

    // ITERABLE Member LIST Member
    class IterMemberListMemberIterCalc
            extends BaseMemberMemberIterCalc {
        IterMemberListMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            Iterable<Member> it1 = (Iterable<Member>) o1;
            List<Member> l2 = (List<Member>) o2;

            if (l2 instanceof RandomAccess) {
                // direct access faster
                return makeIterableList(it1, l2);
            } else {
                // iteration faster
                return makeIterableIterable(it1, l2);
            }
        }
    }
    // LIST Member ITERABLE Member
    class ListMemberIterMemberIterCalc
            extends BaseMemberMemberIterCalc {
        ListMemberIterMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            List<Member> l1 = (List<Member>) o1;
            Iterable<Member> it2 = (Iterable<Member>) o2;

            if (l1 instanceof RandomAccess) {
                // direct access faster
                return makeListIterable(l1, it2);
            } else {
                // iteration faster
                return makeIterableIterable(l1, it2);
            }
        }
    }

    // LIST Member LIST Member
    class ListMemberListMemberIterCalc
            extends BaseMemberMemberIterCalc {
        ListMemberListMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            List<Member> l1 = (List<Member>) o1;
            List<Member> l2 = (List<Member>) o2;

            if (l1 instanceof RandomAccess) {
                // l1 direct access faster
                if (l2 instanceof RandomAccess) {
                    // l2 direct access faster
                    return makeListList(l1, l2);
                } else {
                    // l2 iteration faster
                    return makeListIterable(l1, l2);
                }
            } else {
                // l1 iteration faster
                if (l2 instanceof RandomAccess) {
                    // l2 direct access faster
                    return makeIterableList(l1, l2);
                } else {
                    // l2 iteration faster
                    return makeIterableIterable(l1, l2);
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    // ITERABLE Member ITERABLE Member[]
    class IterMemberIterMemberArrayIterCalc
                extends BaseMemberMemberArrayIterCalc {
        IterMemberIterMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            Iterable<Member> it1 = (Iterable<Member>) o1;
            Iterable<Member[]> it2 = (Iterable<Member[]>) o2;
            return makeIterableIterable(it1, it2);
        }
    }

    // ITERABLE Member LIST Member[]
    class IterMemberListMemberArrayIterCalc
                extends BaseMemberMemberArrayIterCalc {
        IterMemberListMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            Iterable<Member> it1 = (Iterable<Member>) o1;
            List<Member[]> l2 = (List<Member[]>) o2;

            if (l2 instanceof RandomAccess) {
                // direct access faster
                return makeIterableList(it1, l2);
            } else {
                // iteration faster
                return makeIterableIterable(it1, l2);
            }
        }
    }

    // LIST Member ITERABLE Member[]
    class ListMemberIterMemberArrayIterCalc
                extends BaseMemberMemberArrayIterCalc {
        ListMemberIterMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            List<Member> l1 = (List<Member>) o1;
            Iterable<Member[]> it2 = (Iterable<Member[]>) o2;

            if (l1 instanceof RandomAccess) {
                // direct access faster
                return makeListIterable(l1, it2);
            } else {
                // iteration faster
                return makeIterableIterable(l1, it2);
            }
        }
    }

    // LIST Member LIST Member[]
    class ListMemberListMemberArrayIterCalc
                extends BaseMemberMemberArrayIterCalc {
        ListMemberListMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            List<Member> l1 = (List<Member>) o1;
            List<Member[]> l2 = (List<Member[]>) o2;

            if (l1 instanceof RandomAccess) {
                // l1 direct access faster
                if (l2 instanceof RandomAccess) {
                    // l2 direct access faster
                    return makeListList(l1, l2);
                } else {
                    // l2 iteration faster
                    return makeListIterable(l1, l2);
                }
            } else {
                // l1 iteration faster
                if (l2 instanceof RandomAccess) {
                    // l2 direct access faster
                    return makeIterableList(l1, l2);
                } else {
                    // l2 iteration faster
                    return makeIterableIterable(l1, l2);
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    // ITERABLE Member[] ITERABLE Member
    class IterMemberArrayIterMemberIterCalc
                extends BaseMemberArrayMemberIterCalc {
        IterMemberArrayIterMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            Iterable<Member[]> it1 = (Iterable<Member[]>) o1;
            Iterable<Member> it2 = (Iterable<Member>) o2;
            return makeIterableIterable(it1, it2);
        }
    }

    // ITERABLE Member[] LIST Member
    class IterMemberArrayListMemberIterCalc
                extends BaseMemberArrayMemberIterCalc {
        IterMemberArrayListMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            Iterable<Member[]> it1 = (Iterable<Member[]>) o1;
            List<Member> l2 = (List<Member>) o2;

            if (l2 instanceof RandomAccess) {
                // direct access faster
                return makeIterableList(it1, l2);
            } else {
                // iteration faster
                return makeIterableIterable(it1, l2);
            }
        }
    }

    // LIST Member[] ITERABLE Member
    class ListMemberArrayIterMemberIterCalc
                extends BaseMemberArrayMemberIterCalc {
        ListMemberArrayIterMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            List<Member[]> l1 = (List<Member[]>) o1;
            Iterable<Member> it2 = (Iterable<Member>) o2;

            if (l1 instanceof RandomAccess) {
                // direct access faster
                return makeListIterable(l1, it2);
            } else {
                // iteration faster
                return makeIterableIterable(l1, it2);
            }
        }
    }

    // LIST Member[] LIST Member
    class ListMemberArrayListMemberIterCalc
                extends BaseMemberArrayMemberIterCalc {
        ListMemberArrayListMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            List<Member[]> l1 = (List<Member[]>) o1;
            List<Member> l2 = (List<Member>) o2;

            if (l1 instanceof RandomAccess) {
                // l1 direct access faster
                if (l2 instanceof RandomAccess) {
                    // l2 direct access faster
                    return makeListList(l1, l2);
                } else {
                    // l2 iteration faster
                    return makeListIterable(l1, l2);
                }
            } else {
                // l1 iteration faster
                if (l2 instanceof RandomAccess) {
                    // l2 direct access faster
                    return makeIterableList(l1, l2);
                } else {
                    // l2 iteration faster
                    return makeIterableIterable(l1, l2);
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    // ITERABLE Member[] ITERABLE Member[]
    class IterMemberArrayIterMemberArrayIterCalc
                extends BaseMemberArrayMemberArrayIterCalc {
        IterMemberArrayIterMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            Iterable<Member[]> it1 = (Iterable<Member[]>) o1;
            Iterable<Member[]> it2 = (Iterable<Member[]>) o2;
            return makeIterableIterable(it1, it2);
        }
    }

    // ITERABLE Member[] LIST Member[]
    class IterMemberArrayListMemberArrayIterCalc
                extends BaseMemberArrayMemberArrayIterCalc {
        IterMemberArrayListMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            Iterable<Member[]> it1 = (Iterable<Member[]>) o1;
            List<Member[]> l2 = (List<Member[]>) o2;

            if (l2 instanceof RandomAccess) {
                // direct access faster
                return makeIterableList(it1, l2);
            } else {
                // iteration faster
                return makeIterableIterable(it1, l2);
            }
        }
    }

    // LIST Member[] ITERABLE Member[]
    class ListMemberArrayIterMemberArrayIterCalc
                extends BaseMemberArrayMemberArrayIterCalc {
        ListMemberArrayIterMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            List<Member[]> l1 = (List<Member[]>) o1;
            Iterable<Member[]> it2 = (Iterable<Member[]>) o2;

            if (l1 instanceof RandomAccess) {
                // direct access faster
                return makeListIterable(l1, it2);
            } else {
                // iteration faster
                return makeIterableIterable(l1, it2);
            }
        }
    }

    // LIST Member[] LIST Member[]
    class ListMemberArrayListMemberArrayIterCalc
                extends BaseMemberArrayMemberArrayIterCalc {
        ListMemberArrayListMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable<Member[]> makeIterable(Object o1, Object o2) {
            List<Member[]> l1 = (List<Member[]>) o1;
            List<Member[]> l2 = (List<Member[]>) o2;

            if (l1 instanceof RandomAccess) {
                // l1 direct access faster
                if (l2 instanceof RandomAccess) {
                    // l2 direct access faster
                    return makeListList(l1, l2);
                } else {
                    // l2 iteration faster
                    return makeListIterable(l1, l2);
                }
            } else {
                // l1 iteration faster
                if (l2 instanceof RandomAccess) {
                    // l2 direct access faster
                    return makeIterableList(l1, l2);
                } else {
                    // l2 iteration faster
                    return makeIterableIterable(l1, l2);
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    // Immutable List
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    protected ListCalc compileCallImmutableList(final ResolvedFunCall call,
            ExpCompiler compiler) {
        final ListCalc listCalc1 = toList(compiler, call.getArg(0));
        final ListCalc listCalc2 = toList(compiler, call.getArg(1));
        Calc[] calcs = new Calc[] {listCalc1, listCalc2};
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: LIST or MUTABLE_LIST.
        // Since we want an immutable list as the result, it does not
        // matter whether the Calc list are of type
        // LIST and MUTABLE_LIST - they are treated the same; so
        // there are 4 possible combinations - even sweeter.

        // Check returned calc ResultStyles
        checkListResultStyles(listCalc1);
        checkListResultStyles(listCalc2);

        if (isMemberType(listCalc1)) {
            // Member
            if (isMemberType(listCalc2)) {
                // Member
                return new ImmutableListMemberListMemberListCalc(call, calcs);
            } else {
                // Member[]
                return new ImmutableListMemberListMemberArrayListCalc(call, calcs);
            }
        } else {
            // Member[]
            if (isMemberType(listCalc2)) {
                // Member
                return new ImmutableListMemberArrayListMemberListCalc(call, calcs);
            } else {
                // Member[]
                return new ImmutableListMemberArrayListMemberArrayListCalc(call, calcs);
            }
        }
    }

    private ListCalc toList(ExpCompiler compiler, final Exp exp) {
        // Want immutable list or mutable list in that order
        // It is assumed that an immutable list is easier to get than
        // a mutable list.
        final Type type = exp.getType();
        if (type instanceof SetType) {
            return (ListCalc) compiler.compile(exp,
                ExpCompiler.LIST_MUTABLE_LIST_RESULT_STYLE_ARRAY);
        } else {
            return new SetFunDef.ListSetCalc(
                    new DummyExp(new SetType(type)),
                    new Exp[] {exp},
                    compiler,
                    ExpCompiler.LIST_MUTABLE_LIST_RESULT_STYLE_ARRAY
                    );
        }
    }

    abstract class BaseListCalc extends AbstractListCalc {
        protected BaseListCalc(ResolvedFunCall call,
                    Calc[] calcs,
                    boolean mutable) {
            super(call, calcs, mutable);
        }
        public List<Member[]> evaluateList(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                schemaReader.getNativeSetEvaluator(
                    call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (List) nativeEvaluator.execute(
                            ResultStyle.LIST);
            }

            Calc[] calcs = getCalcs();
            ListCalc listCalc1 = (ListCalc) calcs[0];
            ListCalc listCalc2 = (ListCalc) calcs[1];

            Evaluator oldEval = null;
            assert (oldEval = evaluator.push()) != null;

            List l1 = listCalc1.evaluateList(evaluator);
            assert oldEval.equals(evaluator) : "listCalc1 changed context";

            List l2 = listCalc2.evaluateList(evaluator);
            assert oldEval.equals(evaluator) : "listCalc2 changed context";

            //l1 = checkList(evaluator, l1);
            l1 = nonEmptyOptimizeList(evaluator, l1, call);
            if (l1.isEmpty()) {
                return Collections.EMPTY_LIST;
            }
            //l2 = checkList(evaluator, l2);
            l2 = nonEmptyOptimizeList(evaluator, l2, call);
            if (l2.isEmpty()) {
                return Collections.EMPTY_LIST;
            }

            return makeList(l1, l2);
        }

        protected abstract List<Member[]> makeList(List l1, List l2);
    }

    public abstract class BaseImmutableList
                            extends UnsupportedList<Member[]> {
        protected BaseImmutableList() {
        }
        public abstract int size();
        public abstract Member[] get(int index);

        public Object[] toArray() {
            int size = size();
            Object[] result = new Object[size];
            for (int i = 0; i < size; i++) {
                result[i] = get(i);
            }
            return result;
        }
        public List<Member[]> toArrayList() {
            List<Member[]> l = new ArrayList<Member[]>(size());
            Iterator i = iterator();
            while (i.hasNext()) {
                l.add((Member[]) i.next());
            }
            return l;
        }
        public ListIterator<Member[]> listIterator() {
            return new ListItr(0);
        }
        public ListIterator<Member[]> listIterator(int index) {
            return new ListItr(index);
        }
        public Iterator<Member[]> iterator() {
            return new Itr();
        }
    }

    // LIST Member LIST Member
    class ImmutableListMemberListMemberListCalc
            extends BaseListCalc {
        ImmutableListMemberListMemberListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs, false);
        }
        protected List<Member[]> makeList(final List l1, final List l2) {
            final int size = l1.size() * l2.size();
            // This is the mythical "local class" declaration.
            // Observer that in the subList method, there is another
            // such class declaration. The outer one can not be an
            // anonymous class because
            // the inner one must reference, have a name for, the
            // outer one. The inner one is needed because it includes
            // the offset into the outer one as instance variables.
            // The outer class has no explicit instance variables
            // though it does have the implicit List finals, l1 and l2.
            // One can call the inner class's subList method repeatedly
            // and each new Inner object return adds an additional
            // "fromIndex" to the "get" method calls.
            //
            // All of this works because the underlying lists are
            // immutable.
            //
            class Outer extends BaseImmutableList {
                Outer() {}
                public int size() {
                    return size;
                }
                public Member[] get(int index) {
                    int i = (index / l2.size());
                    int j = (index % l2.size());
                    Member m1 = (Member) l1.get(i);
                    Member m2 = (Member) l2.get(j);
                    return new Member[] { m1, m2 };
                }
                public List<Member[]> subList(int fromIndex, int toIndex) {
                    class Inner extends Outer {
                        int fromIndex;
                        int toIndex;
                        Inner(int fromIndex, int toIndex) {
                            this.fromIndex = fromIndex;
                            this.toIndex = toIndex;
                        }
                        public int size() {
                            return (this.toIndex - this.fromIndex);
                        }
                        public Member[] get(int index) {
                            return Outer.this.get(index + this.fromIndex);
                        }
                        public List<Member[]> subList(int fromIndex, int toIndex) {
                            return new Inner(this.fromIndex+fromIndex, this.fromIndex+toIndex);
                        }
                    }
                    return new Inner(fromIndex, toIndex);
                }
            };
            return new Outer();
        }
    }

    // LIST Member LIST Member[]
    class ImmutableListMemberListMemberArrayListCalc
            extends BaseListCalc {
        ImmutableListMemberListMemberArrayListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs, false);
        }
        protected List<Member[]> makeList(final List l1, final List l2) {
            final int len2 = ((Member[])l2.get(0)).length;
            final int size = (l1.size() * l2.size());
            class Outer extends BaseImmutableList {
                Outer() {}
                public int size() {
                    return size;
                }
                public Member[] get(int index) {
                    int i = (index / l2.size());
                    int j = (index % l2.size());
                    Member[] ma = new Member[1 + len2];
                    Member m1 = (Member) l1.get(i);
                    Member[] ma2 = (Member[]) l2.get(j);
                    ma[0] = m1;
                    System.arraycopy(ma2, 0, ma, 1, len2);
                    return ma;
                }
                public List<Member[]> subList(int fromIndex, int toIndex) {
                    class Inner extends Outer {
                        int fromIndex;
                        int toIndex;
                        Inner(int fromIndex, int toIndex) {
                            this.fromIndex = fromIndex;
                            this.toIndex = toIndex;
                        }
                        public int size() {
                            return (this.toIndex - this.fromIndex);
                        }
                        public Member[] get(int index) {
                            return Outer.this.get(index + this.fromIndex);
                        }
                        public List<Member[]> subList(int fromIndex, int toIndex) {
                            return new Inner(this.fromIndex+fromIndex, this.fromIndex+toIndex);
                        }
                    }
                    return new Inner(fromIndex, toIndex);
                }
            };
            return new Outer();
        }
    }
    // LIST Member[] LIST Member
    class ImmutableListMemberArrayListMemberListCalc
            extends BaseListCalc {
        ImmutableListMemberArrayListMemberListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs, false);
        }
        protected List<Member[]> makeList(final List l1, final List l2) {
            final int len1 = ((Member[])l1.get(0)).length;
            final int size = (l1.size() * l2.size());
            class Outer extends BaseImmutableList {
                Outer() {}
                public int size() {
                    return size;
                }
                public Member[] get(int index) {
                    int i = (index / l2.size());
                    int j = (index % l2.size());
                    Member[] ma = new Member[len1 + 1];
                    Member[] ma1 = (Member[]) l1.get(i);
                    Member m2 = (Member) l2.get(j);
                    System.arraycopy(ma1, 0, ma, 0, len1);
                    ma[len1] = m2;
                    return ma;
                }
                public List<Member[]> subList(int fromIndex, int toIndex) {
                    class Inner extends Outer {
                        int fromIndex;
                        int toIndex;
                        Inner(int fromIndex, int toIndex) {
                            this.fromIndex = fromIndex;
                            this.toIndex = toIndex;
                        }
                        public int size() {
                            return (this.toIndex - this.fromIndex);
                        }
                        public Member[] get(int index) {
                            return Outer.this.get(index + this.fromIndex);
                        }
                        public List<Member[]> subList(int fromIndex, int toIndex) {
                            return new Inner(this.fromIndex+fromIndex, this.fromIndex+toIndex);
                        }
                    }
                    return new Inner(fromIndex, toIndex);
                }
            }
            return new Outer();
        }
    }
    // LIST Member[] LIST Member[]
    class ImmutableListMemberArrayListMemberArrayListCalc
            extends BaseListCalc {
        ImmutableListMemberArrayListMemberArrayListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs, false);
        }
        protected List<Member[]> makeList(final List l1, final List l2) {
            final int len1 = ((Member[])l1.get(0)).length;
            final int len2 = ((Member[])l2.get(0)).length;
            final int size = (l1.size() * l2.size());

            class Outer extends BaseImmutableList {
                Outer() {}
                public int size() {
                    return size;
                }
                public Member[] get(int index) {
                    int i = (index / l2.size());
                    int j = (index % l2.size());
                    Member[] ma = new Member[len1 + len2];
                    Member[] ma1 = (Member[]) l1.get(i);
                    Member[] ma2 = (Member[]) l2.get(j);
                    System.arraycopy(ma1, 0, ma, 0, len1);
                    System.arraycopy(ma2, 0, ma, len1, len2);
                    return ma;
                }
                public List<Member[]> subList(int fromIndex, int toIndex) {
                    class Inner extends Outer {
                        int fromIndex;
                        int toIndex;
                        Inner(int fromIndex, int toIndex) {
                            this.fromIndex = fromIndex;
                            this.toIndex = toIndex;
                        }
                        public int size() {
                            return (this.toIndex - this.fromIndex);
                        }
                        public Member[] get(int index) {
                            return Outer.this.get(index + this.fromIndex);
                        }
                        public List<Member[]> subList(int fromIndex, int toIndex) {
                            return new Inner(this.fromIndex+fromIndex, this.fromIndex+toIndex);
                        }
                    }
                    return new Inner(fromIndex, toIndex);
                }
            }
            return new Outer();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    // Mutable List
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    protected ListCalc compileCallMutableList(final ResolvedFunCall call,
            ExpCompiler compiler) {
        final ListCalc listCalc1 = toList(compiler, call.getArg(0));
        final ListCalc listCalc2 = toList(compiler, call.getArg(1));

        Calc[] calcs = new Calc[] {listCalc1, listCalc2};
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: LIST or MUTABLE_LIST.
        // Since we want an mutable list as the result, it does not
        // matter whether the Calc list are of type
        // LIST and MUTABLE_LIST - they are treated the same,
        // regardless of type, one must materialize the result list; so
        // there are 4 possible combinations - even sweeter.

        // Check returned calc ResultStyles
        checkListResultStyles(listCalc1);
        checkListResultStyles(listCalc2);

        if (isMemberType(listCalc1)) {
            // Member
            if (isMemberType(listCalc2)) {
                // Member
                return new MutableListMemberListMemberListCalc(call, calcs);
            } else {
                // Member[]
                return new MutableListMemberListMemberArrayListCalc(call, calcs);
            }
        } else {
            // Member[]
            if (isMemberType(listCalc2)) {
                // Member
                return new MutableListMemberArrayListMemberListCalc(call, calcs);
            } else {
                // Member[]
                return new MutableListMemberArrayListMemberArrayListCalc(call, calcs);
            }
        }
    }

    /**
     * A BaseMutableList can be sorted, its elements rearranged, but
     * its size can not be changed (the add or remove methods are not
     * supported).
     */
    public abstract class BaseMutableList
                            extends UnsupportedList<Member[]> {
        protected final Member[] members;
        protected BaseMutableList(Member[] members) {
            this.members = members;
        }
        public abstract int size();
        public abstract Member[] get(int index);
        public abstract Member[] set(int index, Member[] element);
        public abstract Member[] remove(int index);
        public abstract List<Member[]> subList(int fromIndex, int toIndex);

        public Object[] toArray() {
            int size = size();
            Object[] result = new Object[size];
            for (int i = 0; i < size; i++) {
                result[i] = get(i);
            }
            return result;
        }
        public List<Member[]> toArrayList() {
            List<Member[]> l = new ArrayList<Member[]>(size());
            Iterator i = iterator();
            while (i.hasNext()) {
                l.add((Member[]) i.next());
            }
            return l;
        }
        public ListIterator<Member[]> listIterator() {
            return new LocalListItr(0);
        }
        public ListIterator<Member[]> listIterator(int index) {
            return new LocalListItr(index);
        }
        public Iterator<Member[]> iterator() {
            return new LocalItr();
        }
        private class LocalItr extends Itr {
            public LocalItr() {
                super();
            }
            public void remove() {
                if (lastRet == -1) {
                    throw new IllegalStateException();
                }
                //checkForComodification();

                try {
                    CrossJoinFunDef.BaseMutableList.this.remove(lastRet);
                    if (lastRet < cursor) {
                        cursor--;
                    }
                    lastRet = -1;
                    //expectedModCount = modCount;
                } catch(IndexOutOfBoundsException e) {
                    throw new ConcurrentModificationException();
                }
            }
        }
        private class LocalListItr extends ListItr {
            public LocalListItr(int index) {
                super(index);
            }
            public void set(Member[] o) {
                if (lastRet == -1)
                    throw new IllegalStateException();
                try {
                    CrossJoinFunDef.BaseMutableList.this.set(lastRet, o);
                } catch(IndexOutOfBoundsException e) {
                    throw new ConcurrentModificationException();
                }
            }
        }

    }

    // LIST Member LIST Member
    class MutableListMemberListMemberListCalc
            extends BaseListCalc {
        MutableListMemberListMemberListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs, true);
        }
        protected List<Member[]> makeList(final List l1, final List l2) {
            int size1 = l1.size();
            // len1 == 1
            int size2 = l2.size();
            // len2 == 1
            int arraySize = (2 * (size1 * size2));

            Member[] members = new Member[arraySize];
            for (int i = 0; i < size1; i++) {
                Member m1 = (Member) l1.get(i);
                int ii = i*size2;
                for (int j = 0; j < size2; j++) {
                    Member m2 = (Member) l2.get(j);
                    members[2*(ii + j)] = m1;
                    members[2*(ii + j)+1] = m2;
                }
            }
            return makeList(members);
        }
        protected List<Member[]> makeList(Member[] members) {
            // externally looks like:
            //  [] <- [a][A]
            //  [] <- [a][B]
            //  ...
            //  [] <- [m][N]
            //
            // but internally is:
            //  [a][A][a][B] ... [m][M][m][N]
            List<Member[]> list = new BaseMutableList(members) {
                int size = members.length/2;
                public int size() {
                    return size;
                }
                public Member[] get(int index) {
                    int i = index+index;
                    return new Member[] { members[i], members[i+1] };
                }
                public Member[] set(int index, Member[] element) {
                    int i = index+index;
                    Member[] oldValue =
                        new Member[] { members[i], members[i+1] };

                    members[i] = element[0];
                    members[i+1] = element[1];

                    return oldValue;
                }
                public Member[] remove(int index) {
                    int i = index+index;
                    Member[] oldValue =
                        new Member[] { members[i], members[i+1] };

                    System.arraycopy(members, i+2, members, i,
                            members.length - (i+2));

                    size--;
                    return oldValue;
                }
                public List<Member[]> subList(int fromIndex, int toIndex) {
                    int from = fromIndex + fromIndex;
                    int to = toIndex + toIndex;
                    Member[] sublist = new Member[to - from];
                    System.arraycopy(members, from, sublist, 0, to - from);
                    return makeList(sublist);
                }
            };
            return list;
        }
    }

    // LIST Member LIST Member[]
    class MutableListMemberListMemberArrayListCalc
            extends BaseListCalc {
        MutableListMemberListMemberArrayListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs, true);
        }
        protected List<Member[]> makeList(final List l1, final List l2) {
            int size1 = l1.size();
            // len1 == 1
            int size2 = l2.size();
            int len2 = ((Member[])l2.get(0)).length;
            int totalLen = 1+len2;
            int arraySize = (totalLen * (size1 * size2));

            Member[] members = new Member[arraySize];
            for (int i = 0; i < size1; i++) {
                Member m1 = (Member) l1.get(i);
                int ii = i*size2;
                for (int j = 0; j < size2; j++) {
                    Member[] ma2 = (Member[]) l2.get(j);
                    members[totalLen*(ii + j)] = m1;
                    for (int k = 0; k < len2; k++) {
                        Member m2 = (Member) ma2[k];
                        members[totalLen*(ii + j)+k+1] = m2;
                    }
                }
            }

            return makeList(members, totalLen);
        }
        protected List<Member[]> makeList(Member[] members, final int totalLen) {
            // l1: a,b
            // l2: {A,B,C},{D,E,F}
            //
            // externally looks like:
            //  [] <- {a,A,B,C}
            //  [] <- {a,D,E,F}
            //  [] <- {b,A,B,C}
            //  [] <- {b,D,E,F}
            //
            // but internally is:
            // a,A,B,C,a,D,E,F,b,A,B,C,b,D,E,F
            List<Member[]> list = new BaseMutableList(members) {
                int size = members.length/totalLen;
                public int size() {
                    return size;
                }
                public Member[] get(int index) {
                    int i = totalLen*index;
                    Member[] ma = new Member[totalLen];
                    System.arraycopy(members, i, ma, 0, totalLen);
                    return ma;
                }
                public Member[] set(int index, Member[] element) {
                    int i = totalLen*index;
                    Member[] oldValue = new Member[totalLen];
                    System.arraycopy(members, i, oldValue, 0, totalLen);

                    System.arraycopy(element, 0, members, i, totalLen);

                    return oldValue;
                }
                public Member[] remove(int index) {
                    int i = totalLen*index;
                    Member[] oldValue = new Member[totalLen];
                    System.arraycopy(members, i, oldValue, 0, totalLen);

                    System.arraycopy(members, i+totalLen,
                            members, i,
                            members.length-(i+totalLen));

                    size--;
                    return oldValue;
                }
                public List<Member[]> subList(int fromIndex, int toIndex) {
                    int from = totalLen*fromIndex;
                    int to = totalLen*toIndex;
                    Member[] sublist = new Member[to - from];
                    System.arraycopy(members, from, sublist, 0, to - from);
                    return makeList(sublist, totalLen);
                }
            };
            return list;
        }
    }
    // LIST Member[] LIST Member
    class MutableListMemberArrayListMemberListCalc
            extends BaseListCalc {
        MutableListMemberArrayListMemberListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs, true);
        }
        protected List<Member[]> makeList(final List l1, final List l2) {
            int size1 = l1.size();
            int len1 = ((Member[])l1.get(0)).length;
            int size2 = l2.size();
            // len2 == 1
            int totalLen = 1+len1;
            int arraySize = (totalLen * (size1 * size2));

            Member[] members = new Member[arraySize];
            for (int i = 0; i < size1; i++) {
                Member[] ma1 = (Member[]) l1.get(i);
                int ii = i*size2;
                for (int j = 0; j < size2; j++) {
                    for (int k = 0; k < len1; k++) {
                        Member m1 = (Member) ma1[k];
                        members[totalLen*(ii + j)+k] = m1;
                    }
                    Member m2 = (Member) l2.get(j);
                    members[totalLen*(ii + j)+len1] = m2;
                }
            }

            return makeList(members, totalLen);
        }
        protected List<Member[]> makeList(Member[] members, final int totalLen) {
            // l1: {A,B,C},{D,E,F}
            // l2: a,b
            //
            // externally looks like:
            //  [] <- {A,B,C,a}
            //  [] <- {A,B,C,b}
            //  [] <- {D,E,F,a}
            //  [] <- {D,E,F,b}
            //
            // but internally is:
            //  A,B,C,a,A,B,C,b,D,E,F,a,D,E,F,b
            List<Member[]> list = new BaseMutableList(members) {
                int size = members.length/totalLen;
                public int size() {
                    return size;
                }
                public Member[] get(int index) {
                    int i = totalLen*index;
                    Member[] ma = new Member[totalLen];
                    System.arraycopy(members, i, ma, 0, totalLen);
                    return ma;
                }
                public Member[] set(int index, Member[] element) {
                    int i = totalLen*index;
                    Member[] oldValue = new Member[totalLen];
                    System.arraycopy(members, i, oldValue, 0, totalLen);

                    System.arraycopy(element, 0, members, i, totalLen);

                    return oldValue;
                }
                public Member[] remove(int index) {
                    int i = totalLen*index;
                    Member[] oldValue = new Member[totalLen];
                    System.arraycopy(members, i, oldValue, 0, totalLen);

                    System.arraycopy(members, i+totalLen,
                            members, i,
                            members.length-(i+totalLen));

                    size--;
                    return oldValue;
                }
                public List<Member[]> subList(int fromIndex, int toIndex) {
                    int from = totalLen*fromIndex;
                    int to = totalLen*toIndex;
                    Member[] sublist = new Member[to - from];
                    System.arraycopy(members, from, sublist, 0, to - from);
                    return makeList(sublist, totalLen);
                }
            };
            return list;
        }
    }
    // LIST Member[] LIST Member[]
    class MutableListMemberArrayListMemberArrayListCalc
            extends BaseListCalc {
        MutableListMemberArrayListMemberArrayListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs, true);
        }
        protected List<Member[]> makeList(final List l1, final List l2) {
            int size1 = l1.size();
            int len1 = ((Member[])l1.get(0)).length;
            int size2 = l2.size();
            int len2 = ((Member[])l2.get(0)).length;
            int totalLen = len1+len2;
            int arraySize = (totalLen * (size1 * size2));

            Member[] members = new Member[arraySize];
            for (int i = 0; i < size1; i++) {
                Member[] ma1 = (Member[]) l1.get(i);
                int ii = i*size2;
                for (int j = 0; j < size2; j++) {
                    for (int k = 0; k < len1; k++) {
                        Member m1 = (Member) ma1[k];
                        members[totalLen*(ii + j)+k] = m1;
                    }
                    Member[] ma2 = (Member[]) l2.get(j);
                    for (int k = 0; k < len2; k++) {
                        Member m2 = (Member) ma2[k];
                        members[totalLen*(ii + j)+len1+k] = m2;
                    }
                }
            }
            return makeList(members, totalLen);
        }
        protected List<Member[]> makeList(Member[] members, final int totalLen) {

            // l1: {A,B,C},{D,E,F}
            // l2: {a,b},{c,d},{e,f}
            //
            // externally looks like:
            //  [] <- {A,B,C,a,b}
            //  [] <- {A,B,C,c,d}
            //  [] <- {A,B,C,e,f}
            //  [] <- {D,E,F,a,b}
            //  [] <- {D,E,F,c,d}
            //  [] <- {D,E,F,e,d}
            //
            // but internally is:
            //  A,B,C,a,b,A,B,C,c,d,A,B,C,e,f,D,E,F,a,b,D,E,F,c,d,D,E,F,e,d
            //
            List<Member[]> list = new BaseMutableList(members) {
                int size = members.length/totalLen;
                public int size() {
                    return size;
                }
                public Member[] get(int index) {
                    int i = totalLen*index;
                    Member[] ma = new Member[totalLen];
                    System.arraycopy(members, i, ma, 0, totalLen);
                    return ma;
                }
                public Member[] set(int index, Member[] element) {
                    int i = totalLen*index;
                    Member[] oldValue = new Member[totalLen];
                    System.arraycopy(members, i, oldValue, 0, totalLen);

                    System.arraycopy(element, 0, members, i, totalLen);

                    return oldValue;
                }

                public Member[] remove(int index) {
                    int i = totalLen*index;
                    Member[] oldValue = new Member[totalLen];
                    System.arraycopy(members, i, oldValue, 0, totalLen);

                    System.arraycopy(members, i+totalLen,
                            members, i,
                            members.length-(i+totalLen));

                    size--;
                    return oldValue;
                }
                public List<Member[]> subList(int fromIndex, int toIndex) {
                    int from = totalLen*fromIndex;
                    int to = totalLen*toIndex;
                    Member[] sublist = new Member[to - from];
                    System.arraycopy(members, from, sublist, 0, to - from);
                    return makeList(sublist, totalLen);
                }
            };
            return list;
        }
    }


    protected List nonEmptyOptimizeList(
            Evaluator evaluator, 
            List list,
            ResolvedFunCall call) {

        int opSize = MondrianProperties.instance().CrossJoinOptimizerSize.get();
        int size = list.size();

        if (size > opSize && evaluator.isNonEmpty()) {
            // instead of overflow exception try to further
            // optimize nonempty(crossjoin(a,b)) ==
            // nonempty(crossjoin(nonempty(a),nonempty(b))
            final int missCount = evaluator.getMissCount();

            list = nonEmptyList(evaluator, list, call);
            size = list.size();
            // list may be empty after nonEmpty optimization
            if (size == 0) {
                return Collections.EMPTY_LIST;
            }
            final int missCount2 = evaluator.getMissCount();
            final int puntMissCountListSize = 1000;
            if (missCount2 > missCount && size > puntMissCountListSize) {
                // We've hit some cells which are not in the cache. They
                // registered as non-empty, but we won't really know until
                // we've populated the cache. The cartesian product is still
                // huge, so let's quit now, and try again after the cache
                // has been loaded.
                // Return an empty list short circuits higher level
                // evaluation poping one all the way to the top.
                return Collections.EMPTY_LIST;
            }
        }
        return list;
    }
    List crossJoin(
        List list1,
        List list2,
        Evaluator evaluator,
        ResolvedFunCall call)
    {
        if (list1.isEmpty() || list2.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        // Optimize nonempty(crossjoin(a,b)) ==
        //  nonempty(crossjoin(nonempty(a),nonempty(b))
        long size = (long)list1.size() * (long)list2.size();
        int resultLimit = MondrianProperties.instance().ResultLimit.get();

        // Throw an exeption, if the size of the crossjoin exceeds the result
        // limit.
        //
        // FIXME: If we're going to apply a NON EMPTY constraint later, it's
        // possible that the ultimate result will be much smaller.
        if (resultLimit > 0 && resultLimit < size) {
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                size, resultLimit);
        }

        // Throw an exception if the crossjoin exceeds a reasonable limit.
        // (Yes, 4 billion is a reasonable limit.)
        if (size > Integer.MAX_VALUE) {
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                size, Integer.MAX_VALUE);
        }

        // Now we can safely cast size to an integer. It still might be very
        // large - which means we're allocating a huge array which we might
        // pare down later by applying NON EMPTY constraints - which is a
        // concern.
        List<Member[]> result = new ArrayList<Member[]>((int) size);

        boolean neitherSideIsTuple = true;
        int arity0 = 1;
        int arity1 = 1;
        if (list1.get(0) instanceof Member[]) {
            arity0 = ((Member[]) list1.get(0)).length;
            neitherSideIsTuple = false;
        }
        if (list2.get(0) instanceof Member[]) {
            arity1 = ((Member[]) list2.get(0)).length;
            neitherSideIsTuple = false;
        }

        if (neitherSideIsTuple) {
            // Simpler routine if we know neither side contains tuples.
            for (Member o0 : (List<Member>) list1) {
                for (Member o1 : (List<Member>) list2) {
                    result.add(new Member[]{o0, o1});
                }
            }
        } else {
            // More complex routine if one or both sides are arrays
            // (probably the product of nested CrossJoins).
            Member[] row = new Member[arity0 + arity1];
            for (int i = 0, m = list1.size(); i < m; i++) {
                int x = 0;
                Object o0 = list1.get(i);
                if (o0 instanceof Member) {
                    row[x++] = (Member) o0;
                } else {
                    assertTrue(o0 instanceof Member[]);
                    final Member[] members = (Member[]) o0;
                    for (Member member : members) {
                        row[x++] = member;
                    }
                }
                for (int j = 0, n = list2.size(); j < n; j++) {
                    Object o1 = list2.get(j);
                    if (o1 instanceof Member) {
                        row[x++] = (Member) o1;
                    } else {
                        assertTrue(o1 instanceof Member[]);
                        final Member[] members = (Member[]) o1;
                        for (Member member : members) {
                            row[x++] = member;
                        }
                    }
                    result.add(row.clone());
                    x = arity0;
                }
            }
        }
        return result;
    }

    /**
     * Visitor which builds a list of all measures referenced in a query,
     * provided the measures don't reference the function call we're trying
     * to evaluate for non-emptiness.
     */
    private static class MeasureVisitor extends MdxVisitorImpl {

        // This set is null unless a measure is found.
        Set<Member> measureSet;
        Set<Member> queryMeasureSet;
        // measures referencing this call should be excluded from the list
        // of measures found
        ResolvedFunCall crossJoinCall;

        MeasureVisitor(
            Set<Member> queryMeasureSet,
            ResolvedFunCall crossJoinCall)
        {
            this.queryMeasureSet = queryMeasureSet;
            this.crossJoinCall = crossJoinCall;
        }

        public Object visit(mondrian.mdx.ParameterExpr parameterExpr) {
            final Parameter parameter = parameterExpr.getParameter();
            final Type type = parameter.getType();
            if (type instanceof mondrian.olap.type.MemberType) {
                final Object value = parameter.getValue();
                if (value instanceof Member) {
                    final Member member = (Member) value;
                    process(member);
                }
            }

            return null;
        }
        public Object visit(mondrian.mdx.MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            process(member);
            return null;
        }
        private void process(final Member member) {
            for (Member measure : queryMeasureSet) {
                if (measure.equals(member)) {
                    if (validMeasure(measure)) {
                        if (measureSet == null) {
                            measureSet = new HashSet<Member>();
                        }
                        measureSet.add(measure);
                        break;
                    }
                }
            }
        }

        /**
         * Determines if a measure should be added to the set of measures
         * that make up the evaluation context for the nonempty cross join.
         * It should not be if it is a calculated measure that references
         * the cross join, unless the cross join itself also references
         * that calculated measure, in which case, we have a recursive call,
         * and an exception is thrown.
         *
         * @param measure measure being examined
         *
         * @return true if the measure should be added
         */
        private boolean validMeasure(Member measure)
        {
            if (measure.isCalculated()) {
                // check if the measure references the crossjoin
                Exp measureExp = measure.getExpression();
                ResolvedFunCallFinder finder =
                    new ResolvedFunCallFinder(crossJoinCall);
                measureExp.accept(finder);
                if (finder.found) {
                    // check if the arguments to the cross join reference
                    // the measure
                    Exp [] args = crossJoinCall.getArgs();
                    for (int i = 0; i < args.length; i++) {
                        Set<Member> measureSet = new HashSet<Member>();
                        measureSet.add(measure);
                        MeasureVisitor measureFinder =
                            new MeasureVisitor(measureSet, null);
                        args[i].accept(measureFinder);
                        measureSet = measureFinder.measureSet;
                        if (measureSet != null && measureSet.size() > 0) {
                            // recursive condition
                            throw FunUtil.newEvalException(null,
                                "Infinite loop detected in " +
                                crossJoinCall.toString());
                        }
                    }
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Visitor class used to locate a resolved function call within an
     * expression
     */
    private static class ResolvedFunCallFinder
        extends MdxVisitorImpl
    {
        private ResolvedFunCall call;
        public boolean found;

        public ResolvedFunCallFinder(ResolvedFunCall call)
        {
            this.call = call;
            found = false;
        }

        public Object visit(ResolvedFunCall funCall)
        {
            if (funCall == call) {
                found = true;
            }
            return null;
        }

        public Object visit(mondrian.mdx.MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            if (member.isCalculated()) {
                Exp memberExp = member.getExpression();
                memberExp.accept(this);
            }
            return null;
        }
    }

    /**
     * This is the entry point to the crossjoin non-empty optimizer code.
     * <p>
     * What one wants to determine is for each individual Member of the input
     * parameter list, a 'List-Member', whether across a slice there is any 
     * data. 
     * <p>
     * But what data?
     * <p>
     * For Members other than those in the list, the 'non-List-Members', 
     * one wants to consider 
     * all data across the scope of these other Members. For instance, if
     * Time is not a List-Member, then one wants to consider data
     * across All Time. Or, if Customer is not a List-Member, then
     * look at data across All Customers. The theory here, is if there
     * is no data for a particular Member of the list where all other
     * Members not part of the list are span their complete hierarchy, then
     * there is certainly no data for Members of that Hierarchy at a
     * more specific Level (more on this below).
     * <p>
     * When a Member that is a non-List-Member is part of a Hierarchy
     * that has an
     * All Member (hasAll="true"), then its very easy to make sure that
     * the All Member is used during the optimization. 
     * If a non-List-Member is part of a Hierarchy that does not have
     * an All Member, then one must, in fact, iterate over all top-level
     * Members of the Hierarchy!!! - otherwise a List-Member might 
     * be excluded because the optimization code was not looking everywhere.
     * <p>
     * Concerning default Members for those Hierarchies for the 
     * non-List-Members, ignore them. What is wanted is either the
     * All Member or one must iterate across all top-level Members, what
     * happens to be the default Member of the Hierarchy is of no relevant.
     * <p>
     * The Measures Hierarchy has special considerations. First, there is
     * no All Measure. But, certainly one need only involve Measures
     * that are actually in the query... yes and no. For Calculated Measures
     * one must also get all of the non-Calculated Measures that make up
     * each Calculated Measure. Thus, one ends up iterating across all
     * Calculated and non-Calculated Measures that are explicitly 
     * mentioned in the query as well as all Calculated and non-Calculated
     * Measures that are used to define the Calculated Measures in 
     * the query. Why all of these? because this represents the total
     * scope of possible Measures that might yield a non-null value
     * for the List-Members and that is what we what to find. It might
     * be a super set, but thats ok; we just do not want to miss anything.
     *
     *
     * For other Members, the default Member is used, but for Measures one
     * should look for that data for all Measures associated with the query, not
     * just one Measure. For a dense dataset this may not be a problem or even
     * apparent, but for a sparse dataset, the first Measure may, in fact, have
     * not data but other Measures associated with the query might.
     * Hence, the solution here is to identify all Measures associated with the
     * query and then for each Member of the list, determine if there is any
     * data iterating across all Measures until non-null data is found or the
     * end of the Measures is reached.
     *
     * @param evaluator evaluator
     * @param list list of members being checked for non-emptiness
     * @param call the cross join function call
     */
    protected List nonEmptyList(
        Evaluator evaluator,
        List list,
        ResolvedFunCall call)
    {
        return nonEmptyListNEW(evaluator, list, call);
        //return nonEmptyListOLD(evaluator, list, call);
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // OLD optimizer
    //
    /////////////////////////////////////////////////////////////////////////
    protected static List nonEmptyListOLD(
        Evaluator evaluator,
        List list,
        ResolvedFunCall call)
    {
        if (list.isEmpty()) {
            return list;
        }

        // A compromise between allocating too much and having lots of allocations.
        // If everything is null, then this is too big, but if nothing is null
        // then this results in TWO calls to ArrayList's ensureCapacity method
        // and its associated System.arraycopy method.
        // What is best?
        // Note that an ArrayList does not have an adjustable "growth factor"
        // but rather grows by 1.5.
        List result = new ArrayList((list.size() + 2) >> 1);

        // Get all Measures
        // RME: The only mechanism I could find for getting all Measures
        // associated with the query was to use a the MdxVisitor to get all
        // MemberExprs and test if its Member was one of the Measures.  First,
        // it might be expected that at query parse-time one could determine
        // what Measures were associated with which axes saving the use of the
        // visitor and, many times, this might be true but 2) if the Measures
        // are dynamically generated, for instance using a function such as
        // StrToSet, then one can not count on visiting the axes' Exp and determining
        // all Measures - they can only be known at execution-time.
        // So, here it is assumed that all Measures are known statically by
        // this stage of the processing.
        Query query = evaluator.getQuery();
        Set<Member> measureSet = null;
        Set<Member> queryMeasureSet = query.getMeasuresMembers();
        // if the slicer contains a Measure, then the other axes can not
        // contain a Measure, so look at slicer axis first
        if (queryMeasureSet.size() > 0) {
            MeasureVisitor visitor =
                new MeasureVisitor(queryMeasureSet, call);
            QueryAxis[] axes = query.getAxes();
            QueryAxis slicerAxis = query.getSlicerAxis();
            if (slicerAxis != null) {
                slicerAxis.accept(visitor);
            }
            if (visitor.measureSet != null) {
                // Slicer had a Measure, 1) use it and 2) do not need to look at
                // the other axes.
                measureSet = visitor.measureSet;

            } else if (axes.length > 0) {
                for (int i = 0; i < axes.length; i++) {
                    if (axes[i] != null) {
                        axes[i].accept(visitor);
                    }
                }
                // It maybe null, but thats ok here
                measureSet = visitor.measureSet;
            }
        }

        // Determine if there is any data.
        evaluator = evaluator.push();
        if (list.get(0) instanceof Member[]) {
            for (Member[] ms : ((List<Member[]>) list)) {
                evaluator.setContext(ms);
                // no measures found, use standard algorithm
                if (measureSet == null) {
                    Object value = evaluator.evaluateCurrent();
                    if (value != null && !(value instanceof Throwable)) {
                        result.add(ms);
                    }
                } else {
                    Iterator<Member> measureIter = measureSet.iterator();
                    MEASURES_LOOP:
                    while (measureIter.hasNext()) {
                        Member measure = measureIter.next();
                        evaluator.setContext(measure);
                        Object value = evaluator.evaluateCurrent();
                        if (value != null && !(value instanceof Throwable)) {
                            result.add(ms);
                            break MEASURES_LOOP;
                        }
                    }
                }
            }
        } else {
            for (Iterator listItr = list.iterator(); listItr.hasNext();) {
                Member m = (Member) listItr.next();
                evaluator.setContext(m);
                // no measures found, use standard algorithm
                if (measureSet == null) {
                    Object value = evaluator.evaluateCurrent();
                    if (value != null && !(value instanceof Throwable)) {
                        result.add(m);
                    }
                } else {
                    Iterator<Member> measureIter = measureSet.iterator();
                    measuresLoop:
                    while (measureIter.hasNext()) {
                        Member measure = measureIter.next();
                        evaluator.setContext(measure);
                        Object value = evaluator.evaluateCurrent();
                        if (value != null && !(value instanceof Throwable)) {
                            result.add(m);
                            break measuresLoop;
                        }
                    }
                }
            }
        }
        return result;
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // NEW optimizer
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * The MeasureVisitorNEW class traverses the function call tree of
     * the non empty crossjoin function and populates the queryMeasureSet 
     * with base measures
     */
    private static class MeasureVisitorNEW extends MdxVisitorImpl {

        Set<Member> queryMeasureSet;
        //
        // measures referencing this call should be excluded from the list
        // of measures found
        ResolvedFunCall crossJoinCall;

        MeasureVisitorNEW(Set<Member> queryMeasureSet, 
                ResolvedFunCall crossJoinCall) {
            this.queryMeasureSet = queryMeasureSet;
            this.crossJoinCall = crossJoinCall;
        }

        public Object visit(mondrian.mdx.ResolvedFunCall funcall) {
            Exp[] exps = funcall.getArgs();
            if (exps != null) {
                for (Exp exp: exps) {
                    exp.accept(this);
                }
            }
            return null;
        }
        public Object visit(mondrian.mdx.ParameterExpr parameterExpr) {
            final Parameter parameter = parameterExpr.getParameter();
            final Type type = parameter.getType();
            if (type instanceof mondrian.olap.type.MemberType) {
                final Object value = parameter.getValue();
                if (value instanceof Member) {
                    final Member member = (Member) value;
                    process(member);
                }
            }

            return null;
        }
        public Object visit(mondrian.mdx.MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            process(member);
            return null;
        }
        private void process(final Member member) {
            if (member.isMeasure()) {
                if (member.isCalculated()) {
                    Exp exp = member.getExpression();
                    ResolvedFunCallFinder finder =
                        new ResolvedFunCallFinder(crossJoinCall);
                    exp.accept(finder);
                    if (! finder.found) {
                        exp.accept(this);
                        // commented line out to fix bug #1696772
                        // queryMeasureSet.add(member);
                    }
                } else {
                    queryMeasureSet.add(member);
                }
            }
        }
    }
    
    /** 
     * This is a non-optimistic optimizer. What this means is that an
     * element of the input parameter List is only not included in the
     * returned result List if for no combination of Measures, non-All
     * Members (for Hierarchies that have no All Members) and evaluator
     * default Members did the element evaluate to non-null.
     * 
     * @param evaluator The Evaluator
     * @param list The List of elements that are to be determined if there are
     * any non-null.
     * @param call The calling ResolvedFunCall used to determine what Measures
     * to use.
     * @return List of elements from the input parameter list that have
     * evaluated to non-null.
     */
    protected List nonEmptyListNEW(
        Evaluator evaluator,
        List list,
        ResolvedFunCall call)
    {
        if (list.isEmpty()) {
            return list;
        }

        List result = new ArrayList((list.size() + 2) >> 1);

        //
        // Get all of the Measures
        //
        final Query query = evaluator.getQuery();

        final String measureSetKey = "MEASURE_SET-"+ctag;
        Set<Member> measureSet = 
                (Set<Member>) query.getEvalCache(measureSetKey);
        // If not in query cache, then create and place into cache.
        // This information is used for each iteration so it makes
        // sense to create and cache it.
        if (measureSet == null) {
            measureSet = new HashSet<Member>();
            Set<Member> queryMeasureSet = query.getMeasuresMembers();
            MeasureVisitorNEW visitor = new MeasureVisitorNEW(measureSet, call);
            for (Member m : queryMeasureSet) {
                if (m.isCalculated()) {
                    Exp exp = m.getExpression();
                    exp.accept(visitor);
                } else {
                    measureSet.add(m);
                }
            }

            Formula[] formula = query.getFormulas();
            if (formula != null) {
                for (Formula f: formula) {
                    f.accept(visitor);
                }
            }

            query.putEvalCache(measureSetKey, measureSet);
        }

        final String allMemberListKey = "ALL_MEMBER_LIST-"+ctag;
        List<Member> allMemberList = 
                (List<Member>) query.getEvalCache(allMemberListKey);

        final String nonAllMembersKey = "NON_ALL_MEMBERS-"+ctag;
        Member[][] nonAllMembers = 
            (Member[][]) query.getEvalCache(nonAllMembersKey);
        if (nonAllMembers == null) {
            //
            // Get all of the All Members and those Hierarchies that
            // do not have All Members.
            //
            Member[] evalMembers = (Member[]) evaluator.getMembers().clone();

            Member[] listMembers = (list.get(0) instanceof Member[])
                ? (Member[]) list.get(0) 
                : new Member[] { (Member) list.get(0) }; 


            // Remove listMembers from evalMembers    
            for (Member lm : listMembers) {
                Hierarchy h = lm.getHierarchy();
                for (int i = 0; i < evalMembers.length; i++) {
                    Member em = evalMembers[i];
                    if ((em != null) && h.equals(em.getHierarchy())) {
                        evalMembers[i] = null;
                    }
                }
            }

            // Now we have the non-List-Members, but some of them may not be 
            // All Members (default Member need not be the All Member) and
            // for some Hierarchies there may not be an All Member. 
            // So we create an array of Objects some elements of which are 
            // All Members and others elements will be an array of all top-level
            // Members when there is not an All Member.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            allMemberList = new ArrayList<Member>();
            List<Member[]> nonAllMemberList = new ArrayList<Member[]>();
            for (int i = 0, j = 0; i < evalMembers.length; i++) {
                Member em = evalMembers[i];
                if (em == null) {
                    // Above we might have removed some by setting them
                    // to null.
                    continue;
                }
                if (em.isMeasure()) {
                    continue;
                }
                if (em.isCalculated()) {
                    continue;
                }
                // The member is not the All member
                if (! em.isAll()) {
                    Hierarchy h = em.getHierarchy();
                    Member[] rootMembers = schemaReader.getHierarchyRootMembers(h);
                    if (h.hasAll()) {
                        // The Hierarchy has an All member
                        boolean found = false;
                        for (Member m : rootMembers) {
                            if (m.isAll()) {
                                allMemberList.add(m);
                                found = true;
                                break;
                            }
                        }
                        if (! found) {
System.out.println("CrossJoinFunDef.nonEmptyListNEW: ERROR");
                        }
                    } else {
                        // The Hierarchy does NOT have an All member
                        nonAllMemberList.add(rootMembers);
                    }
                }
            }
            nonAllMembers = 
                (Member[][]) nonAllMemberList.toArray(new Member[0][]);

            query.putEvalCache(allMemberListKey, allMemberList);
            query.putEvalCache(nonAllMembersKey, nonAllMembers);
        }

        //
        // Determine if there is any data.
        //
        evaluator = evaluator.push();

        // Put all of the All Members into Evaluator
        evaluator.setContext(allMemberList);

        // Iterate over elements of the input list (whether it contains
        // Member[] or Member elements). If for any combination of
        // Measure and non-All Members evaluation is non-null, then
        // add it to the result List.
        if (list.get(0) instanceof Member[]) {
            for (Member[] ms : ((List<Member[]>) list)) {
                evaluator.setContext(ms);
                if (checkData(nonAllMembers, nonAllMembers.length-1, 
                            measureSet, evaluator)) {
                    result.add(ms);
                }
            }
        } else {
            for (Member m : ((List<Member>) list)) {
                evaluator.setContext(m);
                if (checkData(nonAllMembers, nonAllMembers.length-1, 
                            measureSet, evaluator)) {
                    result.add(m);
                }
            }
        }

        return result;
    }

    /** 
     * Return <code>true</code> if for some combination of Members
     * from the nonAllMembers array of Member arrays and Measures from
     * the Set of Measures evaluate to a non-null value. Even if a
     * particular combination is non-null, all combinations are tested
     * just to make sure that the data is loaded.
     * 
     * @param nonAllMembers array of Member arrays of top-level Members
     * for Hierarchies that have no All Member.
     * @param cnt which Member array is to be processed.
     * @param measureSet Set of all that should be tested against.
     * @param evaluator the Evaluator.
     * @return True if at least one combination evaluated to non-null.
     */
    private static boolean checkData(
            Member[][] nonAllMembers, 
            int cnt,
            Set<Member> measureSet,
            Evaluator evaluator) {

        if (cnt < 0) {
            // no measures found, use standard algorithm
            if (measureSet.isEmpty()) {
                Object value = evaluator.evaluateCurrent();
                if (value != null && !(value instanceof Throwable)) {
                    return true;
                }
            } else {
                // Here we evaluate across all measures just to
                // make sure that the data is all loaded
                boolean found = false;
                for (Member measure : measureSet) {
                    evaluator.setContext(measure);
                    Object value = evaluator.evaluateCurrent();
                    if (value != null && !(value instanceof Throwable)) {
                        found = true;
                    }
                }
                return found;
            }
        } else {
            boolean found = false;
            for (Member m : nonAllMembers[cnt]) {
                evaluator.setContext(m);
                if (checkData(nonAllMembers, cnt-1, measureSet, evaluator)) {
                    found = true;
                }
            }
            return found;
        }
        return false;
    }

    private static class StarCrossJoinResolver extends MultiResolver {
        public StarCrossJoinResolver() {
            super(
                    "*",
                    "<Set1> * <Set2>",
                    "Returns the cross product of two sets.",
                    new String[]{"ixxx", "ixmx", "ixxm", "ixmm"});
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount) {
            // This function only applies in contexts which require a set.
            // Elsewhere, "*" is the multiplication operator.
            // This means that [Measures].[Unit Sales] * [Gender].[M] is
            // well-defined.
            if (validator.requiresExpression()) {
                return null;
            }
            return super.resolve(args, validator, conversionCount);
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new CrossJoinFunDef(dummyFunDef);
        }
    }


}

// End CrossJoinFunDef.java
