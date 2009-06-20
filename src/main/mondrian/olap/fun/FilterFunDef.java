/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.AbstractMemberIterCalc;
import mondrian.calc.impl.AbstractTupleIterCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.type.SetType;
import mondrian.olap.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Definition of the <code>Filter</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>Filter(&lt;Set&gt;, &lt;Search
 * Condition&gt;)</code></blockquote>
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class FilterFunDef extends FunDefBase {
    static final FilterFunDef instance = new FilterFunDef();

    private FilterFunDef() {
        super(
            "Filter",
            "Returns the set resulting from filtering a set based on a search condition.",
            "fxxb");
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        // Ignore the caller's priority. We prefer to return iterable, because
        // it makes NamedSet.CurrentOrdinal work.
        final List<ResultStyle> styles = compiler.getAcceptableResultStyles();
        if (styles.contains(ResultStyle.ITERABLE)
            || styles.contains(ResultStyle.ANY))
        {
            return compileCallIterable(call, compiler);
        } else if (styles.contains(ResultStyle.LIST)
            || styles.contains(ResultStyle.MUTABLE_LIST))
        {
            return compileCallList(call, compiler);
        } else {
            throw ResultStyleException.generate(
                ResultStyle.ITERABLE_LIST_MUTABLELIST_ANY,
                styles);
        }
    }

    /**
     * Returns an IterCalc.
     *
     * <p>Here we would like to get either a IterCalc or ListCalc (mutable)
     * from the inner expression. For the IterCalc, its Iterator
     * can be wrapped with another Iterator that filters each element.
     * For the mutable list, remove all members that are filtered.
     *
     * @param call Call
     * @param compiler Compiler
     * @return Implementation of this function call in the Iterable result style
     */
    protected IterCalc compileCallIterable(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        // want iterable, mutable list or immutable list in that order
        Calc imlcalc = compiler.compileAs(
            call.getArg(0), null, ResultStyle.ITERABLE_LIST_MUTABLELIST);
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
        Calc[] calcs = new Calc[] {imlcalc, bcalc};

        // check returned calc ResultStyles
        checkIterListResultStyles(imlcalc);

        if (((SetType) imlcalc.getType()).getArity() == 1) {
            if (imlcalc.getResultStyle() == ResultStyle.ITERABLE) {
                return new IterMemberIterCalc(call, calcs);
            } else if (imlcalc.getResultStyle() == ResultStyle.LIST) {
                return new ImMutableMemberIterCalc(call, calcs);
            } else {
                return new MutableMemberIterCalc(call, calcs);
            }

        } else {
            if (imlcalc.getResultStyle() == ResultStyle.ITERABLE) {
                return new IterMemberArrayIterCalc(call, calcs);
            } else if (imlcalc.getResultStyle() == ResultStyle.LIST) {
                return new ImMutableMemberArrayIterCalc(call, calcs);
            } else {
                return new MutableMemberArrayIterCalc(call, calcs);
            }
        }
    }

    private static abstract class BaseMemberIterCalc
        extends AbstractMemberIterCalc
    {
        protected BaseMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }

        public Iterable<Member> evaluateMemberIterable(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                    schemaReader.getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return Util.castToIterable(
                    nativeEvaluator.execute(
                        ResultStyle.ITERABLE));
            } else {
                return makeIterable(evaluator);
            }
        }

        protected abstract Iterable<Member> makeIterable(Evaluator evaluator);

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }

    private static abstract class BaseTupleIterCalc
        extends AbstractTupleIterCalc
    {
        protected BaseTupleIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }

        public Iterable<Member[]> evaluateTupleIterable(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                    schemaReader.getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (Iterable<Member[]>) nativeEvaluator.execute(
                    ResultStyle.ITERABLE);
            } else {
                return makeIterable(evaluator);
            }
        }

        protected abstract Iterable<Member[]> makeIterable(Evaluator evaluator);

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }

    //
    // Member Iter Calcs
    //
    private static class MutableMemberIterCalc extends BaseMemberIterCalc {
        MutableMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof MemberListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected Iterable<Member> makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            MemberListCalc lcalc = (MemberListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            List<Member> members = lcalc.evaluateMemberList(evaluator);
            // make list mutable
            members = new ArrayList<Member>(members);
            Iterator<Member> it = members.iterator();
            while (it.hasNext()) {
                Member member = it.next();
                evaluator2.setContext(member);
                if (! bcalc.evaluateBoolean(evaluator2)) {
                    it.remove();
                }
            }
            return members;
        }
    }

    private static class ImMutableMemberIterCalc extends BaseMemberIterCalc {
        ImMutableMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof MemberListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected Iterable<Member> makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            MemberListCalc lcalc = (MemberListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            List<Member> members = lcalc.evaluateMemberList(evaluator);

            // Not mutable, must create new list
            List<Member> result = new ArrayList<Member>();
            for (int i = 0, count = members.size(); i < count; i++) {
                Member member = members.get(i);
                evaluator2.setContext(member);
                if (bcalc.evaluateBoolean(evaluator2)) {
                    result.add(member);
                }
            }
            return result;
        }
    }

    private static class IterMemberIterCalc extends BaseMemberIterCalc {
        IterMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof MemberIterCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected Iterable<Member> makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            MemberIterCalc icalc = (MemberIterCalc) calcs[0];
            final BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            // This does dynamics, just in time,
            // as needed filtering
            final Iterable<Member> iter =
                icalc.evaluateMemberIterable(evaluator);

            return new Iterable<Member>() {
                public Iterator<Member> iterator() {
                    return new Iterator<Member>() {
                        Iterator<Member> it = iter.iterator();
                        Member m = null;
                        public boolean hasNext() {
                            if (m != null) {
                                return true;
                            }
                            if (! it.hasNext()) {
                                return false;
                            }
                            this.m = it.next();
                            evaluator2.setContext(this.m);
                            while (! bcalc.evaluateBoolean(evaluator2)) {
                                if (! it.hasNext()) {
                                    return false;
                                }
                                this.m = it.next();
                                evaluator2.setContext(this.m);
                            }
                            return true;
                        }
                        public Member next() {
                            try {
                                return this.m;
                            } finally {
                                this.m = null;
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

    //
    // Member[] Iter Calcs
    //
    private static class MutableMemberArrayIterCalc extends BaseTupleIterCalc {
        MutableMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof TupleListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected Iterable<Member[]> makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            TupleListCalc lcalc = (TupleListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            List<Member[]> members = lcalc.evaluateTupleList(evaluator);

            // make list mutable
            members = new ArrayList<Member[]>(members);
            Iterator<Member[]> it = members.iterator();
            while (it.hasNext()) {
                Member[] member = it.next();
                evaluator2.setContext(member);
                if (! bcalc.evaluateBoolean(evaluator2)) {
                    it.remove();
                }
            }
            return members;
        }
    }

    private static class ImMutableMemberArrayIterCalc
        extends BaseTupleIterCalc
    {
        ImMutableMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof TupleListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected Iterable<Member[]> makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            TupleListCalc lcalc = (TupleListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            List<Member[]> members = lcalc.evaluateTupleList(evaluator);

            // Not mutable, must create new list
            List<Member[]> result = new ArrayList<Member[]>();
            for (int i = 0, count = members.size(); i < count; i++) {
                Member[] member = members.get(i);
                evaluator2.setContext(member);
                if (bcalc.evaluateBoolean(evaluator2)) {
                    result.add(member);
                }
            }
            return result;
        }
    }

    private static class IterMemberArrayIterCalc
        extends BaseTupleIterCalc
    {
        IterMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof TupleIterCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected Iterable<Member[]> makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            TupleIterCalc icalc = (TupleIterCalc) calcs[0];
            final BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);

            // This does dynamics, just in time,
            // as needed filtering
            final Iterable<Member[]> iter =
                icalc.evaluateTupleIterable(evaluator);
            return new Iterable<Member[]>() {
                public Iterator<Member[]> iterator() {
                    return new Iterator<Member[]>() {
                        Iterator<Member[]> it = iter.iterator();
                        Member[] m = null;
                        public boolean hasNext() {
                            if (m != null) {
                                return true;
                            }
                            if (! it.hasNext()) {
                                return false;
                            }
                            this.m = it.next();
                            evaluator2.setContext(this.m);
                            while (! bcalc.evaluateBoolean(evaluator2)) {
                                if (! it.hasNext()) {
                                    return false;
                                }
                                this.m = it.next();
                                evaluator2.setContext(this.m);
                            }
                            return true;
                        }
                        public Member[] next() {
                            try {
                                return this.m;
                            } finally {
                                this.m = null;
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


    /**
     * Returns a ListCalc.
     *
     * @param call Call
     * @param compiler Compiler
     * @return Implementation of this function call in the List result style
     */
    protected ListCalc compileCallList(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        Calc ilcalc = compiler.compileList(call.getArg(0), false);
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
        Calc[] calcs = new Calc[] {ilcalc, bcalc};

        // Note that all of the ListCalc's return will be mutable
        if (((SetType) ilcalc.getType()).getArity() == 1) {
            switch (ilcalc.getResultStyle()) {
            case LIST:
                return new ImmutableMemberListCalc(call, calcs);
            case MUTABLE_LIST:
                return new MutableMemberListCalc(call, calcs);
            }
            throw ResultStyleException.generateBadType(
                ResultStyle.MUTABLELIST_LIST,
                ilcalc.getResultStyle());

        } else {
            switch (ilcalc.getResultStyle()) {
            case LIST:
                return new ImmutableTupleListCalc(call, calcs);
            case MUTABLE_LIST:
                return new MutableTupleListCalc(call, calcs);
            }
            throw ResultStyleException.generateBadType(
                ResultStyle.MUTABLELIST_LIST,
                ilcalc.getResultStyle());
        }
    }

    private static abstract class BaseListCalc extends AbstractListCalc {
        protected BaseListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        public List evaluateList(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                    schemaReader.getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (List) nativeEvaluator.execute(
                        ResultStyle.ITERABLE);
//                return (List) nativeEvaluator.execute(
//                        ResultStyle.MUTABLE_LIST);
            } else {
                return makeList(evaluator);
            }
        }
        protected abstract List makeList(Evaluator evaluator);

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }
    //
    // Member List Calcs
    //
    private static class MutableMemberListCalc extends BaseListCalc {
        MutableMemberListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected List makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            List members = lcalc.evaluateList(evaluator);

            // make list mutable
            members = new ArrayList(members);
            Iterator it = members.iterator();
            while (it.hasNext()) {
                Member member = (Member) it.next();
                evaluator2.setContext(member);
                if (! bcalc.evaluateBoolean(evaluator2)) {
                    it.remove();
                }
            }
            return members;
        }
    }

    private static class ImmutableMemberListCalc extends BaseListCalc {
        ImmutableMemberListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof MemberListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected List makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            MemberListCalc lcalc = (MemberListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            List<Member> members = lcalc.evaluateMemberList(evaluator);

            // Not mutable, must create new list
            List<Member> result = new ArrayList<Member>();
            for (int i = 0, count = members.size(); i < count; i++) {
                Member member = members.get(i);
                evaluator2.setContext(member);
                if (bcalc.evaluateBoolean(evaluator2)) {
                    result.add(member);
                }
            }
            return result;
        }
    }

    //
    // Member[] List Calcs
    //
    private static class MutableTupleListCalc extends BaseListCalc {
        MutableTupleListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof TupleListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected List makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            TupleListCalc lcalc = (TupleListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            List<Member[]> members = lcalc.evaluateTupleList(evaluator);

            // make list mutable
            members = new ArrayList<Member[]>(members);
            Iterator<Member[]> it = members.iterator();
            while (it.hasNext()) {
                Member[] member = it.next();
                evaluator2.setContext(member);
                if (! bcalc.evaluateBoolean(evaluator2)) {
                    it.remove();
                }
            }
            return members;
        }
    }

    private static class ImmutableTupleListCalc extends BaseListCalc {
        ImmutableTupleListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof TupleListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected List makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            TupleListCalc lcalc = (TupleListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push(false);
            List<Member[]> members = lcalc.evaluateTupleList(evaluator);

            // Not mutable, must create new list
            List<Member[]> result = new ArrayList<Member[]>();
            for (int i = 0, count = members.size(); i < count; i++) {
                Member[] member = members.get(i);
                evaluator2.setContext(member);
                if (bcalc.evaluateBoolean(evaluator2)) {
                    result.add(member);
                }
            }
            return result;
        }
    }
}

// End FilterFunDef.java
