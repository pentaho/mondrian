/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.AbstractIterCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.type.SetType;
import mondrian.olap.type.MemberType;
import mondrian.olap.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Definition of the <code>Filter</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>Filter(&lt;Set&gt;, &lt;Search Condition&gt;)</code></blockquote>
 *
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
        // What is the desired return type?
        for (ResultStyle r : compiler.getAcceptableResultStyles()) {
            switch (r) {
            case ITERABLE:
            case ANY:
                // Consumer wants ITERABLE or ANY
                return compileCallIterable(call, compiler);
            case MUTABLE_LIST:
            case LIST:
                // Consumer wants MUTABLE_LIST or LIST
                return compileCallList(call, compiler);
            }
        }
        throw ResultStyleException.generate(
            ResultStyle.ITERABLE_LIST_MUTABLELIST_ANY,
            compiler.getAcceptableResultStyles());
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
        Calc imlcalc = compiler.compileAs(call.getArg(0),
            null, ResultStyle.ITERABLE_LIST_MUTABLELIST);
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
        Calc[] calcs = new Calc[] {imlcalc, bcalc};

        // check returned calc ResultStyles
        checkIterListResultStyles(imlcalc);

        if (((SetType) imlcalc.getType()).getElementType() instanceof MemberType) {
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

    private static abstract class BaseIterCalc extends AbstractIterCalc {
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
            } else {
                return makeIterable(evaluator);
            }
        }
        protected abstract Iterable makeIterable(Evaluator evaluator);

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }
    //
    // Member Iter Calcs
    //
    private static class MutableMemberIterCalc extends BaseIterCalc {
        MutableMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            List members = lcalc.evaluateList(evaluator);

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
    private static class ImMutableMemberIterCalc extends BaseIterCalc {
        ImMutableMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            List<Member> members = lcalc.evaluateList(evaluator);

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
    private static class IterMemberIterCalc extends BaseIterCalc {
        IterMemberIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            IterCalc icalc = (IterCalc) calcs[0];
            final BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            // This does dynamics, just in time,
            // as needed filtering
            final Iterable<Member> iter = icalc.evaluateIterable(evaluator);

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
    private static class MutableMemberArrayIterCalc extends BaseIterCalc {
        MutableMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            List members = lcalc.evaluateList(evaluator);

            Iterator it = members.iterator();
            while (it.hasNext()) {
                Member[] member = (Member[]) it.next();
                evaluator2.setContext(member);
                if (! bcalc.evaluateBoolean(evaluator2)) {
                    it.remove();
                }
            }
            return members;
        }
    }
    private static class ImMutableMemberArrayIterCalc extends BaseIterCalc {
        ImMutableMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            List<Member[]> members = lcalc.evaluateList(evaluator);

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
    private static class IterMemberArrayIterCalc extends BaseIterCalc {
        IterMemberArrayIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected Iterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            IterCalc icalc = (IterCalc) calcs[0];
            final BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();

            // This does dynamics, just in time,
            // as needed filtering
            final Iterable<Member[]> iter = icalc.evaluateIterable(evaluator);
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
        Calc ilcalc = compiler.compileAs(
            call.getArg(0),
            null, ResultStyle.MUTABLELIST_LIST);
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
        Calc[] calcs = new Calc[] {ilcalc, bcalc};

        // Note that all of the ListCalc's return will be mutable
        if (((SetType) ilcalc.getType()).getElementType() instanceof MemberType) {
            switch (ilcalc.getResultStyle()) {
            case LIST:
                return new ImMutableMemberListCalc(call, calcs);
            case MUTABLE_LIST:
                return new MutableMemberListCalc(call, calcs);
            }
            throw ResultStyleException.generateBadType(
                ResultStyle.MUTABLELIST_LIST,
                ilcalc.getResultStyle());

        } else {
            switch (ilcalc.getResultStyle()) {
            case LIST:
                return new ImMutableMemberArrayListCalc(call, calcs);
            case MUTABLE_LIST:
                return new MutableMemberArrayListCalc(call, calcs);
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
        }
        protected List makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            List members = lcalc.evaluateList(evaluator);

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
    private static class ImMutableMemberListCalc extends BaseListCalc {
        ImMutableMemberListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected List makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            List<Member> members = lcalc.evaluateList(evaluator);

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
    private static class MutableMemberArrayListCalc extends BaseListCalc {
        MutableMemberArrayListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected List makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            List members = lcalc.evaluateList(evaluator);

            Iterator it = members.iterator();
            while (it.hasNext()) {
                Member[] member = (Member[]) it.next();
                evaluator2.setContext(member);
                if (! bcalc.evaluateBoolean(evaluator2)) {
                    it.remove();
                }
            }
            return members;
        }
    }
    private static class ImMutableMemberArrayListCalc extends BaseListCalc {
        ImMutableMemberArrayListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }
        protected List makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];

            final Evaluator evaluator2 = evaluator.push();
            List<Member[]> members = lcalc.evaluateList(evaluator);

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
