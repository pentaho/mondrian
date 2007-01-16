/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ExpCompiler.ResultStyle;
import mondrian.calc.IterCalc;
import mondrian.calc.ListCalc;
import mondrian.calc.BooleanCalc;
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
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class FilterFunDef extends FunDefBase {
    static final FilterFunDef instance = new FilterFunDef();

    private FilterFunDef() {
        super(
                "Filter",
                "Filter(<Set>, <Search Condition>)",
                "Returns the set resulting from filtering a set based on a search condition.",
                "fxxb");
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        ResultStyle[] rs = compiler.getAcceptableResultStyles();
        // What is the desired return type?
        for (int i = 0; i < rs.length; i++) {
            switch (rs[i]) {
            case ITERABLE :
            case ANY :
if (! Util.PreJdk15) {
                // Consumer wants ITERABLE or ANY
                return compileCallIterable(call, compiler);
}
            case MUTABLE_LIST:
            case LIST :
                // Consumer wants MUTABLE_LIST or LIST
                return compileCallList(call, compiler);
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


    /** 
     * Returns an IterCalc. 
     * Here we would like to get either a IterCalc or ListCalc (mutable)
     * from the inner expression. For the IterCalc, its Iterator
     * can be wrapped with another Iterator that filters each element.
     * For the mutable list, remove all members that are filtered.
     * 
     * @param call 
     * @param compiler 
     * @return 
     */
    protected Calc compileCallIterable(final ResolvedFunCall call, 
            ExpCompiler compiler) {
        // want iterable, mutable list or immutable list in that order
        Calc imlcalc = compiler.compile(call.getArg(0),
                    ExpCompiler.ITERABLE_LIST_MUTABLE_LIST_RESULT_STYLE_ARRAY);
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
            List members = lcalc.evaluateList(evaluator);

            // Not mutable, must create new list
            List result = new ArrayList();
            for (int i = 0, count = members.size(); i < count; i++) {
                Member member = (Member) members.get(i);
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
            final Iterable<Member> iter = (Iterable<Member>) 
                    icalc.evaluateIterable(evaluator);

            Iterable result = new Iterable<Member>() {
                public Iterator<Member> iterator() {
                    return new Iterator<Member>() {
                        Iterator<Member> it = iter.iterator();
                        Member m = null;
                        public boolean hasNext() {
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
                            return this.m;
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
            List members = lcalc.evaluateList(evaluator);

            // Not mutable, must create new list
            List result = new ArrayList();
            for (int i = 0, count = members.size(); i < count; i++) {
                Member[] member = (Member[]) members.get(i);
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
            final Iterable<Member[]> iter = (Iterable<Member[]>) 
                    icalc.evaluateIterable(evaluator);
            Iterable result = new Iterable<Member[]>() {
                public Iterator<Member[]> iterator() {
                    return new Iterator<Member[]>() {
                        Iterator<Member[]> it = iter.iterator();
                        Member[] m = null;
                        public boolean hasNext() {
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
                            return this.m;
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


    /** 
     * Returns an ListCalc. 
     * 
     * @param call 
     * @param compiler 
     * @return 
     */
    protected Calc compileCallList(final ResolvedFunCall call, 
            ExpCompiler compiler) {
        Calc ilcalc = compiler.compile(call.getArg(0),
                ExpCompiler.MUTABLE_LIST_LIST_RESULT_STYLE_ARRAY
                );
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
        Calc[] calcs = new Calc[] {ilcalc, bcalc};

        // Note that all of the ListCalc's return will be mutable
        if (((SetType) ilcalc.getType()).getElementType() instanceof MemberType) {
            switch (ilcalc.getResultStyle()) {
            case LIST :
                return new ImMutableMemberListCalc(call, calcs);
            case MUTABLE_LIST :
                return new MutableMemberListCalc(call, calcs);
            }
            throw ResultStyleException.generateBadType(
                new ResultStyle[] {
                    ResultStyle.LIST,
                    ResultStyle.MUTABLE_LIST
                },
                ilcalc.getResultStyle());

        } else {

            switch (ilcalc.getResultStyle()) {
            case LIST :
                return new ImMutableMemberArrayListCalc(call, calcs);
            case MUTABLE_LIST :
                return new MutableMemberArrayListCalc(call, calcs);
            }
            throw ResultStyleException.generateBadType(
                new ResultStyle[] {
                    ResultStyle.LIST,
                    ResultStyle.MUTABLE_LIST
                },
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
                        ResultStyle.MUTABLE_LIST);
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
            List members = lcalc.evaluateList(evaluator);

            // Not mutable, must create new list
            List result = new ArrayList();
            for (int i = 0, count = members.size(); i < count; i++) {
                Member member = (Member) members.get(i);
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
            List members = lcalc.evaluateList(evaluator);

            // Not mutable, must create new list
            List result = new ArrayList();
            for (int i = 0, count = members.size(); i < count; i++) {
                Member[] member = (Member[]) members.get(i);
                evaluator2.setContext(member);
                if (bcalc.evaluateBoolean(evaluator2)) {
                    result.add(member);
                }
            }
            return result;
        }
    }

/*
 TODO: Previous code, remove if new code passes code review
    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final BooleanCalc calc = compiler.compileBoolean(call.getArg(1));
        if (((SetType) listCalc.getType()).getElementType() instanceof MemberType) {
            return new AbstractListCalc(call, new Calc[] {listCalc, calc}) {
                public List evaluateList(Evaluator evaluator) {
                    // Use a native evaluator, if more efficient.
                    // TODO: Figure this out at compile time.
                    SchemaReader schemaReader = evaluator.getSchemaReader();
                    NativeEvaluator nativeEvaluator =
                            schemaReader.getNativeSetEvaluator(
                                    call.getFunDef(), call.getArgs(), evaluator, this);
                    if (nativeEvaluator != null) {
                        return (List) nativeEvaluator.execute();
                    }

                    List members = listCalc.evaluateList(evaluator);
                    List result = new ArrayList();
                    Evaluator evaluator2 = evaluator.push();
                    for (int i = 0, count = members.size(); i < count; i++) {
                        Member member = (Member) members.get(i);
                        evaluator2.setContext(member);
                        if (calc.evaluateBoolean(evaluator2)) {
                            result.add(member);
                        }
                    }
                    return result;
                }

                public boolean dependsOn(Dimension dimension) {
                    return anyDependsButFirst(getCalcs(), dimension);
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc, calc}) {
                public List evaluateList(Evaluator evaluator) {
                    // Use a native evaluator, if more efficient.
                    // TODO: Figure this out at compile time.
                    SchemaReader schemaReader = evaluator.getSchemaReader();
                    NativeEvaluator nativeEvaluator =
                            schemaReader.getNativeSetEvaluator(
                                    call.getFunDef(), call.getArgs(), evaluator, this);
                    if (nativeEvaluator != null) {
                        return (List) nativeEvaluator.execute();
                    }

                    List tupleList = listCalc.evaluateList(evaluator);
                    List result = new ArrayList();
                    Evaluator evaluator2 = evaluator.push();
                    for (int i = 0, count = tupleList.size(); i < count; i++) {
                        Member[] members = (Member []) tupleList.get(i);
                        evaluator2.setContext(members);
                        if (calc.evaluateBoolean(evaluator2)) {
                            result.add(members);
                        }
                    }
                    return result;
                }

                public boolean dependsOn(Dimension dimension) {
                    return anyDependsButFirst(getCalcs(), dimension);
                }
            };
        }
    }
*/
}

// End FilterFunDef.java
