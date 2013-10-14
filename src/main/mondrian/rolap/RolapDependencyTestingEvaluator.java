/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.olap.type.SetType;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Evaluator which checks dependencies of expressions.
 *
 * <p>For each expression evaluation, this valuator evaluates each
 * expression more times, and makes sure that the results of the expression
 * are independent of dimensions which the expression claims to be
 * independent of.
 *
 * <p>Since it evaluates each expression twice, it also exposes function
 * implementations which change the context of the evaluator.
 *
 * @author jhyde
 * @since September, 2005
 */
public class RolapDependencyTestingEvaluator extends RolapEvaluator {

    /**
     * Creates an dependency-testing evaluator.
     *
     * @param result Result we are building
     * @param expDeps Number of dependencies to check
     */
    RolapDependencyTestingEvaluator(RolapResult result, int expDeps) {
        super(new DteRoot(result, expDeps));
    }

    /**
     * Creates a child evaluator.
     *
     * @param root Root evaluation context
     * @param evaluator Parent evaluator
     */
    private RolapDependencyTestingEvaluator(
        RolapEvaluatorRoot root,
        RolapDependencyTestingEvaluator evaluator,
        List<List<Member>> aggregationList)
    {
        super(root, evaluator, aggregationList);
    }

    public Object evaluate(
        Calc calc,
        RolapCubeHierarchy[] independentHierarchies,
        String mdxString)
    {
        final DteRoot dteRoot =
                (DteRoot) root;
        if (dteRoot.faking) {
            ++dteRoot.fakeCallCount;
        } else {
            ++dteRoot.callCount;
        }
        // Evaluate the call for real.
        final Object result = calc.evaluate(this);
        if (dteRoot.result.isDirty()) {
            return result;
        }

        // If the result is a list and says that it is mutable, see whether it
        // really is.
        if (calc.getResultStyle() == ResultStyle.MUTABLE_LIST) {
            List<Object> list = (List) result;
            if (list.size() > 0) {
                final Object zeroth = list.get(0);
                list.set(0, zeroth);
            }
        }

        // Change one of the allegedly independent dimensions and evaluate
        // again.
        //
        // Don't do it if the faking is disabled,
        // or if we're already faking another dimension,
        // or if we're filtering out nonempty cells (which makes us
        // dependent on everything),
        // or if the ratio of fake evals to real evals is too high (which
        // would make us too slow).
        if (dteRoot.disabled
            || dteRoot.faking
            || isNonEmpty()
            || (double) dteRoot.fakeCallCount
            > (double) dteRoot.callCount * dteRoot.random.nextDouble()
            * 2 * dteRoot.expDeps)
        {
            return result;
        }
        if (independentHierarchies.length == 0) {
            return result;
        }
        dteRoot.faking = true;
        ++dteRoot.fakeCount;
        ++dteRoot.fakeCallCount;
        final int i = dteRoot.random.nextInt(independentHierarchies.length);
        final Member saveMember = getContext(independentHierarchies[i]);
        final Member otherMember =
            dteRoot.chooseOtherMember(
                saveMember, getQuery().getSchemaReader(false));
        setContext(otherMember);
        final Object otherResult = calc.evaluate(this);
        if (false) {
            System.out.println(
                "original=" + saveMember.getUniqueName()
                + ", member=" + otherMember.getUniqueName()
                + ", originalResult=" + result
                + ", result=" + otherResult);
        }
        if (!equals(otherResult, result)) {
            final Member[] members = getMembers();
            final StringBuilder buf = new StringBuilder();
            for (int j = 0; j < members.length; j++) {
                if (j > 0) {
                    buf.append(", ");
                }
                buf.append(members[j].getUniqueName());
            }
            throw Util.newInternal(
                "Expression '" + mdxString
                + "' claims to be independent of dimension "
                + saveMember.getDimension() + " but is not; context is {"
                + buf.toString() + "}; First result: "
                + toString(result) + ", Second result: "
                + toString(otherResult));
        }
        // Restore context.
        setContext(saveMember);
        dteRoot.faking = false;
        return result;
    }

    public RolapEvaluator _push(List<List<Member>> aggregationList) {
        return new RolapDependencyTestingEvaluator(root, this, aggregationList);
    }

    private boolean equals(Object o1, Object o2) {
        if (o1 == null) {
            return o2 == null;
        }
        if (o2 == null) {
            return false;
        }
        if (o1 instanceof Object[]) {
            if (o2 instanceof Object[]) {
                Object[] a1 = (Object[]) o1;
                Object[] a2 = (Object[]) o2;
                if (a1.length == a2.length) {
                    for (int i = 0; i < a1.length; i++) {
                        if (!equals(a1[i], a2[i])) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }
        if (o1 instanceof List) {
            return o2 instanceof List
                && equals(
                    ((List) o1).toArray(),
                    ((List) o2).toArray());
        }
        if (o1 instanceof Iterable) {
            if (o2 instanceof Iterable) {
                return equals(toList((Iterable) o1), toList((Iterable) o2));
            } else {
                return false;
            }
        }
        return o1.equals(o2);
    }

    private String toString(Object o) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        toString(o, pw);
        return sw.toString();
    }

    private <T> List<T> toList(Iterable<T> iterable) {
        final ArrayList<T> list = new ArrayList<T>();
        for (T t : iterable) {
            list.add(t);
        }
        return list;
    }

    private void toString(Object o, PrintWriter pw) {
        if (o instanceof Object[]) {
            Object[] a = (Object[]) o;
            pw.print("{");
            for (int i = 0; i < a.length; i++) {
                Object o1 = a[i];
                if (i > 0) {
                    pw.print(", ");
                }
                toString(o1, pw);
            }
            pw.print("}");
        } else if (o instanceof List) {
            List list = (List) o;
            toString(list.toArray(), pw);
        } else if (o instanceof Member) {
            Member member = (Member) o;
            pw.print(member.getUniqueName());
        } else {
            pw.print(o);
        }
    }

    /**
     * Holds context for a tree of {@link RolapDependencyTestingEvaluator}.
     */
    static class DteRoot extends RolapResult.RolapResultEvaluatorRoot {
        final int expDeps;
        int callCount;
        int fakeCallCount;
        int fakeCount;
        boolean faking;
        boolean disabled;
        final Random random = Util.createRandom(
            MondrianProperties.instance().TestSeed.get());

        DteRoot(RolapResult result, int expDeps) {
            super(result);
            this.expDeps = expDeps;
        }

        /**
         * Chooses another member of the same hierarchy.
         * The member will come from all levels with the same probability.
         * Calculated members are not included.
         *
         * @param save Previous member
         * @param schemaReader Schema reader
         * @return other member of same hierarchy
         */
        private Member chooseOtherMember(
            final Member save,
            SchemaReader schemaReader)
        {
            final Hierarchy hierarchy = save.getHierarchy();
            int attempt = 0;
            while (true) {
                // Choose a random level.
                final List<Level> levels = Util.cast(hierarchy.getLevelList());
                final int levelDepth = random.nextInt(levels.size()) + 1;
                Member member = null;
                for (int i = 0; i < levelDepth; i++) {
                    List<Member> members;
                    if (i == 0) {
                        members =
                            schemaReader.getLevelMembers(levels.get(i), false);
                    } else {
                        members = schemaReader.getMemberChildren(member);
                    }
                    if (members.size() == 0) {
                        break;
                    }
                    member = members.get(random.nextInt(members.size()));
                }
                // If the member chosen happens to be the same as the original
                // member, try again. Give up after 100 attempts (in case the
                // hierarchy has only one member).
                if (member != save || ++attempt > 100) {
                    return member;
                }
            }
        }
    }

    /**
     * Expression which checks dependencies of an underlying scalar expression.
     */
    private static class DteScalarCalcImpl extends GenericCalc {
        private final Calc calc;
        private final RolapCubeHierarchy[] independentHierarchies;
        private final String mdxString;

        DteScalarCalcImpl(
            Calc calc,
            RolapCubeHierarchy[] independentHierarchies,
            String mdxString)
        {
            super(new DummyExp(calc.getType()));
            this.calc = calc;
            this.independentHierarchies = independentHierarchies;
            this.mdxString = mdxString;
        }

        public Calc[] getCalcs() {
            return new Calc[] {calc};
        }

        public Object evaluate(Evaluator evaluator) {
            RolapDependencyTestingEvaluator dtEval =
                (RolapDependencyTestingEvaluator) evaluator;
            return dtEval.evaluate(calc, independentHierarchies, mdxString);
        }

        public ResultStyle getResultStyle() {
            return calc.getResultStyle();
        }
    }

    /**
     * Expression which checks dependencies and list immutability of an
     * underlying list or iterable expression.
     */
    private static class DteIterCalcImpl extends GenericIterCalc {
        private final Calc calc;
        private final RolapCubeHierarchy[] independentHierarchies;
        private final boolean mutableList;
        private final String mdxString;

        DteIterCalcImpl(
            Calc calc,
            RolapCubeHierarchy[] independentHierarchies,
            boolean mutableList,
            String mdxString)
        {
            super(new DummyExp(calc.getType()));
            this.calc = calc;
            this.independentHierarchies = independentHierarchies;
            this.mutableList = mutableList;
            this.mdxString = mdxString;
        }

        public Calc[] getCalcs() {
            return new Calc[] {calc};
        }

        public Object evaluate(Evaluator evaluator) {
            RolapDependencyTestingEvaluator dtEval =
                    (RolapDependencyTestingEvaluator) evaluator;
            return dtEval.evaluate(calc, independentHierarchies, mdxString);
        }

        public TupleList evaluateList(Evaluator evaluator) {
            TupleList list = super.evaluateList(evaluator);
            if (!mutableList) {
                list = TupleCollections.unmodifiableList(list);
            }
            return list;
        }

        public ResultStyle getResultStyle() {
            return calc.getResultStyle();
        }
    }

    /**
     * Expression compiler which introduces dependency testing.
     *
     * <p>It also checks that the caller does not modify lists unless it has
     * explicitly asked for a mutable list.
     */
    static class DteCompiler extends DelegatingExpCompiler {
        DteCompiler(ExpCompiler compiler) {
            super(compiler);
        }

        protected Calc afterCompile(Exp exp, Calc calc, boolean mutable) {
            RolapCubeHierarchy[] hierarchies = getIndependentHierarchies(calc);
            calc = super.afterCompile(exp, calc, mutable);
            if (calc.getType() instanceof SetType) {
                return new DteIterCalcImpl(
                    calc,
                    hierarchies,
                    mutable,
                    Util.unparse(exp));
            } else {
                return new DteScalarCalcImpl(
                    calc,
                    hierarchies,
                    Util.unparse(exp));
            }
        }

        /**
         * Returns the dimensions an expression does not depend on. If the
         * current member of any of these dimensions changes, the expression
         * will return the same result.
         *
         * @param calc Expression
         * @return List of dimensions that the expression does not depend on
         */
        private RolapCubeHierarchy[] getIndependentHierarchies(Calc calc) {
            List<RolapCubeHierarchy> list = new ArrayList<RolapCubeHierarchy>();
            final RolapCube cube =
                (RolapCube) getValidator().getQuery().getCube();
            for (RolapCubeHierarchy hierarchy : cube.getHierarchyList()) {
                if (!calc.dependsOn(hierarchy)) {
                    list.add(hierarchy);
                }
            }
            return list.toArray(new RolapCubeHierarchy[list.size()]);
        }
    }
}

// End RolapDependencyTestingEvaluator.java
