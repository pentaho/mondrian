/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.DelegatingExpCompiler;
import mondrian.calc.impl.GenericCalc;

import java.util.*;
import java.io.StringWriter;
import java.io.PrintWriter;

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
 * @version $Id$
 */
public class RolapDependencyTestingEvaluator extends RolapEvaluator {

    /**
     * Creates an evaluator.
     */
    RolapDependencyTestingEvaluator(RolapResult result, int expDeps) {
        super(new DteRoot(result, expDeps));
    }

    /**
     * Creates a child evaluator.
     */
    private RolapDependencyTestingEvaluator(
            RolapEvaluatorRoot root,
            RolapDependencyTestingEvaluator evaluator) {
        super(root, evaluator);
    }

    public Object evaluate(
            Calc calc,
            Dimension[] independentDimensions,
            String mdxString) {
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

        // Change one of the allegedly independent dimensions and evaluate
        // again.
        //
        // Don't do it if the faking is disabled,
        // or if we're already faking another dimension,
        // or if we're filtering out nonempty cells (which makes us
        // dependent on everything),
        // or if the ratio of fake evals to real evals is too high (which
        // would make us too slow).
        if (dteRoot.disabled ||
                dteRoot.faking ||
                isNonEmpty() ||
                (double) dteRoot.fakeCallCount >
                (double) dteRoot.callCount * dteRoot.random.nextDouble() *
                2 * dteRoot.expDeps) {
            return result;
        }
        if (independentDimensions.length == 0) {
            return result;
        }
        dteRoot.faking = true;
        ++dteRoot.fakeCount;
        ++dteRoot.fakeCallCount;
        final int i = dteRoot.random.nextInt(independentDimensions.length);
        final Member saveMember = getContext(independentDimensions[i]);
        final Member otherMember =
                dteRoot.chooseOtherMember(
                        saveMember, getQuery().getSchemaReader(false));
        setContext(otherMember);
        final Object otherResult = calc.evaluate(this);
        if (false) {
            System.out.println(
                    "original=" + saveMember.getUniqueName() +
                    ", member=" + otherMember.getUniqueName() +
                    ", originalResult=" + result + "" +
                    ", result=" + otherResult);
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
                    "Expression '" + mdxString +
                    "' claims to be independent of dimension " +
                    saveMember.getDimension() + " but is not; context is {" +
                    buf.toString() + "}; First result: " +
                    toString(result) + ", Second result: " +
                    toString(otherResult));
        }
        // Restore context.
        setContext(saveMember);
        dteRoot.faking = false;
        return result;
    }

    public RolapEvaluator _push() {
        return new RolapDependencyTestingEvaluator(root, this);
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
            if (o2 instanceof List) {
                return equals(
                        ((List) o1).toArray(),
                        ((List) o2).toArray());
            }
            return false;
        }
        return o1.equals(o2);
    }

    private String toString(Object o) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        toString(o, pw);
        return sw.toString();
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
        final RolapResult result;
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
            this.result = result;
        }

        /**
         * Chooses another member of the same hierarchy.
         * The member will come from all levels with the same probability.
         * Calculated members are not included.
         *
         * @param save
         * @param schemaReader
         */
        private Member chooseOtherMember(
                final Member save, SchemaReader schemaReader) {
            final Hierarchy hierarchy = save.getHierarchy();
            int attempt = 0;
            while (true) {
                // Choose a random level.
                final Level[] levels = hierarchy.getLevels();
                final int levelDepth = random.nextInt(levels.length) + 1;
                Member member = null;
                for (int i = 0; i < levelDepth; i++) {
                    Member[] members;
                    if (i == 0) {
                        members = schemaReader.getLevelMembers(levels[i], false);
                    } else {
                        members = schemaReader.getMemberChildren(member);
                    }
                    if (members.length == 0) {
                        break;
                    }
                    member = members[random.nextInt(members.length)];
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
     * Expression which checks dependencies and list immutability.
     */
    private static class DteCalcImpl extends GenericCalc {
        private final Calc calc;
        private final Dimension[] independentDimensions;
        private final boolean mutableList;
        private final String mdxString;

        DteCalcImpl(
                Calc calc,
                Dimension[] independentDimensions,
                boolean mutableList,
                String mdxString) {
            super(new DummyExp(calc.getType()));
            this.calc = calc;
            this.independentDimensions = independentDimensions;
            this.mutableList = mutableList;
            this.mdxString = mdxString;
        }

        public Calc[] getCalcs() {
            return new Calc[] {calc};
        }

        public Object evaluate(Evaluator evaluator) {
            RolapDependencyTestingEvaluator dtEval =
                    (RolapDependencyTestingEvaluator) evaluator;
            return dtEval.evaluate(calc, independentDimensions, mdxString);
        }

        public List evaluateList(Evaluator evaluator) {
            List list = super.evaluateList(evaluator);
            if (!mutableList) {
                list = Collections.unmodifiableList(list);
            }
            return list;
        }

        public ExpCompiler.ResultStyle getResultStyle() {
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
            Dimension[] dimensions = getIndependentDimensions(calc);
            calc = super.afterCompile(exp, calc, mutable);
            return new DteCalcImpl(
                    calc,
                    dimensions,
                    mutable,
                    Util.unparse(exp));
        }

        /**
         * Returns the dimensions an expression depends on.
         */
        private Dimension[] getIndependentDimensions(Calc calc) {
            List<Dimension> indDimList = new ArrayList<Dimension>();
            final Dimension[] dims =
                getValidator().getQuery().getCube().getDimensions();
            for (Dimension dim : dims) {
                if (!calc.dependsOn(dim)) {
                    indDimList.add(dim);
                }
            }
            return indDimList.toArray(new Dimension[indDimList.size()]);
        }
    }
}

// End RolapDependencyTestingEvaluator.java
