/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.*;

/**
 * Abstract base class for definitions of linear regression functions.
 *
 * @see InterceptFunDef
 * @see PointFunDef
 * @see R2FunDef
 * @see SlopeFunDef
 * @see VarianceFunDef
 *
 * <h2>Correlation coefficient</h2>
 * <p><i>Correlation coefficient</i></p>
 *
 * <p>The correlation coefficient, r, ranges from -1 to  + 1. The
 * nonparametric Spearman correlation coefficient, abbreviated rs, has
 * the same range.</p>
 *
 * <table border="1" cellpadding="6" cellspacing="0">
 *   <tr>
 *     <td>Value of r (or rs)</td>
 *     <td>Interpretation</td>
 *   </tr>
 *   <tr>
 *     <td valign="top">r= 0</td>
 *
 *     <td>The two variables do not vary together at all.</td>
 *   </tr>
 *   <tr>
 *     <td valign="top">0 &gt; r &gt; 1</td>
 *     <td>
 *       <p>The two variables tend to increase or decrease together.</p>
 *     </td>
 *   </tr>
 *   <tr>
 *     <td valign="top">r = 1.0</td>
 *     <td>
 *       <p>Perfect correlation.</p>
 *     </td>
 *   </tr>
 *
 *   <tr>
 *     <td valign="top">-1 &gt; r &gt; 0</td>
 *     <td>
 *       <p>One variable increases as the other decreases.</p>
 *     </td>
 *   </tr>
 *
 *   <tr>
 *     <td valign="top">r = -1.0</td>
 *     <td>
 *       <p></p>
 *       <p>Perfect negative or inverse correlation.</p>
 *     </td>
 *   </tr>
 * </table>
 *
 * <p>If r or rs is far from zero, there are four possible explanations:</p>
 * <p>The X variable helps determine the value of the Y variable.</p>
 * <ul>
 *   <li>The Y variable helps determine the value of the X variable.
 *   <li>Another variable influences both X and Y.
 *   <li>X and Y don't really correlate at all, and you just
 *       happened to observe such a strong correlation by chance. The P value
 *       determines how often this could occur.
 * </ul>
 * <p><i>r2 </i></p>
 *
 * <p>Perhaps the best way to interpret the value of r is to square it to
 * calculate r2. Statisticians call this quantity the coefficient of
 * determination, but scientists call it r squared. It is has a value
 * that ranges from zero to one, and is the fraction of the variance in
 * the two variables that is shared. For example, if r2=0.59, then 59% of
 * the variance in X can be explained by variation in Y. &nbsp;Likewise,
 * 59% of the variance in Y can be explained by (or goes along with)
 * variation in X. More simply, 59% of the variance is shared between X
 * and Y.</p>
 *
 * <p>(<a href="http://www.graphpad.com/articles/interpret/corl_n_linear_reg/correlation.htm">Source</a>).
 *
 * <p>Also see: <a href="http://mathworld.wolfram.com/LeastSquaresFitting.html">least squares fitting</a>.
 */


public abstract class LinReg extends FunDefBase {
    /** Code for the specific function. */
    final int regType;

    public static final int Point = 0;
    public static final int R2 = 1;
    public static final int Intercept = 2;
    public static final int Slope = 3;
    public static final int Variance = 4;

    static final Resolver InterceptResolver =
        new ReflectiveMultiResolver(
            "LinRegIntercept",
            "LinRegIntercept(<Set>, <Numeric Expression>[, <Numeric Expression>])",
            "Calculates the linear regression of a set and returns the value of b in the regression line y = ax + b.",
            new String[]{"fnxn", "fnxnn"},
            InterceptFunDef.class);

    static final Resolver PointResolver =
        new ReflectiveMultiResolver(
            "LinRegPoint",
            "LinRegPoint(<Numeric Expression>, <Set>, <Numeric Expression>[, <Numeric Expression>])",
            "Calculates the linear regression of a set and returns the value of y in the regression line y = ax + b.",
            new String[]{"fnnxn", "fnnxnn"},
            PointFunDef.class);

    static final Resolver SlopeResolver =
        new ReflectiveMultiResolver(
            "LinRegSlope",
            "LinRegSlope(<Set>, <Numeric Expression>[, <Numeric Expression>])",
            "Calculates the linear regression of a set and returns the value of a in the regression line y = ax + b.",
            new String[]{"fnxn", "fnxnn"},
            SlopeFunDef.class);

    static final Resolver R2Resolver =
        new ReflectiveMultiResolver(
            "LinRegR2",
            "LinRegR2(<Set>, <Numeric Expression>[, <Numeric Expression>])",
            "Calculates the linear regression of a set and returns R2 (the coefficient of determination).",
            new String[]{"fnxn", "fnxnn"},
            R2FunDef.class);

    static final Resolver VarianceResolver =
        new ReflectiveMultiResolver(
            "LinRegVariance",
            "LinRegVariance(<Set>, <Numeric Expression>[, <Numeric Expression>])",
            "Calculates the linear regression of a set and returns the variance associated with the regression line y = ax + b.",
            new String[]{"fnxn", "fnxnn"},
            VarianceFunDef.class);


    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final DoubleCalc yCalc = compiler.compileDouble(call.getArg(1));
        final DoubleCalc xCalc =
            call.getArgCount() > 2
            ? compiler.compileDouble(call.getArg(2))
            : new ValueCalc(call);
        return new LinRegCalc(call, listCalc, yCalc, xCalc, regType);
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // Helper
    //
    /////////////////////////////////////////////////////////////////////////
    static class Value {
        private List xs;
        private List ys;
        /**
         * The intercept for the linear regression model. Initialized
         * following a call to accuracy.
         */
        double intercept;

        /**
         * The slope for the linear regression model. Initialized following a
         * call to accuracy.
         */
        double slope;

         /** the coefficient of determination */
        double rSquared = Double.MAX_VALUE;

        /** variance = sum square diff mean / n - 1 */
        double variance = Double.MAX_VALUE;

        Value(double intercept, double slope, List xs, List ys) {
            this.intercept = intercept;
            this.slope = slope;
            this.xs = xs;
            this.ys = ys;
        }

        public double getIntercept() {
            return this.intercept;
        }

        public double getSlope() {
            return this.slope;
        }

        public double getRSquared() {
            return this.rSquared;
        }

        /**
         * strength of the correlation
         *
         * @param rSquared Strength of the correlation
         */
        public void setRSquared(double rSquared) {
            this.rSquared = rSquared;
        }

        public double getVariance() {
            return this.variance;
        }

        public void setVariance(double variance) {
            this.variance = variance;
        }

        public String toString() {
            return "LinReg.Value: slope of "
                + slope
                + " and an intercept of " + intercept
                + ". That is, y="
                + intercept
                + (slope > 0.0 ? " +" : " ")
                + slope
                + " * x.";
        }
    }

    /**
     * Definition of the <code>LinRegIntercept</code> MDX function.
     *
     * <p>Synopsis:
     *
     * <blockquote><code>LinRegIntercept(&lt;Numeric Expression&gt;,
     * &lt;Set&gt;, &lt;Numeric Expression&gt;[, &lt;Numeric
     * Expression&gt;])</code></blockquote>
     */
    public static class InterceptFunDef extends LinReg {
        public InterceptFunDef(FunDef funDef) {
            super(funDef, Intercept);
        }
    }

    /**
     * Definition of the <code>LinRegPoint</code> MDX function.
     *
     * <p>Synopsis:
     *
     * <blockquote><code>LinRegPoint(&lt;Numeric Expression&gt;,
     * &lt;Set&gt;, &lt;Numeric Expression&gt;[, &lt;Numeric
     * Expression&gt;])</code></blockquote>
     */
    public static class PointFunDef extends LinReg {
        public PointFunDef(FunDef funDef) {
            super(funDef, Point);
        }

        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            final DoubleCalc xPointCalc =
                compiler.compileDouble(call.getArg(0));
            final ListCalc listCalc = compiler.compileList(call.getArg(1));
            final DoubleCalc yCalc = compiler.compileDouble(call.getArg(2));
            final DoubleCalc xCalc =
                call.getArgCount() > 3
                ? compiler.compileDouble(call.getArg(3))
                : new ValueCalc(call);
            return new PointCalc(
                call, xPointCalc, listCalc, yCalc, xCalc);
        }
    }

    private static class PointCalc extends AbstractDoubleCalc {
        private final DoubleCalc xPointCalc;
        private final ListCalc listCalc;
        private final DoubleCalc yCalc;
        private final DoubleCalc xCalc;

        public PointCalc(
            ResolvedFunCall call,
            DoubleCalc xPointCalc,
            ListCalc listCalc,
            DoubleCalc yCalc,
            DoubleCalc xCalc)
        {
            super(call, new Calc[]{xPointCalc, listCalc, yCalc, xCalc});
            this.xPointCalc = xPointCalc;
            this.listCalc = listCalc;
            this.yCalc = yCalc;
            this.xCalc = xCalc;
        }

        public double evaluateDouble(Evaluator evaluator) {
            double xPoint = xPointCalc.evaluateDouble(evaluator);
            Value value = process(evaluator, listCalc, yCalc, xCalc);
            if (value == null) {
                return FunUtil.DoubleNull;
            }
            // use first arg to generate y position
            double yPoint =
                xPoint * value.getSlope()
                + value.getIntercept();
            return yPoint;
        }
    }

    /**
     * Definition of the <code>LinRegSlope</code> MDX function.
     *
     * <p>Synopsis:
     *
     * <blockquote><code>LinRegSlope(&lt;Numeric Expression&gt;,
     * &lt;Set&gt;, &lt;Numeric Expression&gt;[, &lt;Numeric
     * Expression&gt;])</code></blockquote>
     */
    public static class SlopeFunDef extends LinReg {
        public SlopeFunDef(FunDef funDef) {
            super(funDef, Slope);
        }
    }

    /**
     * Definition of the <code>LinRegR2</code> MDX function.
     *
     * <p>Synopsis:
     *
     * <blockquote><code>LinRegR2(&lt;Numeric Expression&gt;,
     * &lt;Set&gt;, &lt;Numeric Expression&gt;[, &lt;Numeric
     * Expression&gt;])</code></blockquote>
     */
    public static class R2FunDef extends LinReg {
        public R2FunDef(FunDef funDef) {
            super(funDef, R2);
        }
    }

    /**
     * Definition of the <code>LinRegVariance</code> MDX function.
     *
     * <p>Synopsis:
     *
     * <blockquote><code>LinRegVariance(&lt;Numeric Expression&gt;,
     * &lt;Set&gt;, &lt;Numeric Expression&gt;[, &lt;Numeric
     * Expression&gt;])</code></blockquote>
     */
    public static class VarianceFunDef extends LinReg {
        public VarianceFunDef(FunDef funDef) {
            super(funDef, Variance);
        }
    }

    protected static void debug(String type, String msg) {
        // comment out for no output
// RME
        //System.out.println(type + ": " +msg);
    }


    protected LinReg(FunDef funDef, int regType) {
        super(funDef);
        this.regType = regType;
    }

    protected static LinReg.Value process(
        Evaluator evaluator,
        ListCalc listCalc,
        DoubleCalc yCalc,
        DoubleCalc xCalc)
    {
        final int savepoint = evaluator.savepoint();
        TupleList members;
        try {
            evaluator.setNonEmpty(false);
            members = listCalc.evaluateList(evaluator);
        } finally {
            evaluator.restore(savepoint);
        }
        SetWrapper[] sws;
        try {
            sws =
                evaluateSet(
                    evaluator, members, new DoubleCalc[] {yCalc, xCalc});
        } finally {
            evaluator.restore(savepoint);
        }
        SetWrapper swY = sws[0];
        SetWrapper swX = sws[1];

        if (swY.errorCount > 0) {
            debug("LinReg.process", "ERROR error(s) count ="  + swY.errorCount);
            // TODO: throw exception
            return null;
        } else if (swY.v.size() == 0) {
            return null;
        }

        return linearReg(swX.v, swY.v);
    }

    public static LinReg.Value accuracy(LinReg.Value value) {
        // for variance
        double sumErrSquared = 0.0;

        double sumErr = 0.0;

        // for r2
        // data
        double sumSquaredY = 0.0;
        double sumY = 0.0;
        // predicted
        double sumSquaredYF = 0.0;
        double sumYF = 0.0;

        // Obtain the forecast values for this model
        List yfs = forecast(value);

        // Calculate the Sum of the Absolute Errors
        Iterator ity = value.ys.iterator();
        Iterator ityf = yfs.iterator();
        while (ity.hasNext()) {
            // Get next data point
            Double dy = (Double) ity.next();
            if (dy == null) {
                continue;
            }
            Double dyf = (Double) ityf.next();
            if (dyf == null) {
                continue;
            }

            double y = dy.doubleValue();
            double yf = dyf.doubleValue();

            // Calculate error in forecast, and update sums appropriately

            // the y residual or error
            double error = yf - y;

            sumErr += error;
            sumErrSquared += error * error;

            sumY += y;
            sumSquaredY += (y * y);

            sumYF =+ yf;
            sumSquaredYF =+ (yf * yf);
        }


        // Initialize the accuracy indicators
        int n = value.ys.size();

        // Variance
        // The estimate the value of the error variance is a measure of
        // variability of the y values about the estimated line.
        // http://home.ubalt.edu/ntsbarsh/Business-stat/opre504.htm
        // s2 = SSE/(n-2) = sum (y - yf)2 /(n-2)
        if (n > 2) {
            double variance = sumErrSquared / (n - 2);

            value.setVariance(variance);
        }

        // R2
        // R2 = 1 - (SSE/SST)
        // SSE = sum square error = Sum((error-MSE)*(error-MSE))
        // MSE = mean error = Sum(error)/n
        // SST = sum square y diff = Sum((y-MST)*(y-MST))
        // MST = mean y = Sum(y)/n
        double MSE = sumErr / n;
        double MST = sumY / n;
        double SSE = 0.0;
        double SST = 0.0;
        ity = value.ys.iterator();
        ityf = yfs.iterator();
        while (ity.hasNext()) {
            // Get next data point
            Double dy = (Double) ity.next();
            if (dy == null) {
                continue;
            }
            Double dyf = (Double) ityf.next();
            if (dyf == null) {
                continue;
            }

            double y = dy.doubleValue();
            double yf = dyf.doubleValue();

            double error = yf - y;
            SSE += (error - MSE) * (error - MSE);
            SST += (y - MST) * (y - MST);
        }
        if (SST != 0.0) {
            double rSquared =  1 - (SSE / SST);

            value.setRSquared(rSquared);
        }


        return value;
    }

    public static LinReg.Value linearReg(List xlist, List ylist) {
        // y and x have same number of points
        int size = ylist.size();
        double sumX = 0.0;
        double sumY = 0.0;
        double sumXX = 0.0;
        double sumXY = 0.0;

        debug("LinReg.linearReg", "ylist.size()=" + ylist.size());
        debug("LinReg.linearReg", "xlist.size()=" + xlist.size());
        int n = 0;
        for (int i = 0; i < size; i++) {
            Object yo = ylist.get(i);
            Object xo = xlist.get(i);
            if ((yo == null) || (xo == null)) {
                continue;
            }
            n++;
            double y = ((Double) yo).doubleValue();
            double x = ((Double) xo).doubleValue();

            debug("LinReg.linearReg", " " + i + " (" + x + "," + y + ")");
            sumX += x;
            sumY += y;
            sumXX += x * x;
            sumXY += x * y;
        }

        double xMean = sumX / n;
        double yMean = sumY / n;

        debug("LinReg.linearReg", "yMean=" + yMean);
        debug(
            "LinReg.linearReg",
            "(n*sumXX - sumX*sumX)=" + (n * sumXX - sumX * sumX));
        // The regression line is the line that minimizes the variance of the
        // errors. The mean error is zero; so, this means that it minimizes the
        // sum of the squares errors.
        double slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
        double intercept = yMean - slope * xMean;

        LinReg.Value value = new LinReg.Value(intercept, slope, xlist, ylist);
        debug("LinReg.linearReg", "value=" + value);

        return value;
    }


    public static List forecast(LinReg.Value value) {
        List yfs = new ArrayList(value.xs.size());

        Iterator it = value.xs.iterator();
        while (it.hasNext()) {
            Double d = (Double) it.next();
            // If the value is missing we still must put a place
            // holder in the y axis, otherwise there is a discontinuity
            // between the data and the fit.
            if (d == null) {
                yfs.add(null);
            } else {
                double x = d.doubleValue();
                double yf = value.intercept + value.slope * x;
                yfs.add(new Double(yf));
            }
        }

        return yfs;
    }

    private static class LinRegCalc extends AbstractDoubleCalc {
        private final ListCalc listCalc;
        private final DoubleCalc yCalc;
        private final DoubleCalc xCalc;
        private final int regType;

        public LinRegCalc(
            ResolvedFunCall call,
            ListCalc listCalc,
            DoubleCalc yCalc,
            DoubleCalc xCalc,
            int regType)
        {
            super(call, new Calc[]{listCalc, yCalc, xCalc});
            this.listCalc = listCalc;
            this.yCalc = yCalc;
            this.xCalc = xCalc;
            this.regType = regType;
        }

        public double evaluateDouble(Evaluator evaluator) {
            Value value = process(evaluator, listCalc, yCalc, xCalc);
            if (value == null) {
                return FunUtil.DoubleNull;
            }
            switch (regType) {
            case Intercept:
                return value.getIntercept();
            case Slope:
                return value.getSlope();
            case Variance:
                return value.getVariance();
            case R2:
                return value.getRSquared();
            default:
            case Point:
                throw Util.newInternal("unexpected value " + regType);
            }
        }
    }
}

// End LinReg.java
