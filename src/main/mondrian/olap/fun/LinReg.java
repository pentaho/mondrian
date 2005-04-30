/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/


package mondrian.olap.fun;

import mondrian.olap.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Abstract base class for definitions of linear regression functions.
 *
 * @see Intercept
 * @see Point
 * @see R2
 * @see Slope
 * @see Variance
 *
 * <h2>Correlation coefficient</h2>
 * <p><i>Correlation coefficient</i></p>
 *
 * <p>The correlation coefficient, r, ranges from -1 to +1. The
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


public abstract class LinReg extends FunkBase {
    /** Expression which yields the current member. */
    final Exp valueFunCall;

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
         * @param rSquared
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
                + (slope>0.0 ? " +" : " ")
                + slope
                + " * x.";
        }
    }


    /////////////////////////////////////////////////////////////////////////
    //
    // Implementations of LinRegXXX
    //
    /////////////////////////////////////////////////////////////////////////
    public static class Intercept extends LinReg {
        public Intercept() {
            super();
        }
        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
debug("LinReg.Intercept.evaluator","TOP");
            LinReg.Value value = process(evaluator, args);
            if (value == null) {
                return Util.nullValue;
            }
            return new Double(value.getIntercept());
        }
    }
    public static class Point extends LinReg {
        public Point() {
            super();
        }
        // <Numeric Expression>,
        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
debug("LinReg.Point.evaluator","TOP");
	    double x = getDoubleArg(evaluator, args, 0).doubleValue();
debug("LinReg.Point.evaluator","x=" +x);

debug("LinReg.Point.evaluator","args.length=" +args.length);
            // remove first arg and pass the rest to init
            Exp[] args2 = new Exp[args.length-1];
            System.arraycopy(args, 1, args2, 0, args.length-1);

debug("LinReg.Point.evaluator","args2.length=" +args2.length);
            // remember: pass in args2!!! NOT args
            LinReg.Value value = process(evaluator, args2);
            if (value == null) {
                return Util.nullValue;
            }

            // use first arg to generate y position
            double y = x * value.getSlope() + value.getIntercept();
debug("LinReg.Point.evaluator","y=" +y);
            return new Double(y);
        }
    }
    public static class Slope extends LinReg {
        public Slope() {
            super();
        }
        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
debug("LinReg.Slope.evaluator","TOP");
            LinReg.Value value = process(evaluator, args);
            if (value == null) {
                return Util.nullValue;
            }
            return new Double(value.getSlope());
        }
    }
    public static class R2 extends LinReg {
        public R2() {
            super();
        }

        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
debug("LinReg.R2.evaluator","TOP");

            LinReg.Value value = accuracy(evaluator, args);
            if (value == null) {
                return Util.nullValue;
            }
            return new Double(value.getRSquared());
        }
    }
    public static class Variance extends LinReg {
        public Variance() {
            super();
        }

        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
debug("LinReg.Variance.evaluator","TOP");

            LinReg.Value value = accuracy(evaluator, args);
            if (value == null) {
                return Util.nullValue;
            }
            return new Double(value.getVariance());
        }
    }

    protected static void debug(String type, String msg) {
        // comment out for no output
        //System.out.println(type + ": " +msg);
    }


    protected LinReg() {
        super();
        valueFunCall = BuiltinFunTable.instance().createValueFunCall();
    }

    public abstract Object evaluate(Evaluator evaluator, Exp[] args);


    // <Set>, <Numeric Expression>[, <Numeric  Expression>]
    protected LinReg.Value accuracy(Evaluator evaluator, Exp[] args) {
        LinReg.Value value = process(evaluator, args);
        if (value == null) {
            return null;
        }

        return accuracy(value);
    }

    // <Set>, <Numeric Expression>[, <Numeric  Expression>]
    protected LinReg.Value process(Evaluator evaluator, Exp[] args) {
        List members = (List) getArg(evaluator, args, 0);
debug("LinReg.process","members.size=" +members.size());
        ExpBase expY = (ExpBase) getArgNoEval(args, 1);
        ExpBase expX = (ExpBase) getArgNoEval(args, 2, valueFunCall);

        evaluator = evaluator.push();

        SetWrapper[] sws = evaluateSet(evaluator, members,
                new ExpBase[] {expY, expX});
        SetWrapper swY = sws[0];
        SetWrapper swX = sws[1];

        if (swY.errorCount > 0) {
debug("LinReg.process","ERROR error(s) count =" +swY.errorCount);
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
            double y = ((Double) ity.next()).doubleValue();
            double yf = ((Double) ityf.next()).doubleValue();

            // Calculate error in forecast, and update sums appropriately

            // the y residual or error
            double error = yf - y;

            sumErr += error;
            sumErrSquared += error*error;

            sumY += y;
            sumSquaredY += (y*y);

            sumYF =+ yf;
            sumSquaredYF =+ (yf*yf);
        }


        // Initialize the accuracy indicators
        int n = value.ys.size();

        // Variance
        // The estimate the value of the error variance is a measure of
        // variability of the y values about the estimated line.
        // http://home.ubalt.edu/ntsbarsh/Business-stat/opre504.htm
        // s2 = SSE/(n-2) = sum (y - yf)2 /(n-2)
        if (n > 2) {
            double variance = sumErrSquared / (n-2);

            value.setVariance(variance);
        }

        // R2
        // R2 = 1 - (SSE/SST)
        // SSE = sum square error = Sum( (error-MSE)*(error-MSE) )
        // MSE = mean error = Sum( error )/n
        // SST = sum square y diff = Sum( (y-MST)*(y-MST) )
        // MST = mean y = Sum( y )/n
        double MSE = sumErr/n;
        double MST = sumY/n;
        double SSE = 0.0;
        double SST = 0.0;
        ity = value.ys.iterator();
        ityf = yfs.iterator();
        while (ity.hasNext()) {
            // Get next data point
            double y = ((Double) ity.next()).doubleValue();
            double yf = ((Double) ityf.next()).doubleValue();

            double error = yf - y;
            SSE += (error - MSE)*(error - MSE);
            SST += (y - MST)*(y - MST);
        }
        if (SST != 0.0) {
            double rSquared =  1 - (SSE/SST);

            value.setRSquared(rSquared);
        }


        return value;
    }

    public static LinReg.Value linearReg(List xlist, List ylist) {

        // y and x have same number of points
        int size = ylist.size();
        double sumX = 0.0;
        double sumY = 0.0;
        double sumXX = 0.0;                                                             double sumXY = 0.0;

debug("LinReg.linearReg","ylist.size()=" +ylist.size());
debug("LinReg.linearReg","xlist.size()=" +xlist.size());
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

debug("LinReg.linearReg"," " +i+ " (" +x+ "," +y+ ")");
            sumX += x;
            sumY += y;
            sumXX += x*x;
            sumXY += x*y;
        }

        double xMean = sumX / n;
        double yMean = sumY / n;

debug("LinReg.linearReg", "yMean=" +yMean);
debug("LinReg.linearReg", "(n*sumXX - sumX*sumX)=" +(n*sumXX - sumX*sumX));
        // The regression line is the line that minimizes the variance of the
        // errors. The mean error is zero; so, this means that it minimizes the
        // sum of the squares errors.
        double slope = (n*sumXY - sumX*sumY) / (n*sumXX - sumX*sumX);
        double intercept = yMean - slope*xMean;

        LinReg.Value value = new LinReg.Value(intercept, slope, xlist, ylist);
debug("LinReg.linearReg","value=" +value);

        return value;
    }


    public static List forecast(LinReg.Value value) {
        List yfs = new ArrayList(value.xs.size());

        Iterator it = value.xs.iterator();
        while (it.hasNext()) {
            double x = ((Double) it.next()).doubleValue();
            double yf = value.intercept + value.slope * x;
            yfs.add(new Double(yf));
        }

        return yfs;
    }
}
