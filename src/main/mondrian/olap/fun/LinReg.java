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
 * Base class for an MDX regression analysis function.
 *
 * <p>Inner classes define the standard MDX regression analysis functions:
 * {@link Intercept LinRegIntercept},
 * {@link Point LinRegPoint},
 * {@link Variance LinRegVariance},
 * {@link R2 LinRegR2}.</p>
 *
 * <p><a href="http://home.ubalt.edu/ntsbarsh/Business-stat/opre504.htm#rcomputeodel">
 * Dr Arsham's Statistics site</a> describes the statistics underlying these
 * functions.</p>
 *
 * @author Richard Emberson
 * @since 17 January, 2005
 * @version $Id$
 */
public abstract class LinReg extends FunkBase {

    /**
     * Helper class.
     */
    private static class Value {
        private List xs;
        private List ys;
        /**
         * The intercept for the linear regression model. Initialized
         * following a call to accuracy.
         */
        private double intercept;

        /**
         * The slope for the linear regression model. Initialized following a
         * call to accuracy.
         */
        private double slope;

        /** the coefficient of determination */
        private double rSquared = Double.MAX_VALUE;

        /** variance = sum square diff mean / n - 1 */
        private double variance = Double.MAX_VALUE;

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

    /**
     * Definition of the <code>LinRegIntercept</code> MDX linear regression
     * analysis function.
     */
    public static class Intercept extends LinReg {
        public Intercept() {
            super();
        }
        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
//            debug("LinReg.Intercept.evaluator","TOP");
            LinReg.Value value = process(evaluator, args);
            if (value == null) {
                return Util.nullValue;
            }
            return new Double(value.getIntercept());
        }
    }

    /**
     * Definition of <code>LinRegPoint</code> MDX linear regression
     * analysis function.
     */
    public static class Point extends LinReg {
        public Point() {
            super();
        }
        // <Numeric Expression>,
        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
//            debug("LinReg.Point.evaluator","TOP");
            double x = getDoubleArg(evaluator, args, 0).doubleValue();
//            debug("LinReg.Point.evaluator","x=" +x);

//            debug("LinReg.Point.evaluator","args.length=" +args.length);
            // remove first arg and pass the rest to init
            Exp[] args2 = new Exp[args.length-1];
            System.arraycopy(args, 1, args2, 0, args.length-1);

//            debug("LinReg.Point.evaluator","args2.length=" +args2.length);
            // remember: pass in args2!!! NOT args
            LinReg.Value value = process(evaluator, args2);
            if (value == null) {
                return Util.nullValue;
            }

            // use first arg to generate y position
            double y = x * value.getSlope() + value.getIntercept();
//            debug("LinReg.Point.evaluator","y=" +y);
            return new Double(y);
        }
    }

    /**
     * Definition of <code>LinRegSlope</code> MDX linear regression
     * analysis function.
     */
    public static class Slope extends LinReg {
        public Slope() {
            super();
        }
        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
//            debug("LinReg.Slope.evaluator","TOP");
            LinReg.Value value = process(evaluator, args);
            if (value == null) {
                return Util.nullValue;
            }
            return new Double(value.getSlope());
        }
    }

    /**
     * Definition of <code>LinRegR2</code> MDX linear regression
     * analysis function.
     */
    public static class R2 extends LinReg {
        public R2() {
            super();
        }

        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
//            debug("LinReg.R2.evaluator","TOP");

            LinReg.Value value = accuracy(evaluator, args);
            if (value == null) {
                return Util.nullValue;
            }
            return new Double(value.getRSquared());
        }
    }

    /**
     * Definition of <code>LinRegVariance</code> MDX linear regression
     * analysis function.
     */
    public static class Variance extends LinReg {
        public Variance() {
            super();
        }

        // <Set>, <Numeric Expression>[, <Numeric  Expression>]
        public Object evaluate(Evaluator evaluator, Exp[] args) {
//            debug("LinReg.Variance.evaluator","TOP");

            LinReg.Value value = accuracy(evaluator, args);
            if (value == null) {
                return Util.nullValue;
            }
            return new Double(value.getVariance());
        }
    }

    // initialized by BuiltinFunTable; TODO: eliminate this
    static Exp valueFunCall;

    protected LinReg() {
        super();
    }

    // <Set>, <Numeric Expression>[, <Numeric  Expression>]
    protected LinReg.Value accuracy(Evaluator evaluator, Exp[] args) {
        LinReg.Value value = process(evaluator, args);
        if (value == null) {
            return null;
        }

        // for variance
        double sumErrSquared = 0.0;


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

            //sumErr += error;
            //sumAbsErr += Math.abs(error);
            //sumAbsPercentErr += Math.abs(error / y);

            sumErrSquared += error*error;

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
        // calculate r squared
        // http://home.ubalt.edu/ntsbarsh/Business-stat/opre504.htm

        // sum y*y - (sum y)(sum y)/n
        double ssyy = sumSquaredY - (sumY * sumY) / n;
        double ssff = sumSquaredYF - (sumYF * sumYF) / n;

        // r2 = SSff / SSyy
        if (ssyy != 0.0) {
            double rSquared = ssff / ssyy;

            value.setRSquared(rSquared);
        }
        return value;
    }


    // <Set>, <Numeric Expression>[, <Numeric  Expression>]
    protected LinReg.Value process(Evaluator evaluator, Exp[] args) {
        List members = (List) getArg(evaluator, args, 0);
        ExpBase expY = (ExpBase) getArgNoEval(args, 1);
        ExpBase expX = (ExpBase) getArgNoEval(args, 2, valueFunCall);

        evaluator = evaluator.push();
//        debug("LinReg.process","before SetWrapper swY = evaluateSet(evaluator, members, expY)");
        SetWrapper swY = evaluateSet(evaluator, members, expY);
//        debug("LinReg.process","before SetWrapper swX = evaluateSet(evaluator, members, expX)");
        SetWrapper swX = evaluateSet(evaluator, members, expX);

        if (swY.errorCount > 0) {
//            debug("LinReg.process","ERROR error(s) count =" +swY.errorCount);
            // TODO: throw exception
            return null;
        } else if (swY.v.size() == 0) {
            return null;
        }


        // y and x have same number of points
        int n = swY.v.size();
        double sumX = 0.0;
        double sumY = 0.0;
        double sumXX = 0.0;
        double sumXY = 0.0;

//        debug("LinReg.process","svY.v.size()=" +swY.v.size());
//        debug("LinReg.process","svX.v.size()=" +swX.v.size());
        for (int i = 0; i < n; i++) {
            double y = ((Double) swY.v.get(i)).doubleValue();
            double x = ((Double) swX.v.get(i)).doubleValue();

//            debug("LinReg.process"," " +i+ " (" +x+ "," +y+ ")");
            sumX += x;
            sumY += y;
            sumXX += x * x;
            sumXY += x * y;
        }

        double xMean = sumX / n;
        double yMean = sumY / n;

//        debug("LinReg.process", "yMean=" +yMean);
//        debug("LinReg.process", "(n*sumXX - sumX*sumX)=" +(n*sumXX - sumX*sumX));
        // The regression line is the line that minimizes the variance of the
        // errors. The mean error is zero; so, this means that it minimizes the
        // sum of the squares errors.
        double slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
        double intercept = yMean - slope*xMean;

        LinReg.Value value = new LinReg.Value(intercept, slope, swX.v, swY.v);
//        debug("LinReg.process","value=" +value);

        return value;
    }


    protected List forecast(LinReg.Value value) {
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

// End LinReg.java
