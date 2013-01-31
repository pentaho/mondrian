/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.udf;

import mondrian.olap.Evaluator;
import mondrian.olap.Syntax;
import mondrian.olap.fun.MondrianEvaluationException;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.log4j.Logger;


/**
 * A user-defined function which returns the inverse normal distribution value
 * of its argument.
 *
 * <p>This particular function is useful in Six Sigma calculations, for
 * example,
 *
 * <blockquote><code><pre>
 * WITH MEMBER [Measures].[Yield]
 *         AS '([Measures].[Number of Failures] / [Measures].[Population])',
 *         FORMAT_STRING = "0.00%"
 *     MEMBER [Measures].[Sigma]
 *         AS 'IIf([Measures].[Yield] <&gt; 0,
 *                 IIf([Measures].[Yield] &gt; 0.5,
 *                     0,
 *                     InverseNormal(1 - ([Measures].[Yield])) + 1.5), 6)',
 *         FORMAT_STRING = "0.0000"
 * </pre></code></blockquote>
 */
public class InverseNormalUdf implements UserDefinedFunction {
    private static final Logger LOGGER =
        Logger.getLogger(InverseNormalUdf.class);


    // the zero arg constructor sets the mean equal to zero and standard
    // deviation equal to one
    private static final NormalDistribution nd = new NormalDistributionImpl();

    public String getName() {
        return "InverseNormal";
    }

    public String getDescription() {
        return "Returns inverse normal distribution of its argument";
    }

    public Syntax getSyntax() {
        return Syntax.Function;
    }

    public Type getReturnType(Type[] types) {
        return new NumericType();
    }

    public Type[] getParameterTypes() {
        return new Type[] {new NumericType()};
    }

    public Object execute(Evaluator evaluator, Argument[] args) {
        final Object argValue = args[0].evaluateScalar(evaluator);
        LOGGER.debug("Inverse Normal argument was : " + argValue);
        if (!(argValue instanceof Number)) {
            // Argument might be a RuntimeException indicating that
            // the cache does not yet have the required cell value. The
            // function will be called again when the cache is loaded.
            return null;
        }

        final Double d = new Double(((Number) argValue).doubleValue());
        LOGGER.debug("Inverse Normal argument as Double was : " + d);

        if (d.isNaN()) {
            return null;
        }

        // If probability is nonnumeric or
        //   probability < 0 or
        //   probability > 1,
        // returns an error.
        double dbl = d.doubleValue();
        if (dbl < 0.0 || dbl > 1.0) {
            LOGGER.debug(
                "Invalid value for inverse normal distribution: " + dbl);
            throw new MondrianEvaluationException(
                "Invalid value for inverse normal distribution: " + dbl);
        }
        try {
            Double result = new Double(nd.inverseCumulativeProbability(dbl));
            LOGGER.debug("Inverse Normal result : " + result.doubleValue());
            return result;
        } catch (MathException e) {
            LOGGER.debug(
                "Exception calculating inverse normal distribution: " + dbl, e);
            throw new MondrianEvaluationException(
                "Exception calculating inverse normal distribution: " + dbl);
        }
    }

    public String[] getReservedWords() {
        return null;
    }

}

// End InverseNormalUdf.java
