/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.util;

import mondrian.test.TestContext;

import junit.framework.TestCase;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Testcase for {@link mondrian.util.PrimeFinder}.
 *
 * @author jhyde
 * @since Feb 4, 2007
 */
public class PrimeFinderTest extends TestCase {

    private void assertStatistics(int from, int to, String expected) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        PrimeFinder.statistics(from, to, pw);
        pw.flush();
        TestContext.assertEqualsVerbose(expected, sw.toString());
    }

    public void testOne() {
        assertStatistics(
            1000,
            1000,
            "new maxdev @1000@dev=0.039\n"
            + "Statistics for [1000,1000] are as follows\n"
            + "meanDeviation = 3.9 %\n"
            + "maxDeviation = 3.9 %\n");
    }

    public void testTwo() {
        assertStatistics(
            200,
            1000,
            "new maxdev @200@dev=0.385\n"
            + "Statistics for [200,1000] are as follows\n"
            + "meanDeviation = 6.589286 %\n"
            + "maxDeviation = 38.5 %\n");
    }

    public void testThree() {
        assertStatistics(
            16,
            1000,
            "new maxdev @16@dev=0.0625\n"
            + "new maxdev @18@dev=0.2777777777777778\n"
            + "new maxdev @24@dev=0.2916666666666667\n"
            + "new maxdev @48@dev=0.3958333333333333\n"
            + "new maxdev @98@dev=0.3979591836734694\n"
            + "new maxdev @198@dev=0.398989898989899\n"
            + "Statistics for [16,1000] are as follows\n"
            + "meanDeviation = 7.374975 %\n"
            + "maxDeviation = 39.898987 %\n");
    }

    // disabled because it takes a LONG time
    public void _testFour() {
        assertStatistics(1000, Integer.MAX_VALUE, "");
    }
}

// End PrimeFinderTest.java
