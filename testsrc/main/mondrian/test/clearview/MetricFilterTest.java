/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;
import mondrian.olap.MondrianProperties;
import mondrian.util.Bug;

/**
 * <code>MetricFilterTest</code> is a test suite which tests scenarios of
 * filtering out measures' values in the FoodMart database.
 * MDX queries and their expected results are maintained separately in
 * MetricFilterTest.ref.xml file.If you would prefer to see them as inlined
 * Java string literals, run ant target "generateDiffRepositoryJUnit" and
 * then use file MetricFilterTestJUnit.java which will be generated in
 * this directory.
 *
 * @author Khanh Vu
 * @version $Id$
 */
public class MetricFilterTest extends ClearViewBase {

    public MetricFilterTest() {
        super();
    }

    public MetricFilterTest(String name) {
        super(name);
    }

    protected DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(MetricFilterTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), MetricFilterTest.class);
    }

    protected void runTest() throws Exception {
        // Do not run the test in circumstances which hit bug 1696772. Remove
        // this short-circuit when that bug is fixed.
        if (getName().equals("testMetricFiltersWithNoSubtotals") &&
            !MondrianProperties.instance().EnableNativeCrossJoin.get() &&
            !Bug.Bug1696772Fixed) {
            return;
        }
        super.runTest();
    }
}

// End MetricFilterTest.java
