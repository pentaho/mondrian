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

package mondrian.test.clearview;

import mondrian.olap.MondrianProperties;
import mondrian.test.DiffRepository;
import mondrian.util.Bug;

import junit.framework.TestSuite;

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
 */
public class MetricFilterTest extends ClearViewBase {

    public MetricFilterTest() {
        super();
    }

    public MetricFilterTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(MetricFilterTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), MetricFilterTest.class);
    }

    protected void runTest() throws Exception {
        if (!Bug.BugMondrian2452Fixed
            && (getName().equals("testMetricFiltersWithNoSubtotals"))
            && !MondrianProperties.instance().EnableNativeCrossJoin.get())
        {
            // Tests give wrong results if native crossjoin is disabled.
            return;
        }
        super.runTest();
    }
}

// End MetricFilterTest.java
