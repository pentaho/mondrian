/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.test.clearview;

import mondrian.olap.MondrianProperties;
import mondrian.test.DiffRepository;
import mondrian.util.Bug;

import junit.framework.TestSuite;

/**
 * <code>SummaryMetricPercentTest</code> is a test suite which tests scenarios
 * of computing sums and percentages against the FoodMart database.
 * MDX queries and their expected results are maintained separately in
 * SummaryMetricPercentTest.ref.xml file.If you would prefer to see them as
 * inlined Java string literals, run ant target "generateDiffRepositoryJUnit"
 * and then use file SummaryMetricPercentTestJUnit.java which will be
 * generated in this directory.
 *
 * @author Khanh Vu
 */
public class SummaryMetricPercentTest extends ClearViewBase {

    public SummaryMetricPercentTest() {
        super();
    }

    public SummaryMetricPercentTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(SummaryMetricPercentTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(
            getDiffReposStatic(),
            SummaryMetricPercentTest.class);
    }

    @Override
    protected void runTest() throws Exception {
        if (!Bug.BugMondrian2452Fixed
            && (getName().equals("testSpecialMetricPctOfCol"))
            && !MondrianProperties.instance().EnableNativeCrossJoin.get())
        {
            // Tests give wrong results if native crossjoin is disabled.
            return;
        }
        super.runTest();
    }
}

// End SummaryMetricPercentTest.java
