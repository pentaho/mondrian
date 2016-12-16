/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2016 Pentaho Corporation..  All rights reserved.
*/
package mondrian.test.clearview;

import mondrian.olap.MondrianProperties;
import mondrian.test.DiffRepository;
import mondrian.util.Bug;

import junit.framework.TestSuite;

/**
 * <code>SummaryTest</code> is a test suite which tests scenarios of
 * summing unit sales against the FoodMart database.
 * MDX queries and their expected results are maintained separately in
 * SummaryTest.ref.xml file.If you would prefer to see them as inlined
 * Java string literals, run ant target "generateDiffRepositoryJUnit" and
 * then use file SummaryTestJUnit.java which will be generated in
 * this directory.
 *
 * @author Khanh Vu
 */
public class SummaryTest extends ClearViewBase {

    public SummaryTest() {
        super();
    }

    public SummaryTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(SummaryTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), SummaryTest.class);
    }

    @Override
    protected void runTest() throws Exception {
        if (!Bug.BugMondrian785Fixed
            && (getName().equals("testRankExpandNonNative")
                || getName().equals("testCountExpandNonNative")
                || getName().equals("testCountOverTimeExpandNonNative"))
            && MondrianProperties.instance().EnableNativeCrossJoin.get())
        {
            // Tests give wrong results if native crossjoin is disabled.
            return;
        }
        if (!Bug.BugMondrian2452Fixed
            && (getName().equals("testRankExpandNonNative"))
            && !MondrianProperties.instance().EnableNativeCrossJoin.get())
        {
            // Tests give wrong results if native crossjoin is disabled.
            return;
        }
        super.runTest();
    }
}

// End SummaryTest.java
