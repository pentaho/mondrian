/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test.clearview;

import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * <code>SummaryMetricPercentTest</code> is a test suite which tests scenarios
 * of computing sums and percentages against the FoodMart database.
 * MDX queries and their expected results are maintained separately in
 * SummaryMetricPercentTest.ref.xml file.
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

}

// End SummaryMetricPercentTest.java
