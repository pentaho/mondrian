/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.test.clearview;

import mondrian.olap.MondrianProperties;
import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * <code>BatchedFillTest</code> is a test suite which tests
 * complex queries against the FoodMart database. MDX queries and their
 * expected results are maintained separately in BatchedFillTest.ref.xml file.
 * If you would prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * file BatchedFillTestJUnit.java which will be generated in this directory.
 *
 * @author Khanh Vu
 */
public class BatchedFillTest extends ClearViewBase {

    public BatchedFillTest() {
        super();
    }

    public BatchedFillTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(BatchedFillTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), BatchedFillTest.class);
    }

    protected void runTest() throws Exception {
        if (getName().equals("testBatchedFill2")
            && MondrianProperties.instance().ReadAggregates.get()
            && MondrianProperties.instance().UseAggregates.get())
        {
            // If agg tables are enabled, the SQL generated is 'better' than
            // expected.
        } else {
            super.assertQuerySql(true);
        }
        super.runTest();
    }

}

// End BatchedFillTest.java
