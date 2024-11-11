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

import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * <code>GrandTotalTest</code> is a test suite which tests scenarios of
 * using grand total against the FoodMart database. MDX queries and their
 * expected results are maintained separately in GrandTotalTest.ref.xml file.
 * If you would prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * file GrandTotalTestJUnit.java which will be generated in this directory.
 *
 * @author Khanh Vu
 */
public class GrandTotalTest extends ClearViewBase {

    public GrandTotalTest() {
        super();
    }

    public GrandTotalTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(GrandTotalTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), GrandTotalTest.class);
    }

}

// End GrandTotalTest.java
