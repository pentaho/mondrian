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
 * <code>CVBasicTest</code> is a test suite which tests
 * complex queries against the FoodMart database. MDX queries and their
 * expected results are maintained separately in CVBasicTest.ref.xml file.
 * If you would prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * file CVBasicTestJUnit.java which will be generated in this directory.
 *
 * @author Khanh Vu
 */
public class CVBasicTest extends ClearViewBase {

    public CVBasicTest() {
        super();
    }

    public CVBasicTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(CVBasicTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), CVBasicTest.class);
    }

}

// End CVBasicTest.java
