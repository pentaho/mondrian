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

import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * <code>MiscTest</code> is a test suite which tests miscellaneous
 * complex queries against the FoodMart database. MDX queries and their
 * expected results are maintained separately in MiscTest.ref.xml file.
 * If you would prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * file MiscTestJUnit.java which will be generated in this directory.
 *
 * @author Khanh Vu
 */
public class MiscTest extends ClearViewBase {

    public MiscTest() {
        super();
    }

    public MiscTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(MiscTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), MiscTest.class);
    }

}

// End MiscTest.java
