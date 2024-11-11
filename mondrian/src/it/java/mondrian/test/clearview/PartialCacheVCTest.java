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
 * <code>PartialCacheVCTest</code> is a test suite which tests complex queries
 * against the FoodMart database. MDX queries and their expected results are
 * maintained separately in PartialCacheVCTest.ref.xml file.  If you would
 * prefer to see them as inlined Java string literals, run ant target
 * "generateDiffRepositoryJUnit" and then use file PartialCacheVCTestJUnit.java
 * which will be generated in this directory.
 *
 * @author Khanh Vu
 */
public class PartialCacheVCTest extends ClearViewBase {

    public PartialCacheVCTest() {
        super();
    }

    public PartialCacheVCTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(PartialCacheVCTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), PartialCacheVCTest.class);
    }

}

// End PartialCacheVCTest.java
