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
 * <code>TopBottomTest</code> is a test suite which tests scenarios of
 * selecting top and bottom records against the FoodMart database.
 * MDX queries and their expected results are maintained separately in
 * TopBottomTest.ref.xml file.If you would prefer to see them as inlined
 * Java string literals, run ant target "generateDiffRepositoryJUnit" and
 * then use file TopBottomTestJUnit.java which will be generated in
 * this directory.
 *
 * @author Khanh Vu
 */
public class TopBottomTest extends ClearViewBase {

    public TopBottomTest() {
        super();
    }

    public TopBottomTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(TopBottomTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), TopBottomTest.class);
    }

}

// End TopBottomTest.java
