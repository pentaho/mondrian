/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.test.clearview;

import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * <code>SubTotalTest</code> is a test suite which tests scenarios of
 * using sub totals against the FoodMart database. MDX queries and their
 * expected results are maintained separately in SubTotalTest.ref.xml file.
 * If you would prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * file SubTotalTestJUnit.java which will be generated in this directory.
 *
 * @author Khanh Vu
 */
public class SubTotalTest extends ClearViewBase {

    public SubTotalTest() {
        super();
    }

    public SubTotalTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(SubTotalTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), SubTotalTest.class);
    }

}

// End SubTotalTest.java
