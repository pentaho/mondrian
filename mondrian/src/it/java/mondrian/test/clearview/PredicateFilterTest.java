/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.test.clearview;

import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * <code>PredicateFilterTest</code> is a test suite which tests scenarios of
 * filtering in the FoodMart database.
 * MDX queries and their expected results are maintained separately in
 * PredicateFilterTest.ref.xml file.If you would prefer to see them as inlined
 * Java string literals, run ant target "generateDiffRepositoryJUnit" and
 * then use file PredicateFilterTestJUnit.java which will be generated in
 * this directory.
 *
 * @author Khanh Vu
 */
public class PredicateFilterTest extends ClearViewBase {

    public PredicateFilterTest() {
        super();
    }

    public PredicateFilterTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(PredicateFilterTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), PredicateFilterTest.class);
    }

}

// End PredicateFilterTest.java
