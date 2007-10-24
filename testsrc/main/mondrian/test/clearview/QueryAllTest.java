/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

/**
 * <code>QueryAllTest</code> is a test suite which tests
 * complex queries against the FoodMart database. MDX queries and their
 * expected results are maintained separately in QueryAllTest.ref.xml file.
 * If you would prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * file QueryAllTestJUnit.java which will be generated in this directory.
 *
 * @author Khanh Vu
 * @version $Id$
 */
public class QueryAllTest extends ClearViewBase {

    public QueryAllTest() {
        super();
    }

    public QueryAllTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(QueryAllTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), QueryAllTest.class);
    }

}

// End QueryAllTest.java
