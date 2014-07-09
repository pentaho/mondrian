/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2007 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test.clearview;

import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * <code>QueryAllVCTest</code> is a test suite which tests
 * complex queries against the FoodMart database. MDX queries and their
 * expected results are maintained separately in QueryAllVCTest.ref.xml file.
 *
 * @author Khanh Vu
 */
public class QueryAllVCTest extends ClearViewBase {

    public QueryAllVCTest() {
        super();
    }

    public QueryAllVCTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(QueryAllVCTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), QueryAllVCTest.class);
    }

}

// End QueryAllVCTest.java
