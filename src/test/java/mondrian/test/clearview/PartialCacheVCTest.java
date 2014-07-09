/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test.clearview;

import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * <code>PartialCacheVCTest</code> is a test suite which tests complex queries
 * against the FoodMart database. MDX queries and their expected results are
 * maintained separately in PartialCacheVCTest.ref.xml file.
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
