/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test.clearview;

import mondrian.test.DiffRepository;

import junit.framework.TestSuite;

/**
 * Test for hanger dimension (a dimension with no base table, not joined to
 * the fact table, and which only contains calculated members).
 *
 * <p>MDX queries and their expected results are maintained separately in
 * HangerDimensionClearViewTest.ref.xml file.</p>
 *
 * @author Khanh Vu
 */
public class HangerDimensionClearViewTest extends ClearViewBase {
    public HangerDimensionClearViewTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(HangerDimensionClearViewTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(
            getDiffReposStatic(), HangerDimensionClearViewTest.class);
    }
}

// End HangerDimensionClearViewTest.java
