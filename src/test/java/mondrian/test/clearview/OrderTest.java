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
import mondrian.util.Bug;

import junit.framework.TestSuite;

/**
 * Test for the extended syntax of Order
 * function. See
 * <a href="http://pub.eigenbase.org/wiki/MondrianOrderFunctionExtension">
 *     MondrianOrderFunctionExtension</a>
 * for syntax rules.
 *
 * <p>MDX queries and their expected results are maintained separately in
 * OrderTest.ref.xml file.</p>
 *
 * @author Khanh Vu
 */
public class OrderTest extends ClearViewBase {

    public OrderTest() {
        super();
    }

    public OrderTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(OrderTest.class);
    }

    public static TestSuite suite() {
        if (!Bug.LayoutWrongCardinalty) {
            // OrderTest.testSortRowAtt fails until this is fixed
            return new TestSuite();
        }
        return constructSuite(getDiffReposStatic(), OrderTest.class);
    }

}

// End OrderTest.java
