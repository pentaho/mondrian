/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

/**
 * <code>OrderTest</code> tests the extended syntax of Order
 * function. See { @link
 * http://pub.eigenbase.org/wiki/MondrianOrderFunctionExtension } for
 * syntax rules.
 * MDX queries and their expected results are maintained separately in
 * OrderTest.ref.xml file.If you would prefer to see them as inlined
 * Java string literals, run ant target "generateDiffRepositoryJUnit" and
 * then use file OrderTestJUnit.java which will be generated in
 * this directory.
 *
 * @author Khanh Vu
 * @version $Id$
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
        return constructSuite(getDiffReposStatic(), OrderTest.class);
    }

}

// End OrderTest.java
