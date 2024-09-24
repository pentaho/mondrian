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
