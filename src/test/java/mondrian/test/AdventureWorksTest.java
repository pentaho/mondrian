/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

/**
 * Unit test that uses the AdventureWorks schema by default.
 *
 * @see FoodMartTestCase
 * @see SteelWheelsTestCase
 * @author jhyde
 */
public class AdventureWorksTest extends FoodMartTestCase {
    @Override
    public TestContext getTestContext() {
        return super.getTestContext()
            .with(TestContext.DataSet.ADVENTURE_WORKS_DW)
            .withCube("Internet Sales");
    }

    /** Simple query. */
    public void testFoo() {
        assertQueryReturns(
            "select from [Internet Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "60,398");
    }
}

// End AdventureWorksTest.java
