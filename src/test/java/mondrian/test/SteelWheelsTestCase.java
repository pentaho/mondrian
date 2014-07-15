/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;


import junit.framework.TestCase;

/**
 * Unit test against Pentaho's Steel Wheels sample database.
 *
 * <p>It is not required that the Steel Wheels database be present, so each
 * test should check whether the database exists and trivially succeed if it
 * does not.
 *
 * @author jhyde
 * @since 12 March 2009
 */
public class SteelWheelsTestCase extends TestCase {

    /** Creates a SteelWheelsTestCase. */
    public SteelWheelsTestCase() {
    }

    /**
     * Returns the test context. Override this method if you wish to use a
     * different source for your SteelWheels connection.
     */
    public TestContext getTestContext() {
        return TestContext.instance().with(TestContext.DataSet.STEELWHEELS)
            .withCube("SteelWheelsSales");
    }
}

// End SteelWheelsTestCase.java
