/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2010 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

/**
 * Implementation of {@link org.olap4j.test.TestContext.Tester} for Mondrian's
 * olap4j driver.
 *
 * @author Julian Hyde
 */
public class MondrianOlap4jTester extends AbstractMondrianOlap4jTester
{
    /**
     * Public constructor with {@link org.olap4j.test.TestContext} parameter as
     * required by {@link org.olap4j.test.TestContext.Tester} API.
     *
     * @param testContext Test context
     */
    public MondrianOlap4jTester(org.olap4j.test.TestContext testContext) {
        super(
            testContext,
            DRIVER_URL_PREFIX,
            DRIVER_CLASS_NAME,
            Flavor.MONDRIAN);
    }

    public static final String DRIVER_CLASS_NAME =
        "mondrian.olap4j.MondrianOlap4jDriver";

    public static final String DRIVER_URL_PREFIX = "jdbc:mondrian:";
}

// End MondrianOlap4jTester.java
