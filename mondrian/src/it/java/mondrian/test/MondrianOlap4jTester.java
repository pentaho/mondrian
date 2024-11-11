/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
