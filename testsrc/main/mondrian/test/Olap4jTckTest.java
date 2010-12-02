/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import mondrian.olap.Util;
import org.olap4j.test.TestContext;

import java.util.Properties;

/**
 * Test suite that runs the olap4j Test Compatiblity Kit (TCK) against
 * mondrian's olap4j driver.
 *
 * @author jhyde
 * @since 2010/11/22
 * @version $Id$
 */
public class Olap4jTckTest extends TestCase {
    public static TestSuite suite() {
        final Util.PropertyList list =
            mondrian.test.TestContext.instance()
                .getFoodMartConnectionProperties();
        final String connStr = "jdbc:mondrian:" + list;
        final String catalog = list.get("Catalog");

        final TestSuite suite = new TestSuite();
        suite.setName("olap4j TCK");
        suite.addTest(createMondrianSuite(connStr, false));
        suite.addTest(createMondrianSuite(connStr, true));
        suite.addTest(createXmlaSuite(connStr, catalog, false));
        suite.addTest(createXmlaSuite(connStr, catalog, true));
        return suite;
    }

    private static TestSuite createXmlaSuite(
        String connStr, String catalog, boolean wrapper)
    {
        final Properties properties = new Properties();
        properties.setProperty("org.olap4j.test.connectUrl", connStr);
        properties.setProperty(
            "org.olap4j.test.helperClassName",
            "org.olap4j.XmlaTester");
        properties.setProperty("org.olap4j.XmlaTester.CatalogUrl", catalog);
        properties.setProperty(
            "org.olap4j.test.wrapper",
            wrapper ? "NONE" : "DBCP");
        String name =
            "XMLA olap4j driver talking to mondrian's XMLA server";
        if (wrapper) {
            name += " (DBCP wrapper)";
        }
        return TestContext.createTckSuite(properties, name);
    }

    private static TestSuite createMondrianSuite(
        String connStr, boolean wrapper)
    {
        final Properties properties = new Properties();
        properties.setProperty("org.olap4j.test.connectUrl", connStr);
        properties.setProperty(
            "org.olap4j.test.helperClassName",
            MondrianOlap4jTester.class.getName());
        properties.setProperty(
            "org.olap4j.test.wrapper",
            wrapper ? "NONE" : "DBCP");
        final String name =
            "mondrian olap4j driver"
            + (wrapper ? " (DBCP wrapper)" : "");
        return TestContext.createTckSuite(properties, name);
    }
}

// End Olap4jTckTest.java
