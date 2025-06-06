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

import mondrian.olap.Util;

import junit.framework.*;

import org.olap4j.test.TestContext;

import java.util.Properties;

/**
 * Test suite that runs the olap4j Test Compatiblity Kit (TCK) against
 * mondrian's olap4j driver.
 *
 * @author jhyde
 * @since 2010/11/22
 */
public class Olap4jTckTest extends TestCase {
    private static final Util.Functor1<Boolean, Test> CONDITION =
        new Util.Functor1<Boolean, Test>() {
            public Boolean apply(Test test) {
                if (!(test instanceof TestCase)) {
                    return true;
                }
                final TestCase testCase = (TestCase) test;
                final String testCaseName = testCase.getName();
                return !testCaseName.equals("testStatementTimeout")
                    // olap4j-tck does not close ResultSet, and that's a
                    // resource leak
                    && !testCaseName.startsWith(
                        "testCubesDrillthroughReturnClause")
                    && !testCaseName.equals("testStatementCancel")
                    && !testCaseName.equals("testDatabaseMetaDataGetCatalogs")
                    && !testCaseName.equals("testCellSetBug")
                    // Disabling the following until the olap4j test expected
                    // value is updated.
                    && !testCaseName.equals(
                        "testDatabaseMetaDataGetDatasources");
            }
        };

    public static TestSuite suite() {
        final Util.PropertyList list =
            mondrian.test.TestContext.instance()
                .getConnectionProperties();
        final String connStr = "jdbc:mondrian:" + list;
        final String catalog = list.get("Catalog");

        final TestSuite suite = new TestSuite();

        suite.setName("olap4j TCK");

        // These suits are commented due to an update from commons-dbcp to commons-dbcp2 in this commit bellow.
        // https://github.com/pentaho/mondrian/commit/1ba3650e73208a5282f466068e5388ea169f1deb
        // The Olap4j library depends on the commons-dbcp library and there is no new version at the moment.
        // Ideally this library should be updated or replaced by another. Until then, these tests are commented.

        // suite.addTest(createMondrianSuite(connStr, false));
        // suite.addTest(createMondrianSuite(connStr, true));
        // suite.addTest(createXmlaSuite(connStr, catalog, false));
        // suite.addTest(createXmlaSuite(connStr, catalog, true));
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
        final TestSuite suite = TestContext.createTckSuite(properties, name);
        if (CONDITION == null) {
            return suite;
        }
        return mondrian.test.TestContext.copySuite(suite, CONDITION);
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
        final TestSuite suite = TestContext.createTckSuite(properties, name);
        if (CONDITION == null) {
            return suite;
        }
        return mondrian.test.TestContext.copySuite(suite, CONDITION);
    }
}

// End Olap4jTckTest.java
