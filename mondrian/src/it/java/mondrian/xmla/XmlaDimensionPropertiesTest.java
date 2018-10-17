/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2018 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

import java.util.Properties;

/**
 * Test of dimension properties in xmla response.
 * Checks each property is added to its own hierarchy.
 *  - fix for MONDRIAN-2302 issue.
 *
 * @author Yury_Bakhmutski.
 */
public class XmlaDimensionPropertiesTest extends XmlaBaseTestCase {

    public void testOneHierarchyProperties() throws Exception {
        executeTest("HR");
    }

    public void testTwoHierarchiesProperties() throws Exception {
        executeTest("HR");
    }

    public void testMondrian2342() throws Exception {
        executeTest("Sales");
    }

    private void executeTest(String cubeName) throws Exception {
        TestContext context = getTestContext().withCube(cubeName);
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, context);
    }

    @Override
    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaDimensionPropertiesTest.class);
    }

    @Override
    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }
}

// End XmlaDimensionPropertiesTest.java
