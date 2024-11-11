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


package mondrian.xmla;

import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

/**
 * Test XMLA output in tabular (flattened) format.
 *
 * @author Julio Caub&iacute;n, jhyde
 */
public class XmlaTabularTest extends XmlaBaseTestCase {

    public XmlaTabularTest() {
    }

    public XmlaTabularTest(String name) {
        super(name);
    }

    public void testTabularOneByOne() throws Exception {
        executeMDX();
    }

    public void testTabularOneByTwo() throws Exception {
        executeMDX();
    }

    public void testTabularTwoByOne() throws Exception {
        executeMDX();
    }

    public void testTabularTwoByTwo() throws Exception {
        executeMDX();
    }

    public void testTabularZeroByZero() throws Exception {
        executeMDX();
    }

    public void testTabularVoid() throws Exception {
        executeMDX();
    }

    public void testTabularThreeAxes() throws Exception {
        executeMDX();
    }

    private void executeMDX() throws Exception {
        String requestType = "EXECUTE";
        doTest(
            requestType,
            getDefaultRequestProperties(requestType),
            TestContext.instance());
    }

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaTabularTest.class);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return null;
    }

    protected String getSessionId(Action action) {
        return null;
    }
}

// End XmlaTabularTest.java
