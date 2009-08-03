/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

/**
 * Test XMLA output in tabular (flattened) format.
 *
 * @author Julio Caub&iacute;n, jhyde
 * @version $Id$
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
        doTest(requestType, getDefaultRequestProperties(requestType), TestContext.instance());
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
