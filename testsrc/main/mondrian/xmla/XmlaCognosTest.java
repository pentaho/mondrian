/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.MondrianProperties;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

/**
 * Test suite for compatibility of Mondrian XMLA with Cognos8.2 connected via
 * Simba O2X bridge.
 *
 * @author Thiyagu, Shishir
 * @version $Id$
 */

public class XmlaCognosTest extends XmlaBaseTestCase {

    public XmlaCognosTest() {
    }

    public XmlaCognosTest(String name) {
        super(name);
    }

    public void testDimensionPropertyForPercentageIssue() throws Exception {
        executeMDX();
    }

    public void testNegativeSolveOrder() throws Exception {
        executeMDX();
    }

    public void testNonEmptyWithCognosCalcOneLiteral() throws Exception {
        boolean currentState =
                MondrianProperties.instance().EnableNonEmptyOnAllAxis.get();
        MondrianProperties.instance().EnableNonEmptyOnAllAxis.set(true);
        executeMDX();
        MondrianProperties.instance().EnableNonEmptyOnAllAxis.set(currentState);
    }

    public void testCellProperties() throws Exception {
        executeMDX();
    }

    public void testCrossJoin() throws Exception {
        executeMDX();
    }

    public void testWithFilterOn3rdAxis() throws Exception {
        executeMDX();
    }

    public void testWithSorting() throws Exception {
        executeMDX();
    }

    public void testWithFilter() throws Exception {
        executeMDX();
    }

    public void testWithAggregation() throws Exception {
        executeMDX();
    }

    private void executeMDX() throws Exception {
        String requestType = "EXECUTE";
        doTest(requestType, getDefaultRequestProperties(requestType), TestContext.instance());
    }

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaCognosTest.class);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return null;
    }

    protected String getSessionId(Action action) {
        return null;
    }

    protected void setUp() throws Exception {
    }

    protected void tearDown() throws Exception {
    }
}
// XmlaCognosTest.java