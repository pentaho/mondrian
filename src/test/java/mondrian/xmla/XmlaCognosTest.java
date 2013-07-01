/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2010 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.spi.Dialect;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.util.Bug;

import org.eigenbase.util.property.BooleanProperty;

/**
 * Test suite for compatibility of Mondrian XMLA with Cognos8.2 connected via
 * Simba O2X bridge.
 *
 * @author Thiyagu, Shishir
 */

public class XmlaCognosTest extends XmlaBaseTestCase {

    public XmlaCognosTest() {
    }

    public XmlaCognosTest(String name) {
        super(name);
    }

    @Override
    protected String filter(
        String testCaseName, String filename, String content)
    {
        if ("testWithFilter".equals(testCaseName)
            && filename.equals("response"))
        {
            Dialect dialect = TestContext.instance().getDialect();
            switch (dialect.getDatabaseProduct()) {
            case DERBY:
                content = Util.replace(
                    content,
                    "<Value xsi:type=\"xsd:double\">",
                    "<Value xsi:type=\"xsd:int\">");
                break;
            }
        }
        return content;
    }

    public void testCognosMDXSuiteHR_001() throws Exception {
        Dialect dialect = TestContext.instance().getDialect();
        switch (dialect.getDatabaseProduct()) {
        case DERBY:
            // Derby gives right answer, but many cells have wrong xsi:type.
            return;
        }
        executeMDX();
    }

    public void testCognosMDXSuiteHR_002() throws Exception {
        Dialect dialect = TestContext.instance().getDialect();
        switch (dialect.getDatabaseProduct()) {
        case DERBY:
            // Derby gives right answer, but many cells have wrong xsi:type.
            return;
        }
        executeMDX();
    }

    public void testCognosMDXSuiteSales_001() throws Exception {
        executeMDX();
    }

    public void testCognosMDXSuiteSales_002() throws Exception {
        executeMDX();
    }

    public void testCognosMDXSuiteSales_003() throws Exception {
        executeMDX();
    }

    public void testCognosMDXSuiteSales_004() throws Exception {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_003()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_005()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_006()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_007()
        throws Exception
    {
        executeMDX();
    }

    // disabled because runs out of memory/hangs
    public void _testCognosMDXSuiteConvertedAdventureWorksToFoodMart_009()
        throws Exception
    {
        executeMDX();
    }

    // disabled because runs out of memory/hangs
    public void _testCognosMDXSuiteConvertedAdventureWorksToFoodMart_012()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_013()
        throws Exception
    {
        executeMDX();
    }

    // disabled because runs out of memory/hangs
    public void _testCognosMDXSuiteConvertedAdventureWorksToFoodMart_014()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_015()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_016()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_017()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_020()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_021()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_024()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_028()
        throws Exception
    {
        executeMDX();
    }

    public void testCognosMDXSuiteConvertedAdventureWorksToFoodMart_029()
        throws Exception
    {
        executeMDX();
    }

    public void testDimensionPropertyForPercentageIssue() throws Exception {
        executeMDX();
    }

    public void testNegativeSolveOrder() throws Exception {
        executeMDX();
    }

    public void testNonEmptyWithCognosCalcOneLiteral() throws Exception {
        final BooleanProperty enableNonEmptyOnAllAxes =
                MondrianProperties.instance().EnableNonEmptyOnAllAxis;
        boolean nonEmptyAllAxesCurrentState = enableNonEmptyOnAllAxes.get();

        final BooleanProperty enableNativeNonEmpty =
                MondrianProperties.instance().EnableNativeNonEmpty;
        boolean nativeNonemptyCurrentState = enableNativeNonEmpty.get();

        try {
            enableNonEmptyOnAllAxes.set(true);
            enableNativeNonEmpty.set(false);
            executeMDX();
            if (Bug.BugMondrian446Fixed) {
                enableNativeNonEmpty.set(true);
                executeMDX();
            }
        } finally {
            enableNativeNonEmpty.set(nativeNonemptyCurrentState);
            enableNonEmptyOnAllAxes.set(nonEmptyAllAxesCurrentState);
        }
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
        if (getTestContext().getDialect().getDatabaseProduct()
            == Dialect.DatabaseProduct.ACCESS)
        {
            // Disabled because of bug on access: generates query with
            // distinct-count even though access does not support it. Bug
            // 2685902, "Mondrian generates invalid count distinct on access"
            // logged.
            return;
        }
        executeMDX();
    }

    public void testWithAggregation() throws Exception {
        executeMDX();
    }

    private void executeMDX() throws Exception {
        String requestType = "EXECUTE";
        doTest(
            requestType, getDefaultRequestProperties(requestType),
            TestContext.instance());
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
}

// End XmlaCognosTest.java
