/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.olap4j.MondrianOlap4jDriver;
import mondrian.spi.Dialect;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.tui.XmlUtil;
import mondrian.tui.XmlaSupport;

import org.olap4j.metadata.XmlaConstants;

import org.w3c.dom.Document;

import java.util.*;

/**
 * Test XML/A functionality.
 *
 * @author Richard M. Emberson
 */
public class XmlaBasicTest extends XmlaBaseTestCase {

    public static final String FORMAT_TABLULAR = "Tabular";

    // unique name
    public static final String UNIQUE_NAME_ELEMENT    = "unique.name.element";

    // dimension unique name
    public static final String UNIQUE_NAME_PROP     = "unique.name";

    public static final String RESTRICTION_NAME_PROP     = "restriction.name";
    public static final String RESTRICTION_VALUE_PROP     = "restriction.value";

    // content
    public static final String CONTENT_PROP     = "content";

    public XmlaBasicTest() {
    }

    public XmlaBasicTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Class.forName(MondrianOlap4jDriver.class.getName());
    }

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaBasicTest.class);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return null;
    }

    protected String extractSoapResponse(
        Document responseDoc,
        XmlaConstants.Content content)
    {
        Document partialDoc = null;
        switch (content) {
        case None:
            // return soap and no content
            break;

        case Schema:
            // return soap plus scheam content
            break;

        case Data:
            // return soap plus data content
            break;

        case SchemaData:
            // return everything
            partialDoc = responseDoc;
            break;
        }

        String responseText = XmlUtil.toString(responseDoc, false);
        return responseText;
    }

    /////////////////////////////////////////////////////////////////////////
    // DISCOVER
    /////////////////////////////////////////////////////////////////////////

    public void testDDatasource() throws Exception {
        String requestType = "DISCOVER_DATASOURCES";
        doTestRT(requestType, getTestContext());
    }

    public void testDEnumerators() throws Exception {
        String requestType = "DISCOVER_ENUMERATORS";
        doTestRT(requestType, getTestContext());
    }

    public void testDKeywords() throws Exception {
        String requestType = "DISCOVER_KEYWORDS";
        doTestRT(requestType, getTestContext());
    }

    public void testDLiterals() throws Exception {
        String requestType = "DISCOVER_LITERALS";
        doTestRT(requestType, getTestContext());
    }

    public void testDProperties() throws Exception {
        String requestType = "DISCOVER_PROPERTIES";
        doTestRT(requestType, getTestContext());
    }

    public void testDSchemaRowsets() throws Exception {
        String requestType = "DISCOVER_SCHEMA_ROWSETS";
        doTestRT(requestType, getTestContext());
    }

    /////////////////////////////////////////////////////////////////////////
    // DBSCHEMA
    /////////////////////////////////////////////////////////////////////////

    public void testDBCatalogs() throws Exception {
        String requestType = "DBSCHEMA_CATALOGS";
        doTestRT(requestType, getTestContext());
    }
    public void testDBSchemata() throws Exception {
        String requestType = "DBSCHEMA_SCHEMATA";
        doTestRT(requestType, getTestContext());
    }
    // passes 2/25 - I think that this is good but not sure
    public void _testDBColumns() throws Exception {
        String requestType = "DBSCHEMA_COLUMNS";
        doTestRT(requestType, getTestContext());
    }
    // passes 2/25 - I think that this is good but not sure
    public void _testDBProviderTypes() throws Exception {
        String requestType = "DBSCHEMA_PROVIDER_TYPES";
        doTestRT(requestType, getTestContext());
    }
    // passes 2/25 - I think that this is good but not sure
    // Should this even be here
    public void _testDBTablesInfo() throws Exception {
        String requestType = "DBSCHEMA_TABLES_INFO";
        doTestRT(requestType, getTestContext());
    }
    // passes 2/25 - I think that this is good but not sure
    public void testDBTables() throws Exception {
        String requestType = "DBSCHEMA_TABLES";
        doTestRT(requestType, getTestContext());
    }

    /////////////////////////////////////////////////////////////////////////
    // MDSCHEMA
    /////////////////////////////////////////////////////////////////////////

    public void testMDActions() throws Exception {
        String requestType = "MDSCHEMA_ACTIONS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, getTestContext());
    }

    public void testMDCubes() throws Exception {
        String requestType = "MDSCHEMA_CUBES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, getTestContext());
    }

    public void testMDCubesJson() throws Exception {
        String requestType = "MDSCHEMA_CUBES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, getTestContext());
    }

    public void testMDCubesDeep() throws Exception {
        String requestType = "MDSCHEMA_CUBES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, "HR");
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, getTestContext());
    }

    public void testMDCubesDeepJson() throws Exception {
        String requestType = "MDSCHEMA_CUBES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, "HR");
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, getTestContext());
    }

    public void testMDCubesLocale() throws Exception {
        String requestType = "MDSCHEMA_CUBES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, "Sales");
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(LOCALE_PROP, Locale.GERMANY.toString());

        doTest(requestType, props, getTestContext());
    }

    public void testMDCubesLcid() throws Exception {
        String requestType = "MDSCHEMA_CUBES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, "Sales");
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(LOCALE_PROP, 0x040c + ""); // LCID code for FRENCH

        doTest(requestType, props, getTestContext());
    }

    public void testMDSets() throws Exception {
        String requestType = "MDSCHEMA_SETS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, getTestContext());
    }

    public void testMDDimensions() throws Exception {
        String requestType = "MDSCHEMA_DIMENSIONS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, getTestContext());
    }

    public void testMDDimensionsShared() throws Exception {
        String requestType = "MDSCHEMA_DIMENSIONS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, "");
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, getTestContext());
    }

    public void testMDFunction() throws Exception {
        String requestType = "MDSCHEMA_FUNCTIONS";
        String restrictionName = "FUNCTION_NAME";
        String restrictionValue = "Item";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(RESTRICTION_NAME_PROP, restrictionName);
        props.setProperty(RESTRICTION_VALUE_PROP, restrictionValue);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    /**
     * Tests the output of the MDSCHEMA_FUNCTIONS call in JDK 1.5 or later. In
     * JDK 1.4, does nothing and trivially succeeds.
     * See {@link #testMDFunctionsJdk14()}.
     *
     * @throws Exception on error
     */
    public void testMDFunctions() throws Exception {
        if (Util.PreJdk15 || Util.Retrowoven) {
            // MDSCHEMA_FUNCTIONS produces different output in JDK 1.4.
            return;
        }
        String requestType = "MDSCHEMA_FUNCTIONS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    /**
     * Tests the output of the MDSCHEMA_FUNCTIONS call in JDK 1.4, which is
     * different because metadata such as function name and description is
     * encoded using Java annotations, and hence is not available until
     * JDK 1.5. In JDK 1.5 and later, does nothing and trivially succeeds.
     * See {@link #testMDFunctions()}.
     *
     * @throws Exception on error
     */
    public void testMDFunctionsJdk14() throws Exception {
        if (!(Util.PreJdk15 || Util.Retrowoven)) {
            // MDSCHEMA_FUNCTIONS produces different output in JDK 1.4.
            return;
        }
        String requestType = "MDSCHEMA_FUNCTIONS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    // good 2/25 : (partial implementation)
    public void testMDHierarchies() throws Exception {
        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            return;
        }
        String requestType = "MDSCHEMA_HIERARCHIES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    public TestContext getTestContext() {
        return TestContext.instance().legacy();
    }

    public void testMDLevels() throws Exception {
        String requestType = "MDSCHEMA_LEVELS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(UNIQUE_NAME_PROP, "[Customers]");
        props.setProperty(UNIQUE_NAME_ELEMENT, "DIMENSION_UNIQUE_NAME");
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    public void testMDLevelsAccessControlled() throws Exception {
        String requestType = "MDSCHEMA_LEVELS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(UNIQUE_NAME_PROP, "[Customers]");
        props.setProperty(UNIQUE_NAME_ELEMENT, "DIMENSION_UNIQUE_NAME");
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        // TestContext which operates in a different Role.
        TestContext testContext =
            getTestContext().withRole("California manager");
        doTest(requestType, props, testContext);
    }

    public void testMDMeasures() throws Exception {
        String requestType = "MDSCHEMA_MEASURES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        // not used here
        props.setProperty(UNIQUE_NAME_PROP, "[Customers]");
        props.setProperty(UNIQUE_NAME_ELEMENT, "MEASURE_UNIQUE_NAME");

        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    public void testMDMembers() throws Exception {
        String requestType = "MDSCHEMA_MEMBERS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(UNIQUE_NAME_PROP, "[Gender]");
        props.setProperty(UNIQUE_NAME_ELEMENT, "HIERARCHY_UNIQUE_NAME");
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    public void testMDMembersMulti() throws Exception {
        String requestType = "MDSCHEMA_MEMBERS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    public void testMDMembersTreeop() throws Exception {
        String requestType = "MDSCHEMA_MEMBERS";

        // Treeop 34 = Ancestors | Siblings
        // MEMBER_UNIQUE_NAME = [USA].[OR]
        // Hence should return {[All], [USA], [USA].[CA], [USA].[WA]}
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    public void testMDProperties() throws Exception {
        String requestType = "MDSCHEMA_PROPERTIES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    public void testApproxRowCountOverridesCountCallsToDatabase()
        throws Exception
    {
        String requestType = "MDSCHEMA_LEVELS";
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(UNIQUE_NAME_PROP, "[Marital Status]");
        props.setProperty(UNIQUE_NAME_ELEMENT, "DIMENSION_UNIQUE_NAME");
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    public void testApproxRowCountInHierarchyOverridesCountCallsToDatabase()
        throws Exception
    {
        String requestType = "MDSCHEMA_HIERARCHIES";
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(UNIQUE_NAME_PROP, "[Marital Status]");
        props.setProperty(UNIQUE_NAME_ELEMENT, "DIMENSION_UNIQUE_NAME");
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    /**
     * Tests an 'DRILLTHROUGH SELECT' statement with a 'MAXROWS' clause.
     *
     * @throws Exception on error
     */
    public void testDrillThroughMaxRows() throws Exception {
        // NOTE: this test uses the filter method to adjust the expected result
        // for different databases
        if (!MondrianProperties.instance().EnableTotalCount.booleanValue()) {
            return;
        }
        String requestType = "EXECUTE";
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    /**
     * Tests an 'DRILLTHROUGH SELECT' statement with no 'MAXROWS' clause.
     *
     * @throws Exception on error
     */
    public void testDrillThrough() throws Exception {
        // NOTE: this test uses the filter method to adjust the expected result
        // for different databases
        if (!MondrianProperties.instance().EnableTotalCount.booleanValue()) {
            return;
        }
        String requestType = "EXECUTE";
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    /**
     * Tests an 'DRILLTHROUGH SELECT' statement with a zero-dimensional query,
     * that is, a query with 'SELECT FROM', and no axes.
     *
     * @throws Exception on error
     */
    public void testDrillThroughZeroDimensionalQuery() throws Exception {
        // NOTE: this test uses the filter method to adjust the expected result
        // for different databases
        if (!MondrianProperties.instance().EnableTotalCount.booleanValue()) {
            return;
        }
        String requestType = "EXECUTE";
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, getTestContext());
    }

    protected String filter(
        String testCaseName,
        String filename,
        String content)
    {
        if (testCaseName.startsWith("testDrillThrough")
            && filename.equals("response"))
        {
            // Different databases have slightly different column types, which
            // results in slightly different inferred xml schema for the drill-
            // through result.
            Dialect dialect = getTestContext().getDialect();
            switch (dialect.getDatabaseProduct()) {
            case ORACLE:
                content = Util.replace(
                    content,
                    " type=\"xsd:double\"",
                    " type=\"xsd:decimal\"");
                content = Util.replace(
                    content,
                    " type=\"xsd:integer\"",
                    " type=\"xsd:decimal\"");
                break;
            case POSTGRESQL:
                content = Util.replace(
                    content,
                    " sql:field=\"Store Sqft\" type=\"xsd:double\"",
                    " sql:field=\"Store Sqft\" type=\"xsd:integer\"");
                content = Util.replace(
                    content,
                    " sql:field=\"Unit Sales\" type=\"xsd:double\"",
                    " sql:field=\"Unit Sales\" type=\"xsd:decimal\"");
                break;
            case DERBY:
            case HSQLDB:
            case INFOBRIGHT:
            case LUCIDDB:
            case MYSQL:
            case NEOVIEW:
            case NETEZZA:
            case TERADATA:
                content = Util.replace(
                    content,
                    " sql:field=\"Store Sqft\" type=\"xsd:double\"",
                    " sql:field=\"Store Sqft\" type=\"xsd:integer\"");
                content = Util.replace(
                    content,
                    " sql:field=\"Unit Sales\" type=\"xsd:double\"",
                    " sql:field=\"Unit Sales\" type=\"xsd:string\"");
                content = Util.replace(
                    content,
                    " sql:field=\"Week\" type=\"xsd:decimal\"",
                    " sql:field=\"Week\" type=\"xsd:integer\"");
                content = Util.replace(
                    content,
                    " sql:field=\"Day\" type=\"xsd:decimal\"",
                    " sql:field=\"Day\" type=\"xsd:integer\"");
                break;
            case ACCESS:
                content = Util.replace(
                    content,
                    " sql:field=\"Week\" type=\"xsd:decimal\"",
                    " sql:field=\"Week\" type=\"xsd:double\"");
                content = Util.replace(
                    content,
                    " sql:field=\"Day\" type=\"xsd:decimal\"",
                    " sql:field=\"Day\" type=\"xsd:integer\"");
                break;
            }
        }
        return content;
    }

    public void testExecuteSlicer() throws Exception {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    public void testExecuteSlicerJson() throws Exception {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    public void testExecuteSlicer_ContentDataOmitDefaultSlicer()
        throws Exception
    {
        doTestExecuteContent(XmlaConstants.Content.DataOmitDefaultSlicer);
    }

    public void testExecuteNoSlicer_ContentDataOmitDefaultSlicer()
        throws Exception
    {
        doTestExecuteContent(XmlaConstants.Content.DataOmitDefaultSlicer);
    }

    public void testExecuteSlicer_ContentDataIncludeDefaultSlicer()
        throws Exception
    {
        doTestExecuteContent(XmlaConstants.Content.DataIncludeDefaultSlicer);
    }

    public void testExecuteNoSlicer_ContentDataIncludeDefaultSlicer()
        throws Exception
    {
        doTestExecuteContent(XmlaConstants.Content.DataIncludeDefaultSlicer);
    }

    public void testExecuteEmptySlicer_ContentDataIncludeDefaultSlicer()
        throws Exception
    {
        doTestExecuteContent(XmlaConstants.Content.DataIncludeDefaultSlicer);
    }

    public void testExecuteEmptySlicer_ContentDataOmitDefaultSlicer()
        throws Exception
    {
        doTestExecuteContent(XmlaConstants.Content.DataOmitDefaultSlicer);
    }

    public void testExecuteWithoutCellProperties() throws Exception
    {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    public void testExecuteWithCellProperties()
            throws Exception
    {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    public void testExecuteWithMemberKeyDimensionPropertyForMemberWithoutKey()
        throws Exception
    {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    public void testExecuteWithMemberKeyDimensionPropertyForMemberWithKey()
        throws Exception
    {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    public void testExecuteWithMemberKeyDimensionPropertyForAllMember()
        throws Exception
    {
        String requestType = "EXECUTE";
        final Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    public void testExecuteWithKeyDimensionProperty()
        throws Exception
    {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    public void testExecuteWithDimensionProperties()
        throws Exception
    {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, getTestContext());
    }

    /**
     * Testcase for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-257">
     * MONDRIAN-257, "Crossjoin gives 'Execute unparse results' error in
     * XMLA"</a>.
     */
    public void testExecuteCrossjoin() throws Exception {
        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            return;
        }
        String requestType = "EXECUTE";
        String query =
            "SELECT CrossJoin({[Product].[All Products].children}, "
            + "{[Customers].[All Customers].children}) ON columns FROM Sales";
        String request =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<soapenv:Envelope\n"
            + "    xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\"\n"
            + "    xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"\n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "    <soapenv:Body>\n"
            + "        <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\">\n"
            + "        <Command>\n"
            + "        <Statement>\n"
            + query + "\n"
            + "         </Statement>\n"
            + "        </Command>\n"
            + "        <Properties>\n"
            + "          <PropertyList>\n"
            + "            <Catalog>${catalog}</Catalog>\n"
            + "            <DataSourceInfo>${data.source.info}</DataSourceInfo>\n"
            + "            <Format>${format}</Format>\n"
            + "            <AxisFormat>TupleFormat</AxisFormat>\n"
            + "          </PropertyList>\n"
            + "        </Properties>\n"
            + "</Execute>\n"
            + "</soapenv:Body>\n"
            + "</soapenv:Envelope>";
        Properties props = getDefaultRequestProperties(requestType);
        doTestInline(
            requestType, request, "response", props, getTestContext());
    }

    /**
     * This test returns the same result as testExecuteCrossjoin above
     * except that the Role used disables accessing
     * [Customers].[All Customers].[Mexico].
     */
    public void testExecuteCrossjoinRole() throws Exception {
        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            return;
        }
        String requestType = "EXECUTE";
        String query =
            "SELECT CrossJoin({[Product].[All Products].children}, "
            + "{[Customers].[All Customers].children}) ON columns FROM Sales";
        String request =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<soapenv:Envelope\n"
            + "    xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\"\n"
            + "    xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"\n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "    <soapenv:Body>\n"
            + "        <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\">\n"
            + "        <Command>\n"
            + "        <Statement>\n"
            + query + "\n"
            + "         </Statement>\n"
            + "        </Command>\n"
            + "        <Properties>\n"
            + "          <PropertyList>\n"
            + "            <Catalog>${catalog}</Catalog>\n"
            + "            <DataSourceInfo>${data.source.info}</DataSourceInfo>\n"
            + "            <Format>${format}</Format>\n"
            + "            <Role>${format}</Role>\n"
            + "            <AxisFormat>TupleFormat</AxisFormat>\n"
            + "          </PropertyList>\n"
            + "        </Properties>\n"
            + "</Execute>\n"
            + "</soapenv:Body>\n"
            + "</soapenv:Envelope>";

        class RR implements Role {
            public RR() {
            }

            public Access getAccess(Cube cube) {
                return Access.ALL;
            }

            public Access getAccess(NamedSet set) {
                return Access.ALL;
            }

            public boolean canAccess(OlapElement olapElement) {
                return true;
            }

            public Access getAccess(Schema schema) {
                return Access.ALL;
            }

            public Access getAccess(Dimension dimension) {
                return Access.ALL;
            }

            public Access getAccess(Hierarchy hierarchy) {
                String mname = "[Customers]";
                if (hierarchy.getUniqueName().equals(mname)) {
                    return Access.CUSTOM;
                } else {
                    return Access.ALL;
                }
            }

            public HierarchyAccess getAccessDetails(Hierarchy hierarchy) {
                String hname = "[Customers]";
                if (hierarchy.getUniqueName().equals(hname)) {
                    return new HierarchyAccess() {
                        public Access getAccess(Member member) {
                            String mname =
                                "[Customers].[Mexico]";
                            if (member.getUniqueName().equals(mname)) {
                                return Access.NONE;
                            } else {
                                return Access.ALL;
                            }
                        }

                        public int getTopLevelDepth() {
                            return 0;
                        }

                        public int getBottomLevelDepth() {
                            return 4;
                        }

                        public RollupPolicy getRollupPolicy() {
                            return RollupPolicy.FULL;
                        }

                        public boolean hasInaccessibleDescendants(
                            Member member)
                        {
                            return false;
                        }
                    };

                } else {
                    return RoleImpl.createAllAccess(hierarchy);
                }
            }

            public Access getAccess(Level level) {
                return Access.ALL;
            }

            public Access getAccess(Member member) {
                String mname = "[Customers].[All Customers]";
                if (member.getUniqueName().equals(mname)) {
                    return Access.ALL;
                } else {
                    return Access.ALL;
                }
            }
        }

        Role role = new RR();

        Properties props = getDefaultRequestProperties(requestType);
        doTestInline(
            requestType, request, "response",
            props, getTestContext(), role);
    }

    public void testExecuteBugMondrian1316() throws Exception {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, TestContext.instance());
    }

    public void doTestRT(String requestType, TestContext testContext)
        throws Exception
    {
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, testContext);
    }

    private void doTestExecuteContent(
        XmlaConstants.Content content)
        throws Exception
    {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        String requestText = fileToString("request");
        TestContext testContext = getTestContext();
        requestText = testContext.upgradeQuery(requestText);
        Document responseDoc = fileToDocument("response", props);

        String connectString = testContext.getConnectString();
        Map<String, String> catalogNameUrls = getCatalogNameUrls(testContext);

        Document expectedDoc;

        final String ns = "cxmla";
        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc,
                new String[][] {{"content", content.name()}}, ns)
            : null;
        doTests(
            requestText, props,
            testContext, connectString, catalogNameUrls,
            expectedDoc, content, null, true);
    }

    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }
}

// End XmlaBasicTest.java
