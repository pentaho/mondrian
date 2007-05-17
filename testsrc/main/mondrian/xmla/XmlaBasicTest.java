/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.olap.MondrianProperties;
import mondrian.test.TestContext;
import mondrian.test.DiffRepository;
import mondrian.tui.XmlUtil;
import mondrian.tui.XmlaSupport;

import org.custommonkey.xmlunit.XMLAssert;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.Properties;
import java.util.Map;

import junit.framework.AssertionFailedError;

/**
 * Test XML/A functionality.
 *
 * @author Richard M. Emberson
 * @version $Id$
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
    public static final String CONTENT_NONE     =
                Enumeration.Content.None.name();
    public static final String CONTENT_DATA     =
                Enumeration.Content.Data.name();
    public static final String CONTENT_SCHEMA   =
                Enumeration.Content.Schema.name();
    public static final String CONTENT_SCHEMADATA =
                Enumeration.Content.SchemaData.name();

    public XmlaBasicTest() {
    }

    public XmlaBasicTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
    }

    protected void tearDown() throws Exception {
    }

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaBasicTest.class);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return null;
    }

    protected String extractSoapResponse(
            Document responseDoc,
            Enumeration.Content content) {
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
        doTestRT(requestType, TestContext.instance());
    }

    public void testDEnumerators() throws Exception {
        String requestType = "DISCOVER_ENUMERATORS";
        doTestRT(requestType, TestContext.instance());
    }

    public void testDKeywords() throws Exception {
        String requestType = "DISCOVER_KEYWORDS";
        doTestRT(requestType, TestContext.instance());
    }

    public void testDLiterals() throws Exception {
        String requestType = "DISCOVER_LITERALS";
        doTestRT(requestType, TestContext.instance());
    }

    public void testDProperties() throws Exception {
        String requestType = "DISCOVER_PROPERTIES";
        doTestRT(requestType, TestContext.instance());
    }

    public void testDSchemaRowsets() throws Exception {
        String requestType = "DISCOVER_SCHEMA_ROWSETS";
        doTestRT(requestType, TestContext.instance());
    }

    /////////////////////////////////////////////////////////////////////////
    // DBSCHEMA
    /////////////////////////////////////////////////////////////////////////

    public void testDBCatalogs() throws Exception {
        String requestType = "DBSCHEMA_CATALOGS";
        doTestRT(requestType, TestContext.instance());
    }
    // passes 2/25 - I think that this is good but not sure
    public void _testDBColumns() throws Exception {
        String requestType = "DBSCHEMA_COLUMNS";
        doTestRT(requestType, TestContext.instance());
    }
    // passes 2/25 - I think that this is good but not sure
    public void _testDBProviderTypes() throws Exception {
        String requestType = "DBSCHEMA_PROVIDER_TYPES";
        doTestRT(requestType, TestContext.instance());
    }
    // passes 2/25 - I think that this is good but not sure
    // Should this even be here
    public void _testDBTablesInfo() throws Exception {
        String requestType = "DBSCHEMA_TABLES_INFO";
        doTestRT(requestType, TestContext.instance());
    }
    // passes 2/25 - I think that this is good but not sure
    public void testDBTables() throws Exception {
        String requestType = "DBSCHEMA_TABLES";
        doTestRT(requestType, TestContext.instance());
    }

    /////////////////////////////////////////////////////////////////////////
    // MDSCHEMA
    /////////////////////////////////////////////////////////////////////////

    public void testMDCubes() throws Exception {
        String requestType = "MDSCHEMA_CUBES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, TestContext.instance());
    }

    public void testMDSets() throws Exception {
        String requestType = "MDSCHEMA_SETS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, TestContext.instance());
    }

    public void testMDimensions() throws Exception {
        String requestType = "MDSCHEMA_DIMENSIONS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, props, TestContext.instance());
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

        doTest(requestType, props, TestContext.instance());
    }

    // only make sure that something is returned
    public void testMDFunctions() throws Exception {
        String requestType = "MDSCHEMA_FUNCTIONS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, TestContext.instance());
    }

    // good 2/25 : (partial implementation)
    public void testMDHierarchies() throws Exception {
        String requestType = "MDSCHEMA_HIERARCHIES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, TestContext.instance());
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

        doTest(requestType, props, TestContext.instance());
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
        TestContext testContext = TestContext.createInRole("California manager");
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

        doTest(requestType, props, TestContext.instance());
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

        doTest(requestType, props, TestContext.instance());
    }

    public void testMDProperties() throws Exception {
        String requestType = "MDSCHEMA_PROPERTIES";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, TestContext.instance());
    }

    public void testApproxRowCountOverridesCountCallsToDatabase() throws Exception {
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

        doTest(requestType, props, TestContext.instance());
    }

    public void testApproxRowCountInHierarchyOverridesCountCallsToDatabase() throws Exception {
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

        doTest(requestType, props, TestContext.instance());
    }

    public void testDrillThrough() throws Exception {
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

        doTest(requestType, props, TestContext.instance());
    }

    public void testExecuteSlicer() throws Exception {
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);

        doTest(requestType, props, TestContext.instance());
    }
    public void testExecuteWithoutCellProperties() throws Exception {
        String requestType = "EXECUTE";

        doTest(requestType, getDefaultRequestProperties(requestType), TestContext.instance());
    }

    public void testExecuteWithCellProperties()
            throws Exception {
        String requestType = "EXECUTE";

        doTest(requestType, getDefaultRequestProperties(requestType), TestContext.instance());
    }
    
    public void testExecuteWithMemberKeyDimensionProperty()
                throws Exception {
            String requestType = "EXECUTE";

            doTest(requestType, getDefaultRequestProperties(requestType), TestContext.instance());
        }
    public void testExecuteWithDimensionProperties()
                throws Exception {
            String requestType = "EXECUTE";

            doTest(requestType, getDefaultRequestProperties(requestType), TestContext.instance());
        }

    // Testcase for bug 1653587.
    public void testExecuteCrossjoin() throws Exception {
       String requestType = "EXECUTE";
        String query = "SELECT CrossJoin({[Product].[All Products].children}, {[Customers].[All Customers].children}) ON columns FROM Sales";
        String request = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
           "<soapenv:Envelope\n" +
           "    xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\"\n" +
           "    xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"\n" +
           "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n" +
           "    <soapenv:Body>\n" +
           "        <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\">\n" +
           "        <Command>\n" +
           "        <Statement>\n" +
           query + "\n" +
           "         </Statement>\n" +
           "        </Command>\n" +
           "        <Properties>\n" +
           "          <PropertyList>\n" +
           "            <Catalog>${catalog}</Catalog>\n" +
           "            <DataSourceInfo>${data.source.info}</DataSourceInfo>\n" +
           "            <Format>${format}</Format>\n" +
           "            <AxisFormat>TupleFormat</AxisFormat>\n" +
           "          </PropertyList>\n" +
           "        </Properties>\n" +
           "</Execute>\n" +
           "</soapenv:Body>\n" +
           "</soapenv:Envelope>";
        Properties props = getDefaultRequestProperties(requestType);
       doTestInline(
           requestType, request, "${response}", props, TestContext.instance());
   }

    /*
     * NOT IMPLEMENTED MDSCHEMA_SETS_out.xml
     */

    public void doTestRT(String requestType, TestContext testContext) throws Exception {

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, props, testContext);
    }

    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }
}

// End XmlaBasicTest.java
