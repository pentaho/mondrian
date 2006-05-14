/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.tui.XmlUtil;
import mondrian.tui.XmlaSupport;

import org.custommonkey.xmlunit.XMLAssert;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Test XML/A functionality.
 *
 * @author Richard M. Emberson<
 * @version $Id$
 */
public class XmlaBasicTest extends FoodMartTestCase {
    // request.type
    public static final String REQUEST_TYPE_PROP = "request.type";

    // data.source.info
    public static final String DATA_SOURCE_INFO_PROP = "data.source.info";
    public static final String DATA_SOURCE_INFO = "MondrianFoodMart";

    // catalog
    public static final String CATALOG_PROP     = "catalog";
    public static final String CATALOG_NAME_PROP= "catalog.name";
    public static final String CATALOG          = "FoodMart";

    // cube
    public static final String CUBE_NAME_PROP   = "cube.name";
    public static final String SALES_CUBE       = "Sales";

    // format
    public static final String FORMAT_PROP     = "format";
    public static final String FORMAT_TABLULAR = "Tabular";
    public static final String FORMAT_MULTI_DIMENSIONAL = "Multidimensional";

    // unique name
    public static final String UNIQUE_NAME_ELEMENT    = "unique.name.element";

    // dimension unique name
    public static final String UNIQUE_NAME_PROP     = "unique.name";

    public static final String RESTRICTION_NAME_PROP     = "restriction.name";
    public static final String RESTRICTION_VALUE_PROP     = "restriction.value";

    // content
    public static final String CONTENT_PROP     = "content";
    public static final String CONTENT_NONE     =
                Enumeration.Content.None.getName();
    public static final String CONTENT_DATA     =
                Enumeration.Content.Data.getName();
    public static final String CONTENT_SCHEMA   =
                Enumeration.Content.Schema.getName();
    public static final String CONTENT_SCHEMADATA =
                Enumeration.Content.SchemaData.getName();

    private static final String XMLA_DIRECTORY = "testsrc/main/mondrian/xmla/";

    private static final boolean DEBUG = false;

    protected File testDir;

    public XmlaBasicTest() {
    }
    public XmlaBasicTest(String name) {
        super(name);
    }


    protected void setUp() throws Exception {
        testDir = new File(XMLA_DIRECTORY + "/basic");
    }
    protected void tearDown() throws Exception {
    }


    protected String fileToString(String filename) throws IOException {
        File file = new File(testDir, filename);
        String requestText = XmlaSupport.readFile(file);
        return requestText;
    }

    protected Document fileToDocument(String filename)
                throws IOException , SAXException {
        File file = new File(testDir, filename);
        Document doc = XmlUtil.parse(file);
        return doc;
    }

    protected String extractSoapResponse(
            Document responseDoc,
            Enumeration.Content content) {
        Document partialDoc = null;
        switch (content.getOrdinal()) {
        case Enumeration.Content.NONE_ORDINAL:
            // return soap and no content
            break;

        case Enumeration.Content.SCHEMA_ORDINAL:
            // return soap plus scheam content
            break;

        case Enumeration.Content.DATA_ORDINAL:
            // return soap plus data content
            break;

        case Enumeration.Content.SCHEMA_DATA_ORDINAL:
            // return everything
            partialDoc = responseDoc;
            break;
        }

        String responseText = XmlUtil.toString(responseDoc, false);
        return responseText;
    }

    public TestContext getTestContext() {
        return TestContext.instance();
    }
    protected String getConnectionString() {
        return getTestContext().getConnectString();
    }

    /////////////////////////////////////////////////////////////////////////
    // DISCOVER
    /////////////////////////////////////////////////////////////////////////
    // good 2/25
    public void testDDatasource() throws Exception {
        String requestType = "DISCOVER_DATASOURCES";
        String reqFileName = "RT_C_in.xml";
        String respFileName = "DISCOVER_DATASOURCES_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // good 2/25
    public void testDEnumerators() throws Exception {
        String requestType = "DISCOVER_ENUMERATORS";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DISCOVER_ENUMERATORS_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // good 2/25
    public void testDKeywords() throws Exception {
        String requestType = "DISCOVER_KEYWORDS";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DISCOVER_KEYWORDS_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // good 2/25
    public void testDLiterals() throws Exception {
        String requestType = "DISCOVER_LITERALS";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DISCOVER_LITERALS_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // good 2/25
    public void testDProperties() throws Exception {
        String requestType = "DISCOVER_PROPERTIES";
        String reqFileName = "RT_C_in.xml";
        String respFileName = "DISCOVER_PROPERTIES_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // good 2/25
    public void testDSchemaRowsets() throws Exception {
        String requestType = "DISCOVER_SCHEMA_ROWSETS";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DISCOVER_SCHEMA_ROWSETS_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }

    /////////////////////////////////////////////////////////////////////////
    // DBSCHEMA
    /////////////////////////////////////////////////////////////////////////
    // good 2/25
    public void testDBCatalogs() throws Exception {
        String requestType = "DBSCHEMA_CATALOGS";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DBSCHEMA_CATALOGS_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // passes 2/25 - I think that this is good but not sure
    public void _testDBColumns() throws Exception {
        String requestType = "DBSCHEMA_COLUMNS";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DBSCHEMA_COLUMNS_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // passes 2/25 - I think that this is good but not sure
    public void _testDBProviderTypes() throws Exception {
        String requestType = "DBSCHEMA_PROVIDER_TYPES";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DBSCHEMA_PROVIDER_TYPES_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // passes 2/25 - I think that this is good but not sure
    // Should this even be here
    public void _testDBTablesInfo() throws Exception {
        String requestType = "DBSCHEMA_TABLES_INFO";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DBSCHEMA_TABLES_INFO_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }
    // passes 2/25 - I think that this is good but not sure
    public void testDBTables() throws Exception {
        String requestType = "DBSCHEMA_TABLES";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "DBSCHEMA_TABLES_out.xml";
        doTestRT(requestType, reqFileName, respFileName);
    }

    /////////////////////////////////////////////////////////////////////////
    // MDSCHEMA
    /////////////////////////////////////////////////////////////////////////
    // good 2/25
    public void testMDCubes() throws Exception {
        String requestType = "MDSCHEMA_CUBES";
        String reqFileName = "RT_DSI_C_F_C_in.xml";
        String respFileName = "MDSCHEMA_CUBES_out.xml";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, reqFileName, respFileName, props);
    }
    // good 2/25
    public void testMDimensions() throws Exception {
        String requestType = "MDSCHEMA_DIMENSIONS";
        String reqFileName = "RT_DSI_C_F_C_in.xml";
        String respFileName = "MDSCHEMA_DIMENSIONS_out.xml";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);

        doTest(requestType, reqFileName, respFileName, props);
    }

    // good 4/21
    public void testMDFunction() throws Exception {
        String requestType = "MDSCHEMA_FUNCTIONS";
        String reqFileName = "RT_R_DSI_C_in.xml";
        String restrictionName = "FUNCTION_NAME";
        String restrictionValue = "Item";
        String respFileName = "MDSCHEMA_FUNCTIONS_out.xml";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(RESTRICTION_NAME_PROP, restrictionName);
        props.setProperty(RESTRICTION_VALUE_PROP, restrictionValue);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, reqFileName, respFileName, props);
    }
    // good 4/21
    // only make sure that something is returned
    public void testMDFunctions() throws Exception {
        String requestType = "MDSCHEMA_FUNCTIONS";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = null;

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, reqFileName, respFileName, props);
    }

    // good 2/25 : (partial implementation)
    public void testMDHierarchies() throws Exception {
        String requestType = "MDSCHEMA_HIERARCHIES";
        String reqFileName = "RT_C_CN_DSI_in.xml";
        String respFileName = "MDSCHEMA_HIERARCHIES_out.xml";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, reqFileName, respFileName, props);
    }
    // good 2/25 : (partial implementation)
    public void testMDLevels() throws Exception {
        String requestType = "MDSCHEMA_LEVELS";
        String reqFileName = "RT_C_CN_DSI_C_F_C_in.xml";
        String respFileName = "MDSCHEMA_LEVELS_out.xml";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(UNIQUE_NAME_PROP, "[Customers]");
        props.setProperty(UNIQUE_NAME_ELEMENT, "DIMENSION_UNIQUE_NAME");
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, reqFileName, respFileName, props);
    }
    // good 2/25
    public void testMDMeasures() throws Exception {
        String requestType = "MDSCHEMA_MEASURES";
        String reqFileName = "MDSCHEMA_MEASURES_in.xml";
        String respFileName = "MDSCHEMA_MEASURES_out.xml";

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

        doTest(requestType, reqFileName, respFileName, props);
    }
    // good 2/25
    public void testMDMembers() throws Exception {
        String requestType = "MDSCHEMA_MEMBERS";
        String reqFileName = "RT_C_CN_DSI_C_F_C_in.xml";
        String respFileName = "MDSCHEMA_MEMBERS_out.xml";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_TABLULAR);
        props.setProperty(UNIQUE_NAME_PROP, "[Gender]");
        props.setProperty(UNIQUE_NAME_ELEMENT, "HIERARCHY_UNIQUE_NAME");
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, reqFileName, respFileName, props);
    }

    // good 2/25
    public void testMDProperties() throws Exception {
        String requestType = "MDSCHEMA_PROPERTIES";
        String reqFileName = "RT_DSI_C_in.xml";
        String respFileName = "MDSCHEMA_PROPERTIES_out.xml";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, reqFileName, respFileName, props);
    }

    /*
     * NOT IMPLEMENTED MDSCHEMA_SETS_out.xml
     */

    public void doTestRT(
            String requestType,
            String reqFileName,
            String respFileName) throws Exception {

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        doTest(requestType, reqFileName, respFileName, props);

    }
    public void doTest(
            String requestType,
            String reqFileName,
            String respFileName,
            Properties props) throws Exception {

        String requestText = fileToString(reqFileName);
        Document responseDoc = (respFileName != null)
            ? fileToDocument(respFileName)
            : null;

        String connectString = getConnectionString();

        Document expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "none"}} )
            : null;

        props.setProperty(CONTENT_PROP, CONTENT_NONE);
        doTests(requestText, props, null, connectString, expectedDoc);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "data"}} )
            : null;
        props.setProperty(CONTENT_PROP, CONTENT_DATA);
        doTests(requestText, props, null, connectString, expectedDoc);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schema"}} )
            : null;
        props.setProperty(CONTENT_PROP, CONTENT_SCHEMA);
        doTests(requestText, props, null, connectString, expectedDoc);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schemadata"}} )
            : null;
        props.setProperty(CONTENT_PROP, CONTENT_SCHEMADATA);
        doTests(requestText, props, null, connectString, expectedDoc);
    }
    protected void doTests(
            String soapRequestText,
            Properties props,
            String soapResponseText,
            String connectString,
            Document expectedDoc) throws Exception {

        if (props != null) {
            soapRequestText = Util.replaceProperties(soapRequestText, props);
        }
if (DEBUG) {
System.out.println("soapRequestText="+soapRequestText);
}
        Document soapReqDoc = XmlUtil.parseString(soapRequestText);

        Document xmlaReqDoc = XmlaSupport.extractBodyFromSoap(soapReqDoc);

        // do XMLA
        byte[] bytes = XmlaSupport.processXmla(xmlaReqDoc, connectString);
        String response = new String(bytes);
if (DEBUG) {
System.out.println("xmla response="+response);
}
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateXmlaUsingXpath(bytes)) {
if (DEBUG) {
                System.out.println("XML Data is Valid");
}
            }
        }

        // do SOAP-XMLA
        bytes = XmlaSupport.processSoapXmla(soapReqDoc, connectString, null);
        response = new String(bytes);
if (DEBUG) {
System.out.println("soap response="+response);
}
        if (XmlUtil.supportsValidation()) {
            if (XmlaSupport.validateSoapXmlaUsingXpath(bytes)) {
if (DEBUG) {
                System.out.println("XML Data is Valid");
}
            }
        }

        Document gotDoc = XmlUtil.parse(bytes);
        String gotStr = XmlUtil.toString(gotDoc, true);
        if (expectedDoc != null) {
            String expectedStr = XmlUtil.toString(expectedDoc, true);
if (DEBUG) {
System.out.println("GOT:\n"+gotStr);
System.out.println("EXPECTED:\n"+expectedStr);
System.out.println("XXXXXXX");
}
            XMLAssert.assertXMLEqual(expectedStr, gotStr);
        }



/*
        if (! gotStr.equals(expectedStr)) {
System.out.println("GOT:\n"+gotStr);
System.out.println("EXPECTED:\n"+expectedStr);
        }
*/
    }

}

// End XmlaBasicTest.java
