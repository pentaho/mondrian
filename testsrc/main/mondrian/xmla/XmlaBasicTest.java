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
                Enumeration.Content.None.name();
    public static final String CONTENT_DATA     =
                Enumeration.Content.Data.name();
    public static final String CONTENT_SCHEMA   =
                Enumeration.Content.Schema.name();
    public static final String CONTENT_SCHEMADATA =
                Enumeration.Content.SchemaData.name();

    private static final boolean DEBUG = false;

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

    protected Document fileToDocument(String filename)
                throws IOException , SAXException {
        String s = getDiffRepos().expand(null, filename);
        if (s.equals(filename)) {
            s = "<?xml version='1.0'?><Empty/>";
        }
        Document doc = XmlUtil.parse(new ByteArrayInputStream(s.getBytes()));
        return doc;
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

    private Properties getDefaultRequestProperties(String requestType) {
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(CATALOG_PROP, CATALOG);
        props.setProperty(CATALOG_NAME_PROP, CATALOG);
        props.setProperty(CUBE_NAME_PROP, SALES_CUBE);
        props.setProperty(FORMAT_PROP, FORMAT_MULTI_DIMENSIONAL);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        return props;
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

    /**
     * Executes an XMLA request, reading the text of the request and the
     * response from attributes in {@link #getDiffRepos()}.
     *
     * @param requestType Request type: "DISCOVER_DATASOURCES", "EXECUTE", etc.
     * @param props Properties for request
     * @param testContext Test context
     */
    public void doTest(
        String requestType,
        Properties props,
        TestContext testContext) throws Exception {

        String requestText = fileToString("request");
        doTestInline(
            requestType, requestText, "${response}", props, testContext);
    }

    public void doTestInline(
        String requestType,
        String requestText,
        String respFileName,
        Properties props,
        TestContext testContext)
        throws Exception
    {
        Document responseDoc = (respFileName != null)
            ? fileToDocument(respFileName)
            : null;

        String connectString = testContext.getConnectString();
        Map<String,String> catalogNameUrls = getCatalogNameUrls(testContext);

        Document expectedDoc;

        // Test 'schemadata' first, so that if it fails, we will be able to
        // amend the ref file with the fullest XML response.
        final String ns = "cxmla";
        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schemadata"}}, ns)
            : null;
        doTests(
            requestText, props, null, connectString, catalogNameUrls,
            expectedDoc, CONTENT_SCHEMADATA);

        if (requestType.equals("EXECUTE")) {
            return;
        }

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "none"}}, ns)
            : null;

        doTests(requestText, props, null, connectString, catalogNameUrls,
                expectedDoc, CONTENT_NONE);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "data"}}, ns)
            : null;
        doTests(requestText, props, null, connectString, catalogNameUrls,
                expectedDoc, CONTENT_DATA);

        expectedDoc = (responseDoc != null)
            ? XmlaSupport.transformSoapXmla(
                responseDoc, new String[][] {{"content", "schema"}}, ns)
            : null;
        doTests(requestText, props, null, connectString, catalogNameUrls,
                expectedDoc, CONTENT_SCHEMA);
    }

    protected void doTests(
            String soapRequestText,
            Properties props,
            String soapResponseText,
            String connectString,
            Map<String, String> catalogNameUrls,
            Document expectedDoc,
            String content) throws Exception {

        if (content != null) {
            props.setProperty(CONTENT_PROP, content);
        }
        soapRequestText = Util.replaceProperties(
            soapRequestText, Util.toMap(props));

if (DEBUG) {
System.out.println("soapRequestText="+soapRequestText);
}
        Document soapReqDoc = XmlUtil.parseString(soapRequestText);

        Document xmlaReqDoc = XmlaSupport.extractBodyFromSoap(soapReqDoc);

        // do XMLA
        byte[] bytes =
            XmlaSupport.processXmla(xmlaReqDoc, connectString, catalogNameUrls);
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
        bytes = XmlaSupport.processSoapXmla(soapReqDoc, connectString, catalogNameUrls, null);
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

        Document gotDoc = ignoreLastUpdateDate(XmlUtil.parse(bytes));
        String gotStr = XmlUtil.toString(gotDoc, true);
        gotStr = Util.maskVersion(gotStr);
        if (expectedDoc != null) {
            String expectedStr = XmlUtil.toString(expectedDoc, true);
if (DEBUG) {
System.out.println("GOT:\n"+gotStr);
System.out.println("EXPECTED:\n"+expectedStr);
System.out.println("XXXXXXX");
}
            try {
                XMLAssert.assertXMLEqual(expectedStr, gotStr);
            } catch (AssertionFailedError e) {
                if (content.equals(CONTENT_SCHEMADATA)) {
                    // Let DiffRepository do the comparison. It will output
                    // a textual difference, and will update the logfile,
                    // XmlaBasicTest.log.xml. If you agree with the change,
                    // copy this file to XmlaBasicTest.ref.xml.
                    getDiffRepos().assertEquals("response", "${response}", gotStr);
                } else {
                    throw e;
                }
            }
        } else {
            if (content.equals(CONTENT_SCHEMADATA)) {
                getDiffRepos().amend("${response}", gotStr);
            }
        }
    }

    private Document ignoreLastUpdateDate(Document document) {
		NodeList elements = document.getElementsByTagName("LAST_SCHEMA_UPDATE");
		for (int i = elements.getLength(); i > 0; i--) {
			removeNode(elements.item(i-1));
		}
		return document;
	}

	private void removeNode(Node node) {
		Node parentNode = node.getParentNode();
		parentNode.removeChild(node);
	}

    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }
}

// End XmlaBasicTest.java
