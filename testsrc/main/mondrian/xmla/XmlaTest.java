/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.test.TestContext;
import mondrian.resource.MondrianResource;

import junit.framework.TestCase;
import org.eigenbase.xom.*;
import org.eigenbase.xom.Parser;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Unit test for Mondrian's XML for Analysis API (package
 * <code>mondrian.xmla</code>).
 *
 * @author jhyde
 * @since May 2, 2003
 * @version $Id$
 **/
public class XmlaTest extends TestCase {
    private static final String nl = System.getProperty("line.separator");
    private static String catalogName;
    private static String dataSource;

    /**
     * Usually null, when {@link #getRequests} sets it, {@link #executeRequest}
     * writes request strings into it and returns null.
     */
    private ArrayList requestList;

    private File dataDir;

    public XmlaTest(String s) {
        super(s);
    }

    protected void setUp() throws Exception {
        super.setUp();

        makeDataDir();

        // Deal with embedded & that can be in the JDBC URL
        String connectString = TestContext.getConnectString();
        dataSource = "MondrianFoodMart";
        catalogName = DriverManager.getConnection(connectString, null, false).getSchema().getName();

        connectString = connectString.replaceAll("&", "&amp;");

        StringReader dsConfigReader = new StringReader(
                "<?xml version=\"1.0\"?>" + nl +
                "<DataSources>" +
                "   <DataSource>" +
                "       <DataSourceName>MondrianFoodMart</DataSourceName>" +
                "       <DataSourceDescription>MondrianFoodMart</DataSourceDescription>" +
                "       <URL>http://localhost:8080/mondrian/xmla</URL>" +
                "       <DataSourceInfo>" + connectString + "</DataSourceInfo>" +
                "       <ProviderName>Mondrian</ProviderName>" +
                "       <ProviderType>MDP</ProviderType>" +
                "       <AuthenticationMode>Unauthenticated</AuthenticationMode>" +
                "   </DataSource>" +
                "</DataSources>");
        final Parser xmlParser = XOMUtil.createDefaultParser();
        final DOMWrapper def = xmlParser.parse(dsConfigReader);
        DataSourcesConfig.DataSources dataSources = new DataSourcesConfig.DataSources(def);
        XmlaMediator.initDataSourcesMap(dataSources);

    }

    /**
     * Executes a request and returns the result.
     *
     * <p>When called from {@link #getRequests}, {@link #requestList} is not
     * null, and this method behaves very differently: it records the request
     * and returns null.
     */
    private String executeRequest(String request) {
        // Run all tests in the US locale, not the system default locale,
        // because the results all assume the US locale.
        MondrianResource.setThreadLocale(Locale.US);
        if (requestList != null) {
            requestList.add(request);
            return null;
        }
        final StringWriter sw = new StringWriter();
        final XmlaMediator mediator = new XmlaMediator();
        mediator.process(request, sw);
        return sw.toString();
    }

    /**
     * Returns a list of all requests in this test. (These requests are used
     * in the sample web page, <code>xmlaTest.jsp</code>.)
     */
    public synchronized Map getRequests() {
        this.requestList = new ArrayList();
        HashMap mapNameToRequest = new HashMap();
        Method[] methods = getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            if (method.getName().startsWith("test")) {
                final int prevRequestCount = requestList.size();
                try {
                    method.invoke(this, null);
                } catch (Throwable e) {
                    // ignore
                }
                final int requestCount = requestList.size();
                if (requestCount > prevRequestCount) {
                    mapNameToRequest.put(method.getName(),
                            requestList.get(prevRequestCount));
                }
            }
        }
        this.requestList = null;
        return mapNameToRequest;
    }

    private void assertRequestYields(String request, String expected) {
        final String response = executeRequest(request);
        TestContext.assertEqualsVerbose(expected, response);
    }

    static String fold(String[] strings) {
        return TestContext.fold(strings);
    }

    private String xmlwrap(String msg) {
        if (msg.endsWith(nl)) {
            msg = msg.substring(0, msg.length() - nl.length());
        }
        return "<?xml version=\"1.0\"?>" + nl + wrap(msg);
    }
    private String wrap(String request) {
        request = Pattern.compile("^").matcher(request).replaceAll("        ");
        return "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                request + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>";
    }

    /**
     * Asserts that a string matches a pattern. If they are not
     * an AssertionFailedError is thrown.
     */
    static public void assertMatches(String message, String pattern, String actual) {
        if (actual != null && Pattern.matches(pattern, actual)) {
            return;
        }
        String formatted= "";
        if (message != null) {
            formatted= message+" ";
        }
        fail(formatted+"expected pattern:<"+pattern+"> but was:<"+actual+">");
    }

    private void assertRequestMatches(String request, String responsePattern) {
        final String response = executeRequest(request);
        assertMatches("Request " + request, responsePattern, response);
    }

    private void assertExceptionMatches(String request, String messagePattern) {
        try {
            executeRequest(request);
        } catch (Throwable t) {
            Throwable rootThrowable = t.getCause();
            while (rootThrowable != null && rootThrowable instanceof MondrianException) {
                t = rootThrowable;
                rootThrowable = rootThrowable.getCause();
            }

            if (Pattern.matches(messagePattern, t.getMessage()))
                return;
            else
                fail("expected exception with message '" + messagePattern + "', but was '" + t.getMessage() + "'");
        }
        fail("expected exception with message '" + messagePattern + "', but there is no exception thrown");
    }

    // tests follow

    public void testDiscoverDataSources() {
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>DISCOVER_DATASOURCES</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList/>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <Content>Data</Content>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            "          <row>",
            "            <DataSourceName>MondrianFoodMart</DataSourceName>",
            "            <DataSourceDescription>MondrianFoodMart</DataSourceDescription>",
            "            <URL>http://localhost:8080/mondrian/xmla</URL>",
            "            <DataSourceInfo>MondrianFoodMart</DataSourceInfo>",
            "            <ProviderName>Mondrian</ProviderName>",
            "            <ProviderType>MDP</ProviderType>",
            "            <AuthenticationMode>Unauthenticated</AuthenticationMode>",
            "          </row>",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    public void testDiscoverCatalogs() {
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>DBSCHEMA_CATALOGS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList/>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            "          <row>",
            "            <CATALOG_NAME>FoodMart</CATALOG_NAME>",
            "          </row>",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    public void testDiscoverCubes() {
String[] request = {
    "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
    "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
    "    <RequestType>MDSCHEMA_CUBES</RequestType>",
    "    <Restrictions>",
    "        <RestrictionList>",
    "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
    "        </RestrictionList>",
    "    </Restrictions>",
    "    <Properties>",
        "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String[] responsePattern = {
            "(?s).*",
            "          <row>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
    "            <CUBE_NAME>Store</CUBE_NAME>",
    "            <CUBE_TYPE>CUBE</CUBE_TYPE>",
    "            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>",
    "            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>",
    "            <IS_LINKABLE>false</IS_LINKABLE>",
    "            <IS_SQL_ALLOWED>false</IS_SQL_ALLOWED>",
    "          </row>",
    ".*"};
assertRequestMatches(wrap(fold(request)), fold(responsePattern));
}

public void testDiscoverCubesRestricted() {
String[] request = {
    "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
    "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
    "    <RequestType>MDSCHEMA_CUBES</RequestType>",
    "    <Restrictions>",
    "        <RestrictionList>",
    "            <CUBE_NAME>Sales</CUBE_NAME>",
    "        </RestrictionList>",
    "    </Restrictions>",
    "    <Properties>",
        "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            "          <row>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <CUBE_TYPE>CUBE</CUBE_TYPE>",
            "            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>",
            "            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>",
            "            <IS_LINKABLE>false</IS_LINKABLE>",
            "            <IS_SQL_ALLOWED>false</IS_SQL_ALLOWED>",
            "          </row>",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    public void testDiscoverCubesRestrictedOnUnrestrictableColumnFails() {
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>MDSCHEMA_CUBES</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "            <IS_WRITE_ENABLED>true</IS_WRITE_ENABLED>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String responsePattern = "(?s).*Rowset 'MDSCHEMA_CUBES' column 'IS_WRITE_ENABLED' does not allow restrictions.*";
        assertExceptionMatches(wrap(fold(request)), responsePattern);
    }

    public void testDiscoverCubesRestrictedOnBadColumn() {
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>MDSCHEMA_CUBES</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "            <NON_EXISTENT_COLUMN>FooBar</NON_EXISTENT_COLUMN>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String responsePattern = "(?s).*Rowset 'MDSCHEMA_CUBES' does not contain column 'NON_EXISTENT_COLUMN'.*";
        assertExceptionMatches(wrap(fold(request)), responsePattern);
    }

    public void testDiscoverDimensions() {
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>MDSCHEMA_DIMENSIONS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String responsePattern = "(?s).*<DIMENSION_NAME>Store</DIMENSION_NAME>.*";
        assertRequestMatches(wrap(fold(request)), responsePattern);
    }

    public void testDiscoverHierarchies() {
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>MDSCHEMA_HIERARCHIES</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String responsePattern = "(?s).*<HIERARCHY_NAME>Store</HIERARCHY_NAME>.*";
        assertRequestMatches(wrap(fold(request)), responsePattern);
    }

    public void testDiscoverLevels() {
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>MDSCHEMA_LEVELS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String responsePattern = "(?s).*<LEVEL_NAME>City</LEVEL_NAME>.*";
        assertRequestMatches(wrap(fold(request)), responsePattern);
    }

    public void testDiscoverMembersRestrictedByHierarchy() {
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>MDSCHEMA_MEMBERS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Gender]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Gender].[(All)]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>0</LEVEL_NUMBER>",
            "            <MEMBER_NAME>All Gender</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>0</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Gender].[All Gender]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>2</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>All Gender</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>2</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>0</PARENT_LEVEL>",
            "          </row>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Gender]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Gender].[Gender]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>1</LEVEL_NUMBER>",
            "            <MEMBER_NAME>F</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Gender].[All Gender].[F]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>F</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>0</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>0</PARENT_LEVEL>",
            "            <PARENT_UNIQUE_NAME>[Gender].[All Gender]</PARENT_UNIQUE_NAME>",
            "          </row>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Gender]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Gender].[Gender]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>1</LEVEL_NUMBER>",
            "            <MEMBER_NAME>M</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Gender].[All Gender].[M]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>M</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>0</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>0</PARENT_LEVEL>",
            "            <PARENT_UNIQUE_NAME>[Gender].[All Gender]</PARENT_UNIQUE_NAME>",
            "          </row>",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    public void testDiscoverMembersRestrictedByMemberAndTreeop() {
        final int treeOp = (Enumeration.TreeOp.Siblings.ordinal |
                Enumeration.TreeOp.Ancestors.ordinal |
                Enumeration.TreeOp.Children.ordinal);
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>MDSCHEMA_MEMBERS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3]</MEMBER_UNIQUE_NAME>",
            "            <TREE_OP>" + treeOp + "</TREE_OP>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Time].[Quarter]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>1</LEVEL_NUMBER>",
            "            <MEMBER_NAME>Q1</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q1]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>Q1</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>3</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>0</PARENT_LEVEL>",
            "            <PARENT_UNIQUE_NAME>[Time].[1997]</PARENT_UNIQUE_NAME>",
            "          </row>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Time].[Quarter]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>1</LEVEL_NUMBER>",
            "            <MEMBER_NAME>Q2</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q2]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>Q2</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>3</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>0</PARENT_LEVEL>",
            "            <PARENT_UNIQUE_NAME>[Time].[1997]</PARENT_UNIQUE_NAME>",
            "          </row>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Time].[Quarter]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>1</LEVEL_NUMBER>",
            "            <MEMBER_NAME>Q4</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q4]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>Q4</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>3</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>0</PARENT_LEVEL>",
            "            <PARENT_UNIQUE_NAME>[Time].[1997]</PARENT_UNIQUE_NAME>",
            "          </row>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Time].[Month]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>2</LEVEL_NUMBER>",
            "            <MEMBER_NAME>7</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3].[7]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>7</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>0</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>1</PARENT_LEVEL>",
            "            <PARENT_UNIQUE_NAME>[Time].[1997].[Q3]</PARENT_UNIQUE_NAME>",
            "          </row>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Time].[Month]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>2</LEVEL_NUMBER>",
            "            <MEMBER_NAME>8</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3].[8]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>8</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>0</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>1</PARENT_LEVEL>",
            "            <PARENT_UNIQUE_NAME>[Time].[1997].[Q3]</PARENT_UNIQUE_NAME>",
            "          </row>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Time].[Month]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>2</LEVEL_NUMBER>",
            "            <MEMBER_NAME>9</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3].[9]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>9</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>0</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>1</PARENT_LEVEL>",
            "            <PARENT_UNIQUE_NAME>[Time].[1997].[Q3]</PARENT_UNIQUE_NAME>",
            "          </row>",
            "          <row>",
            "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>",
            "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>",
            "            <LEVEL_UNIQUE_NAME>[Time].[Year]</LEVEL_UNIQUE_NAME>",
            "            <LEVEL_NUMBER>0</LEVEL_NUMBER>",
            "            <MEMBER_NAME>1997</MEMBER_NAME>",
            "            <MEMBER_ORDINAL>-1</MEMBER_ORDINAL>",
            "            <MEMBER_UNIQUE_NAME>[Time].[1997]</MEMBER_UNIQUE_NAME>",
            "            <MEMBER_TYPE>1</MEMBER_TYPE>",
            "            <MEMBER_CAPTION>1997</MEMBER_CAPTION>",
            "            <CHILDREN_CARDINALITY>4</CHILDREN_CARDINALITY>",
            "            <PARENT_LEVEL>0</PARENT_LEVEL>",
            "          </row>",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    public void testDiscoverMeasures() {
        String[] responsePattern = {
            "(?s).*",
            "          <row>",
            "            <CATALOG_NAME>FoodMart</CATALOG_NAME>",
            "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>",
            "            <CUBE_NAME>Sales</CUBE_NAME>",
            "            <MEASURE_NAME>Unit Sales</MEASURE_NAME>",
            "            <MEASURE_UNIQUE_NAME>\\[Measures\\]\\.\\[Unit Sales\\]</MEASURE_UNIQUE_NAME>",
            "            <MEASURE_CAPTION>Unit Sales</MEASURE_CAPTION>",
            "          </row>",
            ".*"};
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>MDSCHEMA_MEASURES</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Catalog>FoodMart</Catalog>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        assertRequestMatches(wrap(fold(request)), fold(responsePattern));
    }
    /**
     * Tests the {@link RowsetDefinition#DISCOVER_ENUMERATORS} rowset.
     */
    public void testDiscoverEnumerators() {
        String[] responsePattern = {
            "(?s).*",
            "          <row>",
            "            <EnumName>AuthenticationMode</EnumName>",
            "            <EnumDescription>Specification of what type of security mode the data source uses.</EnumDescription>",
            "            <EnumType>EnumString</EnumType>",
            "            <ElementName>Authenticated</ElementName>",
            "            <ElementDescription>User ID and Password must be included in the information required for the connection.</ElementDescription>",
            "            <ElementValue>1</ElementValue>",
            "          </row>",
            ".*"};
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>DISCOVER_ENUMERATORS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList/>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList/>",
            "    </Properties>",
            "</Discover>"};
        assertRequestMatches(wrap(fold(request)), fold(responsePattern));
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_KEYWORDS} rowset.
     */
    public void testDiscoverKeywords() {
        String[] responsePattern = {
            "(?s)" +
                "<[?]xml version=\"1.0\"[?]>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            ".*",
            "          <row>",
            "            <Keyword>AddCalculatedMembers</Keyword>",
            "          </row>",
            "          <row>",
            "            <Keyword>Action</Keyword>",
            "          </row>",
            ".*",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>DISCOVER_KEYWORDS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList/>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        assertRequestMatches(wrap(fold(request)), fold(responsePattern));
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_LITERALS} rowset, and
     * multiple restrictions.
     */
    public void testDiscoverLiterals() {
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            "          <row>",
            "            <LiteralName>DBLITERAL_QUOTE_PREFIX</LiteralName>",
            "            <LiteralValue>[</LiteralValue>",
            "            <LiteralMaxLength>-1</LiteralMaxLength>",
            "          </row>",
            "          <row>",
            "            <LiteralName>DBLITERAL_QUOTE_SUFFIX</LiteralName>",
            "            <LiteralValue>]</LiteralValue>",
            "            <LiteralMaxLength>-1</LiteralMaxLength>",
            "          </row>",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>DISCOVER_LITERALS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <LiteralName>",
            "                <Value>DBLITERAL_QUOTE_PREFIX</Value>",
            "                <Value>DBLITERAL_QUOTE_SUFFIX</Value>",
            "            </LiteralName>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_PROPERTIES} rowset, with no
     * restrictions.
     */
    public void testDiscoverProperties() {
        String[] responsePattern = {
            "(?s).*",
            "          <row>",
            "            <PropertyName>Catalog</PropertyName>",
            "            <PropertyDescription>Specifies the initial catalog or database on which to connect.</PropertyDescription>",
            "            <PropertyType>string</PropertyType>",
            "            <PropertyAccessType>Read/Write</PropertyAccessType>",
            "          </row>",
            ".*"};
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>DISCOVER_PROPERTIES</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        assertRequestMatches(wrap(fold(request)), fold(responsePattern));
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_PROPERTIES} rowset, with no
     * restrictions.
     */
    public void testDiscoverPropertiesUnrestricted() {
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            "          <row>",
            "            <PropertyName>EndRange</PropertyName>",
            "            <PropertyDescription>An integer value corresponding to a CellOrdinal used to restrict an MDDataSet returned by a command to a specific range of cells. Used in conjunction with the BeginRange property. If unspecified, all cells are returned in the rowset. The value -1 means unspecified.</PropertyDescription>",
            "            <PropertyType>Integer</PropertyType>",
            "            <PropertyAccessType>Write</PropertyAccessType>",
            "          </row>",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>DISCOVER_PROPERTIES</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList>",
            "            <PropertyName>EndRange</PropertyName>",
            "        </RestrictionList>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_SCHEMA_ROWSETS} rowset, with
     * no restrictions.
     */
    public void testDiscoverSchemaRowsets() {
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return>",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" elementFormDefault=\"qualified\"/>",
            "          <row>",
            "            <SchemaName>DBSCHEMA_CATALOGS</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>", "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Description>Returns information about literals supported by the provider.</Description>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DBSCHEMA_COLUMNS</SchemaName>",
            "            <Restrictions>",
            "              <Name>TABLE_CATALOG</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TABLE_SCHEMA</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TABLE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>COLUMN_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DBSCHEMA_PROVIDER_TYPES</SchemaName>",
            "            <Restrictions>",
            "              <Name>DATA_TYPE</Name>",
            "              <Type>unsignedInteger</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>BEST_MATCH</Name>",
            "              <Type>boolean</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DBSCHEMA_TABLES</SchemaName>",
            "            <Restrictions>",
            "              <Name>TABLE_CATALOG</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TABLE_SCHEMA</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TABLE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TABLE_TYPE</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DBSCHEMA_TABLES_INFO</SchemaName>",
            "            <Restrictions>",
            "              <Name>TABLE_CATALOG</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TABLE_SCHEMA</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TABLE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TABLE_TYPE</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DISCOVER_DATASOURCES</SchemaName>",
            "            <Restrictions>",
            "              <Name>DataSourceName</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>URL</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>ProviderName</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>ProviderType</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>AuthenticationMode</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Description>Returns a list of XML for Analysis data sources available on the server or Web Service.</Description>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DISCOVER_ENUMERATORS</SchemaName>",
            "            <Restrictions>",
            "              <Name>EnumName</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Description>Returns a list of names, data types, and enumeration values for enumerators supported by the provider of a specific data source.</Description>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DISCOVER_KEYWORDS</SchemaName>",
            "            <Restrictions>",
            "              <Name>Keyword</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Description>Returns an XML list of keywords reserved by the provider.</Description>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DISCOVER_LITERALS</SchemaName>",
            "            <Restrictions>",
            "              <Name>LiteralName</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Description>Returns information about literals supported by the provider.</Description>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DISCOVER_PROPERTIES</SchemaName>",
            "            <Restrictions>",
            "              <Name>PropertyName</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Description>Returns a list of information and values about the requested properties that are supported by the specified data source provider.</Description>",
            "          </row>",
            "          <row>",
            "            <SchemaName>DISCOVER_SCHEMA_ROWSETS</SchemaName>",
            "            <Restrictions>",
            "              <Name>SchemaName</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Description>Returns the names, values, and other information of all supported RequestType enumeration values.</Description>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_ACTIONS</SchemaName>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>COORDINATE</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>COORDINATE_TYPE</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_CUBES</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCHEMA_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_TYPE</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_DIMENSIONS</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCHEMA_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_CAPTION</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_ORDINAL</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_TYPE</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_FUNCTIONS</SchemaName>",
            "            <Restrictions>",
            "              <Name>LIBRARY_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>INTERFACE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>FUNCTION_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>ORIGIN</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_HIERARCHIES</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCHEMA_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>HIERARCHY_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>HIERARCHY_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>HIERARCHY_CAPTION</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_TYPE</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DEFAULT_MEMBER</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>ALL_MEMBER</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_LEVELS</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCHEMA_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>HIERARCHY_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>LEVEL_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>LEVEL_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>LEVEL_CAPTION</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>LEVEL_NUMBER</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>LEVEL_TYPE</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_MEASURES</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCHEMA_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEASURE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEASURE_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEASURE_CAPTION</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_MEMBERS</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCHEMA_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>HIERARCHY_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>LEVEL_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>LEVEL_NUMBER</Name>",
            "              <Type>unsignedInteger</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEMBER_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEMBER_ORDINAL</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEMBER_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEMBER_TYPE</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEMBER_CAPTION</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CHILDREN_CARDINALITY</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>PARENT_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>TREE_OP</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_PROPERTIES</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCHEMA_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>DIMENSION_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>HIERARCHY_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>LEVEL_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>MEMBER_UNIQUE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>PROPERTY_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>PROPERTY_TYPE</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>PROPERTY_CONTENT_TYPE</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>PROPERTY_CAPTION</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "          </row>",
            "          <row>",
            "            <SchemaName>MDSCHEMA_SETS</SchemaName>",
            "            <Restrictions>",
            "              <Name>CATALOG_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCHEMA_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>CUBE_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SET_NAME</Name>",
            "              <Type>string</Type>",
            "            </Restrictions>",
            "            <Restrictions>",
            "              <Name>SCOPE</Name>",
            "              <Type>integer</Type>",
            "            </Restrictions>",
            "          </row>",
            "        </root>",
            "      </return>",
            "    </DiscoverResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        String[] request = {
            "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
            "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "    <RequestType>DISCOVER_SCHEMA_ROWSETS</RequestType>",
            "    <Restrictions>",
            "        <RestrictionList/>",
            "    </Restrictions>",
            "    <Properties>",
            "        <PropertyList>",
            "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "            <Format>Tabular</Format>",
            "        </PropertyList>",
            "    </Properties>",
            "</Discover>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    public void testSelect() {
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <ExecuteResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:mddataset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>",
            "          <OlapInfo>",
            "            <CubeInfo>",
            "              <Cube>",
            "                <CubeName>Sales</CubeName>",
            "              </Cube>",
            "            </CubeInfo>",
            "            <AxesInfo>",
            "              <AxisInfo name=\"SlicerAxis\"/>",
            "              <AxisInfo name=\"Axis0\">",
            "                <HierarchyInfo name=\"Measures\">",
            "                  <UName name=\"[Measures].[MEMBER_UNIQUE_NAME]\"/>",
            "                  <Caption name=\"[Measures].[MEMBER_CAPTION]\"/>",
            "                  <LName name=\"[Measures].[LEVEL_UNIQUE_NAME]\"/>",
            "                  <LNum name=\"[Measures].[LEVEL_NUMBER]\"/>",
            "                  <DisplayInfo name=\"[Measures].[DISPLAY_INFO]\"/>",
            "                </HierarchyInfo>",
            "              </AxisInfo>",
            "            </AxesInfo>",
            "            <CellInfo>",
            "              <Value name=\"VALUE\"/>",
            "              <FmtValue name=\"FORMATTED_VALUE\"/>",
            "              <FormatString name=\"FORMAT_STRING\"/>",
            "            </CellInfo>",
            "          </OlapInfo>",
            "          <Axes>",
            "            <Axis name=\"SlicerAxis\">",
            "              <Tuples>",
            "                <Tuple/>",
            "              </Tuples>",
            "            </Axis>",
            "            <Axis name=\"Axis0\">",
            "              <Tuples>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Profit]</UName>",
            "                    <Caption>Profit</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Profit Growth]</UName>",
            "                    <Caption>Gewinn-Wachstum</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Profit last Period]</UName>",
            "                    <Caption>Profit last Period</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Sales Count]</UName>",
            "                    <Caption>Sales Count</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Store Cost]</UName>",
            "                    <Caption>Store Cost</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Store Sales]</UName>",
            "                    <Caption>Store Sales</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Unit Sales]</UName>",
            "                    <Caption>Unit Sales</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Customer Count]</UName>",
            "                    <Caption>Customer Count</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "              </Tuples>",
            "            </Axis>",
            "          </Axes>",
            "          <CellData>",
            "            <Cell CellOrdinal=\"0\">",
            "              <Value xsi:type=\"xsd:double\">339610.89639999997</Value>",
            "              <FmtValue>$339,610.90</FmtValue>",
            "              <FormatString>$#,##0.00</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"1\">",
            "              <Value xsi:type=\"xsd:double\">0</Value>",
            "              <FmtValue>0.0%</FmtValue>",
            "              <FormatString>0.0%</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"2\">",
            "              <Value xsi:type=\"xsd:double\">339610.89639999997</Value>",
            "              <FmtValue>$339,610.90</FmtValue>",
            "              <FormatString>$#,##0.00</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"3\">",
            "              <Value xsi:type=\"xsd:int\">86837</Value>",
            "              <FmtValue>86,837</FmtValue>",
            "              <FormatString>#,###</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"4\">",
            "              <Value xsi:type=\"xsd:double\">225627.2336</Value>",
            "              <FmtValue>225,627.23</FmtValue>",
            "              <FormatString>#,###.00</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"5\">",
            "              <Value xsi:type=\"xsd:double\">565238.13</Value>",
            "              <FmtValue>565,238.13</FmtValue>",
            "              <FormatString>#,###.00</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"6\">",
            "              <Value xsi:type=\"xsd:double\">266773</Value>",
            "              <FmtValue>266,773</FmtValue>",
            "              <FormatString>Standard</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"7\">",
            "              <Value xsi:type=\"xsd:int\">5581</Value>",
            "              <FmtValue>5,581</FmtValue>",
            "              <FormatString>#,###</FormatString>",
            "            </Cell>",
            "          </CellData>",
            "        </root>",
            "      </return>",
            "    </ExecuteResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};

        /*
         * Tests compatibility with MSAS
         */
        String[] request = {
            "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" ",
            "  SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <Command>",
            "    <Statement>select [Measures].allmembers on Columns from Sales</Statement>",
            "  </Command>",
            "  <Properties>",
            "    <PropertyList>",
            "      <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "      <Catalog>FoodMart</Catalog>",
            "      <Format>Multidimensional</Format>",
            "      <AxisFormat>TupleFormat</AxisFormat>",
            "    </PropertyList>",
            "  </Properties>",
            "</Execute>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    /**
     * Tests that we get the right metadata about a query's axes if the
     * axis is empty. We can't deduce the hierarchies on it by simply looking
     * at the first element on the axis, because there is none, so we need to
     * look at the parse tree instead.
     *
     * TODO: implement
     */
    public void _testSelectEmptyAxis() {
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <OLAPInfo>",
            "      <CubeInfo>",
            "        <Cube>",
            "          <CubeName>Sales</CubeName>",
            "        </Cube>",
            "      </CubeInfo>",
            "      <AxesInfo>",
            "        <AxisInfo name=\"SlicerAxis\"/>",
            "        <AxisInfo name=\"Axis0\"/>",
            "      </AxesInfo>",
            "      <CellInfo>",
            "        <Value name=\"VALUE\"/>",
            "        <FmtValue name=\"FORMATTED_VALUE\"/>",
            "        <FormatString name=\"FORMAT_STRING\"/>",
            "      </CellInfo>",
            "    </OLAPInfo>",
            "    <Axes>",
            "      <Axis name=\"SlicerAxis\">",
            "        <Tuples>",
            "          <Tuple/>",
            "        </Tuples>",
            "      </Axis>",
            "      <Axis name=\"Axis0\">",
            "        <Tuples/>",
            "      </Axis>",
            "    </Axes>",
            "    <CellData/>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        String[] request = {
            "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" ",
            "  SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <Command>",
            "    <Statement>select {[Gender].[F].PrevMember} on Columns from Sales</Statement>",
            "  </Command>",
            "  <Properties>",
            "    <PropertyList>",
            "      <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "      <Catalog>FoodMart</Catalog>",
            "      <Format>Multidimensional</Format>",
            "      <AxisFormat>TupleFormat</AxisFormat>",
            "    </PropertyList>",
            "  </Properties>",
            "</Execute>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    public void testSelect2() {
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <ExecuteResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:mddataset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>",
            "          <OlapInfo>",
            "            <CubeInfo>",
            "              <Cube>",
            "                <CubeName>HR</CubeName>",
            "              </Cube>",
            "            </CubeInfo>",
            "            <AxesInfo>",
            "              <AxisInfo name=\"SlicerAxis\"/>",
            "              <AxisInfo name=\"Axis0\">",
            "                <HierarchyInfo name=\"Measures\">",
            "                  <UName name=\"[Measures].[MEMBER_UNIQUE_NAME]\"/>",
            "                  <Caption name=\"[Measures].[MEMBER_CAPTION]\"/>",
            "                  <LName name=\"[Measures].[LEVEL_UNIQUE_NAME]\"/>",
            "                  <LNum name=\"[Measures].[LEVEL_NUMBER]\"/>",
            "                  <DisplayInfo name=\"[Measures].[DISPLAY_INFO]\"/>",
            "                </HierarchyInfo>",
            "              </AxisInfo>",
            "              <AxisInfo name=\"Axis1\">",
            "                <HierarchyInfo name=\"Store\">",
            "                  <UName name=\"[Store].[MEMBER_UNIQUE_NAME]\"/>",
            "                  <Caption name=\"[Store].[MEMBER_CAPTION]\"/>",
            "                  <LName name=\"[Store].[LEVEL_UNIQUE_NAME]\"/>",
            "                  <LNum name=\"[Store].[LEVEL_NUMBER]\"/>",
            "                  <DisplayInfo name=\"[Store].[DISPLAY_INFO]\"/>",
            "                </HierarchyInfo>",
            "                <HierarchyInfo name=\"Pay Type\">",
            "                  <UName name=\"[Pay Type].[MEMBER_UNIQUE_NAME]\"/>",
            "                  <Caption name=\"[Pay Type].[MEMBER_CAPTION]\"/>",
            "                  <LName name=\"[Pay Type].[LEVEL_UNIQUE_NAME]\"/>",
            "                  <LNum name=\"[Pay Type].[LEVEL_NUMBER]\"/>",
            "                  <DisplayInfo name=\"[Pay Type].[DISPLAY_INFO]\"/>",
            "                </HierarchyInfo>",
            "              </AxisInfo>",
            "            </AxesInfo>",
            "            <CellInfo>",
            "              <Value name=\"VALUE\"/>",
            "              <FmtValue name=\"FORMATTED_VALUE\"/>",
            "              <FormatString name=\"FORMAT_STRING\"/>",
            "            </CellInfo>",
            "          </OlapInfo>",
            "          <Axes>",
            "            <Axis name=\"SlicerAxis\">",
            "              <Tuples>",
            "                <Tuple/>",
            "              </Tuples>",
            "            </Axis>",
            "            <Axis name=\"Axis0\">",
            "              <Tuples>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Org Salary]</UName>",
            "                    <Caption>Org Salary</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Measures\">",
            "                    <UName>[Measures].[Count]</UName>",
            "                    <Caption>Count</Caption>",
            "                    <LName>[Measures].[MeasuresLevel]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "              </Tuples>",
            "            </Axis>",
            "            <Axis name=\"Axis1\">",
            "              <Tuples>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[CA]</UName>",
            "                    <Caption>CA</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>5</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types]</UName>",
            "                    <Caption>All Pay Types</Caption>",
            "                    <LName>[Pay Type].[(All)]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>65538</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[CA]</UName>",
            "                    <Caption>CA</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>131077</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types].[Hourly]</UName>",
            "                    <Caption>Hourly</Caption>",
            "                    <LName>[Pay Type].[Pay Type]</LName>",
            "                    <LNum>1</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[CA]</UName>",
            "                    <Caption>CA</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>131077</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types].[Monthly]</UName>",
            "                    <Caption>Monthly</Caption>",
            "                    <LName>[Pay Type].[Pay Type]</LName>",
            "                    <LNum>1</LNum>",
            "                    <DisplayInfo>131072</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[OR]</UName>",
            "                    <Caption>OR</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>131074</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types]</UName>",
            "                    <Caption>All Pay Types</Caption>",
            "                    <LName>[Pay Type].[(All)]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>65538</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[OR]</UName>",
            "                    <Caption>OR</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>131074</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types].[Hourly]</UName>",
            "                    <Caption>Hourly</Caption>",
            "                    <LName>[Pay Type].[Pay Type]</LName>",
            "                    <LNum>1</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[OR]</UName>",
            "                    <Caption>OR</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>131074</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types].[Monthly]</UName>",
            "                    <Caption>Monthly</Caption>",
            "                    <LName>[Pay Type].[Pay Type]</LName>",
            "                    <LNum>1</LNum>",
            "                    <DisplayInfo>131072</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[WA]</UName>",
            "                    <Caption>WA</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>131079</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types]</UName>",
            "                    <Caption>All Pay Types</Caption>",
            "                    <LName>[Pay Type].[(All)]</LName>",
            "                    <LNum>0</LNum>",
            "                    <DisplayInfo>65538</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[WA]</UName>",
            "                    <Caption>WA</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>131079</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types].[Hourly]</UName>",
            "                    <Caption>Hourly</Caption>",
            "                    <LName>[Pay Type].[Pay Type]</LName>",
            "                    <LNum>1</LNum>",
            "                    <DisplayInfo>0</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "                <Tuple>",
            "                  <Member Hierarchy=\"Store\">",
            "                    <UName>[Store].[All Stores].[USA].[WA]</UName>",
            "                    <Caption>WA</Caption>",
            "                    <LName>[Store].[Store State]</LName>",
            "                    <LNum>2</LNum>",
            "                    <DisplayInfo>131079</DisplayInfo>",
            "                  </Member>",
            "                  <Member Hierarchy=\"Pay Type\">",
            "                    <UName>[Pay Type].[All Pay Types].[Monthly]</UName>",
            "                    <Caption>Monthly</Caption>",
            "                    <LName>[Pay Type].[Pay Type]</LName>",
            "                    <LNum>1</LNum>",
            "                    <DisplayInfo>131072</DisplayInfo>",
            "                  </Member>",
            "                </Tuple>",
            "              </Tuples>",
            "            </Axis>",
            "          </Axes>",
            "          <CellData>",
            "            <Cell CellOrdinal=\"0\">",
            "              <Value xsi:type=\"xsd:double\">14861.5006</Value>",
            "              <FmtValue>$14,861.50</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"1\">",
            "              <Value xsi:type=\"xsd:int\">2316</Value>",
            "              <FmtValue>2,316</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"2\">",
            "              <Value xsi:type=\"xsd:double\">3261.2206</Value>",
            "              <FmtValue>$3,261.22</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"3\">",
            "              <Value xsi:type=\"xsd:int\">972</Value>",
            "              <FmtValue>972</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"4\">",
            "              <Value xsi:type=\"xsd:double\">11600.28</Value>",
            "              <FmtValue>$11,600.28</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"5\">",
            "              <Value xsi:type=\"xsd:int\">1344</Value>",
            "              <FmtValue>1,344</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"6\">",
            "              <Value xsi:type=\"xsd:double\">7848.9727</Value>",
            "              <FmtValue>$7,848.97</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"7\">",
            "              <Value xsi:type=\"xsd:int\">1632</Value>",
            "              <FmtValue>1,632</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"8\">",
            "              <Value xsi:type=\"xsd:double\">2663.8927</Value>",
            "              <FmtValue>$2,663.89</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"9\">",
            "              <Value xsi:type=\"xsd:int\">792</Value>",
            "              <FmtValue>792</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"10\">",
            "              <Value xsi:type=\"xsd:double\">5185.08</Value>",
            "              <FmtValue>$5,185.08</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"11\">",
            "              <Value xsi:type=\"xsd:int\">840</Value>",
            "              <FmtValue>840</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"12\">",
            "              <Value xsi:type=\"xsd:double\">16721.1979</Value>",
            "              <FmtValue>$16,721.20</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"13\">",
            "              <Value xsi:type=\"xsd:int\">3444</Value>",
            "              <FmtValue>3,444</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"14\">",
            "              <Value xsi:type=\"xsd:double\">5481.6379</Value>",
            "              <FmtValue>$5,481.64</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"15\">",
            "              <Value xsi:type=\"xsd:int\">1632</Value>",
            "              <FmtValue>1,632</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"16\">",
            "              <Value xsi:type=\"xsd:double\">11239.56</Value>",
            "              <FmtValue>$11,239.56</FmtValue>",
            "              <FormatString>Currency</FormatString>",
            "            </Cell>",
            "            <Cell CellOrdinal=\"17\">",
            "              <Value xsi:type=\"xsd:int\">1812</Value>",
            "              <FmtValue>1,812</FmtValue>",
            "              <FormatString>#,#</FormatString>",
            "            </Cell>",
            "          </CellData>",
            "        </root>",
            "      </return>",
            "    </ExecuteResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>"};
        String[] request = {
            "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" ",
            "  SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <Command>",
            "    <Statement>SELECT {[Measures].[Org Salary], [Measures].[Count]} ON COLUMNS,",
            "  {[Store].[USA].children * [Pay Type].Members} DIMENSION PROPERTIES [Store].[Store SQFT] ON ROWS",
            "FROM [HR]</Statement>",
            "  </Command>",
            "  <Properties>",
            "    <PropertyList>",
            "      <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "      <Catalog>FoodMart</Catalog>",
            "      <Format>Multidimensional</Format>",
            "      <AxisFormat>TupleFormat</AxisFormat>",
            "    </PropertyList>",
            "  </Properties>",
            "</Execute>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    public void testXmlaError() {
        String[] expected = {
            "<?xml version=\"1.0\"?>",
            "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <SOAP-ENV:Body>",
            "    <ExecuteResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">",
            "      <return xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">",
            "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:mddataset\">",
            "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>",
            "          <Messages>",
            "            <Error ErrorCode=\"mondrian.olap.MondrianException\" Description=\"Mondrian Error:MDX cube &#39;NonexistedCube&#39; not found or not processed\" Source=\"Mondrian\" Help=\"\"/>",
            "          </Messages>",
            "        </root>",
            "      </return>",
            "    </ExecuteResponse>",
            "  </SOAP-ENV:Body>",
            "</SOAP-ENV:Envelope>",};
        String[] request = {
            "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" ",
            "  SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
            "  <Command>",
            "    <Statement>SELECT {[Measures].Members} ON COLUMNS FROM [NonexistedCube]</Statement>",
            "  </Command>",
            "  <Properties>",
            "    <PropertyList>",
            "      <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
            "      <Catalog>FoodMart</Catalog>",
            "      <Format>Multidimensional</Format>",
            "      <AxisFormat>TupleFormat</AxisFormat>",
            "    </PropertyList>",
            "  </Properties>",
            "</Execute>"};
        assertRequestYields(wrap(fold(request)), fold(expected));
    }

    // RME
    // DISCOVER_DATASOURCES
    public void testDiscoverDataSourcesDefault() 
            throws IOException {
        String request = 
            readDataFile("DISCOVER_DATASOURCES_Default_request.xml");
        String response = 
            readDataFile("DISCOVER_DATASOURCES_Default_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    public void testDiscoverDataSourcesNone() 
            throws IOException {
        String request = 
            readDataFile("DISCOVER_DATASOURCES_None_request.xml");
        String response = 
            readDataFile("DISCOVER_DATASOURCES_None_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    public void testDiscoverDataSourcesData() 
            throws IOException {
        String request = 
            readDataFile("DISCOVER_DATASOURCES_Data_request.xml");
        String response = 
            readDataFile("DISCOVER_DATASOURCES_Data_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    public void testDiscoverDataSourcesSchema() 
            throws IOException {
        String request = 
            readDataFile("DISCOVER_DATASOURCES_Schema_request.xml");
        String response = 
            readDataFile("DISCOVER_DATASOURCES_Schema_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    public void testDiscoverDataSourcesSchemaData() 
            throws IOException {
        String request = 
            readDataFile("DISCOVER_DATASOURCES_SchemaData_request.xml");
        String response = 
            readDataFile("DISCOVER_DATASOURCES_SchemaData_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    // DBSCHEMA_CATALOGS
    public void testDiscoverDbSchemaCatalogsDefault() 
            throws IOException {
        String request = 
            readDataFile("DBSCHEMA_CATALOGS_Default_request.xml");
        String response = 
            readDataFile("DBSCHEMA_CATALOGS_Default_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    // MDSCHEMA_MEASURES
    public void testDiscoverMdSchemaMeasuresSchemaData() 
            throws IOException {
        String request = 
            readDataFile("MDSCHEMA_MEASURES_SchemaData_request.xml");
        String response = 
            readDataFile("MDSCHEMA_MEASURES_SchemaData_respose.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    // MDSCHEMA_CUBES
    public void testDiscoverMdSchemaCubesSchemaData() 
            throws IOException {
        String request = 
            readDataFile("MDSCHEMA_CUBES_SchemaData_request.xml");
        String response = 
            readDataFile("MDSCHEMA_CUBES_SchemaData_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    // MDSCHEMA_DIMENSIONS
    public void testDiscoverMdSchemaDimensionsSchemaData() 
            throws IOException {
        String request = 
            readDataFile("MDSCHEMA_DIMENSIONS_SchemaData_request.xml");
        String response = 
            readDataFile("MDSCHEMA_DIMENSIONS_SchemaData_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    // MDSCHEMA_LEVELS
    public void testDiscoverMdSchemaLevelsSchemaData12() 
            throws IOException {
        String request = 
            readDataFile("MDSCHEMA_LEVELS_SchemaData_request_12.xml");
        String response = 
            readDataFile("MDSCHEMA_LEVELS_SchemaData_response_12.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    // EXECUTE
    public void testExecuteData() 
            throws IOException {
        String request = 
            readDataFile("EXECUTE_Data_request.xml");
        String response = 
            readDataFile("EXECUTE_Data_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }
    public void testExecuteSchemaData() 
            throws IOException {
        String request = 
            readDataFile("EXECUTE_SchemaData_request.xml");
        String response = 
            readDataFile("EXECUTE_SchemaData_response.xml");
// TODO
//        assertRequestYields(request, xmlwrap(response));
    }




    /**
     * Asserts that two string arrays are equal.
     */
    public static void assertEquals(String[] expected, String[] actual) {
        assertEquals(Arrays.asList(expected), Arrays.asList(actual));
    }

    /**
     * Tests that the redundant copies of the sample XML/A requests are in
     * sync.
     *
     * <p>The primary set of XML/A requests is encoded in the test methods of
     * this class, and is retrieved by calling {@link #getRequests()}. But
     * there is a redundant set of XML/A requests which is used to drive
     * the XML/A test page, <code>xmlaTest.jsp</code>. This page must not be
     * dependent upon the test classes, the method
     * {@link XmlaUtil#getSampleRequests(String, String)} contains a redundant
     * copy of these requests. If these get out of sync, this test helpfully
     * outputs the code for the redundant copy, and then fails.
     */
    public void testThatRequestListMatches() {
        if (requestList != null) {
            return;
        }
        final Map requestMap = getRequests();
        final Set requestKeySet = requestMap.keySet();
        final String[] requestKeys = (String[])
                requestKeySet.toArray(new String[requestKeySet.size()]);
        Arrays.sort(requestKeys);
        List requestList = new ArrayList();
        for (int i = 0; i < requestKeys.length; i++) {
            String requestKey = requestKeys[i];
            String request = (String) requestMap.get(requestKey);
            requestList.add(new XmlaUtil.XmlaRequest(requestKey, request));
        }
        final XmlaUtil.XmlaRequest[] requests = (XmlaUtil.XmlaRequest[])
                requestList.toArray(
                        new XmlaUtil.XmlaRequest[requestList.size()]);
        final XmlaUtil.XmlaRequest[] sampleRequests =
                XmlaUtil.getSampleRequests(catalogName, dataSource);
        if (!Util.equals(
                Arrays.asList(requests),
                Arrays.asList(sampleRequests))) {
            if (requests == null ||
                    sampleRequests == null ||
                    requests.length != sampleRequests.length) {
                System.out.println(
                        "Unequal results length: " + requests.length +
                        " vs sampleRequests length: " + sampleRequests.length);
            } else {
                System.out.println("Java: {");
                for (int i = 0; i < requests.length; i++) {
                    if (!Util.equals(requests[i], sampleRequests[i])) {
                        System.out.println("Request " + i + " - " + requests[i].name);
                        System.out.println("-------------------");
                        System.out.print(toJava(requests[i].request));
                        System.out.println();
                        System.out.println("-------------------");
                        System.out.println("Sample");
                        System.out.println("-------------------");
                        System.out.print(toJava(sampleRequests[i].request));
                        System.out.println();
                        System.out.println("-------------------");
                    }
                }
                System.out.println("}");
            }
        }
        assertEquals(Arrays.asList(requests), Arrays.asList(sampleRequests));
    }

    private static String toJava(String s) {
        s = Util.replace(s, "\"", "\\\"");
        final String lineBreak = "\" + nl + " + nl + "\"";
        s = Pattern.compile("\r\n|\r|\n").matcher(s).replaceAll(lineBreak);
        s = Util.replace(s, dataSource, "\" + dataSource + \"");
        s = Util.replace(s, catalogName, "\" + catalogName + \"");
        s = "\"" + s + "\"";
        final String spurious = " + " + nl + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        return s;
    }
    protected void makeDataDir() {
        // get location where tests are being run
        // we assume that this is the mondrian directory
        String mondrianPathTop = System.getProperty("user.dir");

        File mondrianDirTop = new File(mondrianPathTop);

        File testSrcDir = new File(mondrianDirTop, "testsrc");
        if (! testSrcDir.exists()) {
            return;
        }
        File mainDir = new File(testSrcDir, "main");
        if (! mainDir.exists()) {
            return;
        }
        File mondrianDir = new File(mainDir, "mondrian");
        if (! mondrianDir.exists()) {
            return;
        }
        File xmlaDir = new File(mondrianDir, "xmla");
        if (! xmlaDir.exists()) {
            return;
        }
        dataDir = new File(xmlaDir, "data");
        if (! dataDir.exists()) {
            dataDir = null;
            return;
        }
    }
    protected String readDataFile(String name) throws IOException {
        if (dataDir == null) {
            fail("Could not create xmla data directory: " +
                "/home/emberson/java/mondrian/mondrian/testsrc");
        }
        File f = new File(dataDir, name);
        if (! f.exists()) {
            fail("Does not exists Data File :: " +
                f.getPath());
        }
        InputStream fin = new FileInputStream(f);
        InputStreamReader reader = new InputStreamReader(fin);
        BufferedReader bufReader = new BufferedReader(reader);

        StringBuffer buf = new StringBuffer(1024);
        char[] chars = new char[256];
        int len = bufReader.read(chars);
        while (len > 0) {
            buf.append(chars, 0, len);
            len = bufReader.read(chars);
        }

        return buf.toString();

    }

}

// End XmlaTest.java
