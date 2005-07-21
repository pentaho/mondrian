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

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;

import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMUtil;

import junit.framework.TestCase;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;

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

    public XmlaTest(String s) {
        super(s);
    }

    protected void setUp() throws Exception {
        super.setUp();

        String catalogURL = MondrianProperties.instance().CatalogURL.get();
        if (catalogURL == null) {
            final File file = new File("demo/FoodMart.xml");
            if (!file.exists()) {
                throw new RuntimeException("CatalogURL must be specified");
            }
            final URL url = Util.toURL(file);
            catalogURL = url.toString();
        }

        String driver = MondrianProperties.instance().JdbcDrivers.get();
        String url = MondrianProperties.instance().FoodmartJdbcURL.get();

        // Deal with embedded & that can be in the JDBC URL
        String connectString =
                "Provider=Mondrian;"
                + "Jdbc=" + url + ";"
                + "Catalog=" + catalogURL + ";"
                + "JdbcDrivers=" + driver +";";
        
        dataSource = "MondrianFoodMart";
        catalogName = DriverManager.getConnection(connectString, null, false).getSchema().getName();
        
        connectString = connectString.replaceAll("&", "&amp;");

        StringReader dsConfigReader = new StringReader(
        	"<?xml version=\"1.0\"?>" + nl +
        	"<DataSources>" + 
        	"	<DataSource>" + 
        	"		<DataSourceName>MondrianFoodMart</DataSourceName>" + 
        	"		<DataSourceDescription>MondrianFoodMart</DataSourceDescription>" + 
        	"		<URL>http://localhost:8080/mondrian/xmla</URL>" + 
        	"		<DataSourceInfo>" + connectString + "</DataSourceInfo>" + 
        	"		<ProviderName>Mondrian</ProviderName>" + 
        	"		<ProviderType>MDP</ProviderType>" + 
        	"		<AuthenticationMode>Unauthenticated</AuthenticationMode>" +
        	"	</DataSource>" +
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
    public synchronized HashMap getRequests() {
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
        assertEquals(expected, response);
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

    // tests follow

    public void testDiscoverDataSources() {
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_DATASOURCES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList/>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList/>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <row>" + nl +
                "            <DataSourceName>MondrianFoodMart</DataSourceName>" + nl +
                "            <DataSourceDescription>MondrianFoodMart</DataSourceDescription>" + nl +
                "            <URL>http://localhost:8080/mondrian/xmla</URL>" + nl +
                "            <DataSourceInfo>MondrianFoodMart</DataSourceInfo>" + nl +
                "            <ProviderName>Mondrian</ProviderName>" + nl +
                "            <ProviderType>" + nl +
                "              <MDP/>" + nl +
                "            </ProviderType>" + nl +
                "            <AuthenticationMode>Unauthenticated</AuthenticationMode>" + nl +
                "          </row>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    public void testDiscoverCatalogs() {
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DBSCHEMA_CATALOGS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList/>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>FoodMart</CATALOG_NAME>" + nl +
                "            <DESCRIPTION/>" + nl +
                "          </row>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    public void testDiscoverCubes() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_CUBES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s).*" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Store</CUBE_NAME>" + nl +
                "            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>" + nl +
                "            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>" + nl +
                "            <IS_LINKABLE>false</IS_LINKABLE>" + nl +
                "            <IS_SQL_ALLOWED>false</IS_SQL_ALLOWED>" + nl +
                "          </row>" + nl +
                ".*");
    }

    public void testDiscoverCubesRestricted() {
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_CUBES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>" + nl +
                "            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>" + nl +
                "            <IS_LINKABLE>false</IS_LINKABLE>" + nl +
                "            <IS_SQL_ALLOWED>false</IS_SQL_ALLOWED>" + nl +
                "          </row>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    public void testDiscoverCubesRestrictedOnUnrestrictableColumnFails() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_CUBES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <IS_WRITE_ENABLED>true</IS_WRITE_ENABLED>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s).*Rowset 'MDSCHEMA_CUBES' column 'IS_WRITE_ENABLED' does not allow restrictions.*");
    }

    public void testDiscoverCubesRestrictedOnBadColumn() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_CUBES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <NON_EXISTENT_COLUMN>FooBar</NON_EXISTENT_COLUMN>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s).*Rowset 'MDSCHEMA_CUBES' does not contain column 'NON_EXISTENT_COLUMN'.*");
    }

    public void testDiscoverDimensions() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_DIMENSIONS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s).*<DIMENSION_NAME>Store</DIMENSION_NAME>.*");
    }

    public void testDiscoverHierarchies() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_HIERARCHIES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s).*<HIERARCHY_NAME>Store</HIERARCHY_NAME>.*");
    }

    public void testDiscoverLevels() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_LEVELS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s).*<LEVEL_NAME>City</LEVEL_NAME>.*");
    }

    public void testDiscoverMembersRestrictedByHierarchy() {
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_MEMBERS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Gender]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>(All)</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>0</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>All Gender</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Gender].[All Gender]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>All Gender</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Gender]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Gender</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>1</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>F</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Gender].[All Gender].[F]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>F</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Gender]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Gender</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>1</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>M</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Gender].[All Gender].[M]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>M</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    public void testDiscoverMembersRestrictedByMemberAndTreeop() {
        final int treeOp = (Enumeration.TreeOp.Siblings.ordinal |
                        Enumeration.TreeOp.Ancestors.ordinal |
                        Enumeration.TreeOp.Children.ordinal);
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_MEMBERS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3]</MEMBER_UNIQUE_NAME>" + nl +
                "            <TREE_OP>" + treeOp + "</TREE_OP>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Quarter</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>1</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>Q1</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q1]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>Q1</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Quarter</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>1</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>Q2</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q2]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>Q2</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Quarter</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>1</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>Q4</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q4]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>Q4</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Month</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>2</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>7</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3].[7]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>7</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Month</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>2</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>8</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3].[8]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>8</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Month</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>2</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>9</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3].[9]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>9</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>"+catalogName+"</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <DIMENSION_UNIQUE_NAME>[Time]</DIMENSION_UNIQUE_NAME>" + nl +
                "            <HIERARCHY_UNIQUE_NAME>[Time]</HIERARCHY_UNIQUE_NAME>" + nl +
                "            <LEVEL_UNIQUE_NAME>Year</LEVEL_UNIQUE_NAME>" + nl +
                "            <LEVEL_NUMBER>0</LEVEL_NUMBER>" + nl +
                "            <MEMBER_NAME>1997</MEMBER_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997]</MEMBER_UNIQUE_NAME>" + nl +
                "            <MEMBER_CAPTION>1997</MEMBER_CAPTION>" + nl +
                "            <MEMBER_TYPE>6</MEMBER_TYPE>" + nl +
                "            <TREE_OP/>" + nl +
                "          </row>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    public void _testDiscoverMeasures() {
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_MEASURES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>x" + nl +
                "x        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }
    /**
     * Tests the {@link RowsetDefinition#DISCOVER_ENUMERATORS} rowset.
     */
    public void testDiscoverEnumerators() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_ENUMERATORS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList/>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList/>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s).*" + nl +
                "          <row>" + nl +
                "            <EnumName>AuthenticationMode</EnumName>" + nl +
                "            <EnumDescription>Specification of what type of security mode the data source uses.</EnumDescription>" + nl +
                "            <EnumType>EnumString</EnumType>" + nl +
                "            <ElementName>Authenticated</ElementName>" + nl +
                "            <ElementDescription>User ID and Password must be included in the information required for the connection.</ElementDescription>" + nl +
                "            <ElementValue>1</ElementValue>" + nl +
                "          </row>" + nl +
                ".*");
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_KEYWORDS} rowset.
     */
    public void testDiscoverKeywords() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_KEYWORDS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList/>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s)" +
                "<[?]xml version=\"1.0\"[?]>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                ".*" + nl +
                "          <row>" + nl +
                "            <Keyword>AddCalculatedMembers</Keyword>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <Keyword>Action</Keyword>" + nl +
                "          </row>" + nl +
                ".*" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_LITERALS} rowset, and
     * multiple restrictions.
     */
    public void testDiscoverLiterals() {
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_LITERALS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <LiteralName>" + nl +
                "                <Value>DBLITERAL_QUOTE_PREFIX</Value>" + nl +
                "                <Value>DBLITERAL_QUOTE_SUFFIX</Value>" + nl +
                "            </LiteralName>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <row>" + nl +
                "            <LiteralName>DBLITERAL_QUOTE_PREFIX</LiteralName>" + nl +
                "            <LiteralValue>[</LiteralValue>" + nl +
                "            <LiteralMaxLength>-1</LiteralMaxLength>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <LiteralName>DBLITERAL_QUOTE_SUFFIX</LiteralName>" + nl +
                "            <LiteralValue>]</LiteralValue>" + nl +
                "            <LiteralMaxLength>-1</LiteralMaxLength>" + nl +
                "          </row>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_PROPERTIES} rowset, with no
     * restrictions.
     */
    public void testDiscoverProperties() {
        assertRequestMatches(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_PROPERTIES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "(?s).*" + nl +
                "          <row>" + nl +
                "            <PropertyName>Catalog</PropertyName>" + nl +
                "            <PropertyDescription>Specifies the initial catalog or database on which to connect.</PropertyDescription>" + nl +
                "            <PropertyType>string</PropertyType>" + nl +
                "            <PropertyAccessType>Read/Write</PropertyAccessType>" + nl +
                "            <IsRequired/>" + nl +
                "            <Value/>" + nl +
                "          </row>" + nl +
                ".*");
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_PROPERTIES} rowset, with no
     * restrictions.
     */
    public void testDiscoverPropertiesUnrestricted() {
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_PROPERTIES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <PropertyName>EndRange</PropertyName>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <row>" + nl +
                "            <PropertyName>EndRange</PropertyName>" + nl +
                "            <PropertyDescription>An integer value corresponding to a CellOrdinal used to restrict an MDDataSet returned by a command to a specific range of cells. Used in conjunction with the BeginRange property. If unspecified, all cells are returned in the rowset. The value -1 means unspecified.</PropertyDescription>" + nl +
                "            <PropertyType>Integer</PropertyType>" + nl +
                "            <PropertyAccessType>Write</PropertyAccessType>" + nl +
                "            <IsRequired/>" + nl +
                "            <Value/>" + nl +
                "          </row>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_SCHEMA_ROWSETS} rowset, with
     * no restrictions.
     */
    public void testDiscoverSchemaRowsets() {
        assertRequestYields(wrap(
                "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_SCHEMA_ROWSETS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList/>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DBSCHEMA_CATALOGS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description>Returns information about literals supported by the provider.</Description>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DBSCHEMA_COLUMNS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <TABLE_CATALOG type=\"string\"/>" + nl +
                "              <TABLE_SCHEMA type=\"string\"/>" + nl +
                "              <TABLE_NAME type=\"string\"/>" + nl +
                "              <COLUMN_NAME type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DBSCHEMA_PROVIDER_TYPES</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <DATA_TYPE type=\"unsignedInteger\"/>" + nl +
                "              <BEST_MATCH type=\"boolean\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DBSCHEMA_TABLES</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <TABLE_CATALOG type=\"string\"/>" + nl +
                "              <TABLE_SCHEMA type=\"string\"/>" + nl +
                "              <TABLE_NAME type=\"string\"/>" + nl +
                "              <TABLE_TYPE type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DBSCHEMA_TABLES_INFO</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <TABLE_CATALOG type=\"string\"/>" + nl +
                "              <TABLE_SCHEMA type=\"string\"/>" + nl +
                "              <TABLE_NAME type=\"string\"/>" + nl +
                "              <TABLE_TYPE type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DISCOVER_DATASOURCES</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <DataSourceName type=\"string\"/>" + nl +
                "              <URL type=\"string\"/>" + nl +
                "              <ProviderName type=\"string\"/>" + nl +
                "              <ProviderType type=\"string\"/>" + nl +
                "              <AuthenticationMode type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description>Returns a list of XML for Analysis data sources available on the server or Web Service.</Description>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DISCOVER_ENUMERATORS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <EnumName type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description>Returns a list of names, data types, and enumeration values for enumerators supported by the provider of a specific data source.</Description>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DISCOVER_KEYWORDS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <Keyword type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description>Returns an XML list of keywords reserved by the provider.</Description>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DISCOVER_LITERALS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <LiteralName type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description>Returns information about literals supported by the provider.</Description>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DISCOVER_PROPERTIES</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <PropertyName type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description>Returns a list of information and values about the requested properties that are supported by the specified data source provider.</Description>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>DISCOVER_SCHEMA_ROWSETS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <SchemaName type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description>Returns the names, values, and other information of all supported RequestType enumeration values.</Description>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_ACTIONS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "              <COORDINATE type=\"string\"/>" + nl +
                "              <COORDINATE_TYPE type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_CUBES</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "              <SCHEMA_NAME type=\"string\"/>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_DIMENSIONS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "              <SCHEMA_NAME type=\"string\"/>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "              <DIMENSION_NAME type=\"string\"/>" + nl +
                "              <DIMENSION_UNIQUE_NAME type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_FUNCTIONS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <LIBRARY_NAME type=\"string\"/>" + nl +
                "              <INTERFACE_NAME type=\"string\"/>" + nl +
                "              <FUNCTION_NAME type=\"string\"/>" + nl +
                "              <ORIGIN type=\"integer\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_HIERARCHIES</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "              <SCHEMA_NAME type=\"string\"/>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "              <DIMENSION_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <HIERARCHY_NAME type=\"string\"/>" + nl +
                "              <HIERARCHY_UNIQUE_NAME type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_LEVELS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "              <SCHEMA_NAME type=\"string\"/>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "              <DIMENSION_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <HIERARCHY_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <LEVEL_NAME type=\"string\"/>" + nl +
                "              <LEVEL_UNIQUE_NAME type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_MEASURES</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "              <SCHEMA_NAME type=\"string\"/>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "              <MEASURE_NAME type=\"string\"/>" + nl +
                "              <MEASURE_UNIQUE_NAME type=\"string\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_MEMBERS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "              <SCHEMA_NAME type=\"string\"/>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "              <DIMENSION_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <HIERARCHY_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <LEVEL_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <LEVEL_NUMBER type=\"unsignedInteger\"/>" + nl +
                "              <MEMBER_NAME type=\"string\"/>" + nl +
                "              <MEMBER_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <MEMBER_CAPTION type=\"string\"/>" + nl +
                "              <MEMBER_TYPE type=\"integer\"/>" + nl +
                "              <TREE_OP type=\"integer\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_PROPERTIES</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "              <SCHEMA_NAME type=\"string\"/>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "              <DIMENSION_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <HIERARCHY_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <LEVEL_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <MEMBER_UNIQUE_NAME type=\"string\"/>" + nl +
                "              <PROPERTY_NAME type=\"string\"/>" + nl +
                "              <PROPERTY_TYPE type=\"integer\"/>" + nl +
                "              <PROPERTY_CONTENT_TYPE type=\"integer\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <SchemaName>MDSCHEMA_SETS</SchemaName>" + nl +
                "            <Restrictions>" + nl +
                "              <CATALOG_NAME type=\"string\"/>" + nl +
                "              <SCHEMA_NAME type=\"string\"/>" + nl +
                "              <CUBE_NAME type=\"string\"/>" + nl +
                "              <SET_NAME type=\"string\"/>" + nl +
                "              <SCOPE type=\"integer\"/>" + nl +
                "            </Restrictions>" + nl +
                "            <Description/>" + nl +
                "          </row>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </DiscoverResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    public void testSelect() {
        assertRequestYields(wrap(
                "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" " + nl +
                "  SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <Command>" + nl +
                "    <Statement>select [Measures].members on Columns from Sales</Statement>" + nl +
                "  </Command>" + nl +
                "  <Properties>" + nl +
                "    <PropertyList>" + nl +
                "      <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "      <Catalog>FoodMart</Catalog>" + nl +
                "      <Format>Multidimensional</Format>" + nl +
                "      <AxisFormat>TupleFormat</AxisFormat>" + nl +
                "    </PropertyList>" + nl +
                "  </Properties>" + nl +
                "</Execute>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <ExecuteResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:mddataset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <OlapInfo>" + nl +
                "            <CubeInfo>" + nl +
                "              <Cube>" + nl +
                "                <CubeName>Sales</CubeName>" + nl +
                "              </Cube>" + nl +
                "            </CubeInfo>" + nl +
                "            <AxesInfo>" + nl +
                "              <AxisInfo name=\"Axis0\">" + nl +
                "                <HierarchyInfo name=\"Measures\">" + nl +
                "                  <UName name=\"[Measures].[MEMBER_UNIQUE_NAME]\"/>" + nl +
                "                  <Caption name=\"[Measures].[MEMBER_CAPTION]\"/>" + nl +
                "                  <LName name=\"[Measures].[LEVEL_UNIQUE_NAME]\"/>" + nl +
                "                  <LNum name=\"[Measures].[LEVEL_NUMBER]\"/>" + nl +
                "                </HierarchyInfo>" + nl +
                "              </AxisInfo>" + nl +
                "            </AxesInfo>" + nl +
                "            <CellInfo>" + nl +
                "              <Value name=\"VALUE\"/>" + nl +
                "              <FmtValue name=\"FORMATTED_VALUE\"/>" + nl +
                "              <FormatString name=\"FORMAT_STRING\"/>" + nl +
                "            </CellInfo>" + nl +
                "          </OlapInfo>" + nl +
                "          <Axes>" + nl +
                "            <Axis name=\"Axis0\">" + nl +
                "              <Tuples>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Measures\">" + nl +
                "                    <UName>[Measures].[Unit Sales]</UName>" + nl +
                "                    <Caption>Unit Sales</Caption>" + nl +
                "                    <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Measures\">" + nl +
                "                    <UName>[Measures].[Store Cost]</UName>" + nl +
                "                    <Caption>Store Cost</Caption>" + nl +
                "                    <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Measures\">" + nl +
                "                    <UName>[Measures].[Store Sales]</UName>" + nl +
                "                    <Caption>Store Sales</Caption>" + nl +
                "                    <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Measures\">" + nl +
                "                    <UName>[Measures].[Sales Count]</UName>" + nl +
                "                    <Caption>Sales Count</Caption>" + nl +
                "                    <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Measures\">" + nl +
                "                    <UName>[Measures].[Customer Count]</UName>" + nl +
                "                    <Caption>Customer Count</Caption>" + nl +
                "                    <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "              </Tuples>" + nl +
                "            </Axis>" + nl +
                "          </Axes>" + nl +
                "          <CellData>" + nl +
                "            <Cell CellOrdinal=\"0\">" + nl +
                "              <Value>266773</Value>" + nl +
                "              <FmtValue>266,773</FmtValue>" + nl +
                "              <FormatString>Standard</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"1\">" + nl +
                "              <Value>225627.2336</Value>" + nl +
                "              <FmtValue>225,627.23</FmtValue>" + nl +
                "              <FormatString>#,###.00</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"2\">" + nl +
                "              <Value>565238.13</Value>" + nl +
                "              <FmtValue>565,238.13</FmtValue>" + nl +
                "              <FormatString>#,###.00</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"3\">" + nl +
                "              <Value>86837</Value>" + nl +
                "              <FmtValue>86,837</FmtValue>" + nl +
                "              <FormatString>#,###</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"4\">" + nl +
                "              <Value>5581</Value>" + nl +
                "              <FmtValue>5,581</FmtValue>" + nl +
                "              <FormatString>#,###</FormatString>" + nl +
                "            </Cell>" + nl +
                "          </CellData>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </ExecuteResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
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
        assertRequestYields(wrap(
                "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" " + nl +
                "  SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <Command>" + nl +
                "    <Statement>select {[Gender].[F].PrevMember} on Columns from Sales</Statement>" + nl +
                "  </Command>" + nl +
                "  <Properties>" + nl +
                "    <PropertyList>" + nl +
                "      <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "      <Catalog>FoodMart</Catalog>" + nl +
                "      <Format>Multidimensional</Format>" + nl +
                "      <AxisFormat>TupleFormat</AxisFormat>" + nl +
                "    </PropertyList>" + nl +
                "  </Properties>" + nl +
                "</Execute>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <OLAPInfo>" + nl +
                "      <CubeInfo>" + nl +
                "        <Cube>" + nl +
                "          <CubeName>Sales</CubeName>" + nl +
                "        </Cube>" + nl +
                "      </CubeInfo>" + nl +
                "      <AxesInfo>" + nl +
                "        <AxisInfo name=\"Axis0\">" + nl +
                "          <HierarchyInfo name=\"Measures\">" + nl +
                "            <UName name=\"[Gender].[MEMBER_UNIQUE_NAME]\"/>" + nl +
                "            <Caption name=\"[Gender].[MEMBER_CAPTION]\"/>" + nl +
                "            <LName name=\"[Gender].[LEVEL_UNIQUE_NAME]\"/>" + nl +
                "            <LNum name=\"[Gender].[LEVEL_NUMBER]\"/>" + nl +
                "          </HierarchyInfo>" + nl +
                "        </AxisInfo>" + nl +
                "      </AxesInfo>" + nl +
                "      <CellInfo>" + nl +
                "        <Value name=\"VALUE\"/>" + nl +
                "        <FmtValue name=\"FORMATTED_VALUE\"/>" + nl +
                "        <FormatString name=\"FORMAT_STRING\"/>" + nl +
                "      </CellInfo>" + nl +
                "    </OLAPInfo>" + nl +
                "    <Axes>" + nl +
                "      <Axis name=\"Axis0\">" + nl +
                "        <Tuples/>" + nl +
                "      </Axis>" + nl +
                "    </Axes>" + nl +
                "    <CellData/>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    public void testSelect2() {
        assertRequestYields(wrap(
                "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" " + nl +
                "  SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <Command>" + nl +
                "    <Statement>SELECT {[Measures].[Org Salary], [Measures].[Count]} ON COLUMNS," + nl +
                "  {[Store].[USA].children * [Pay Type].Members} DIMENSION PROPERTIES [Store].[Store SQFT] ON ROWS" + nl +
                "FROM [HR]</Statement>" + nl +
                "  </Command>" + nl +
                "  <Properties>" + nl +
                "    <PropertyList>" + nl +
                "      <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "      <Catalog>FoodMart</Catalog>" + nl +
                "      <Format>Multidimensional</Format>" + nl +
                "      <AxisFormat>TupleFormat</AxisFormat>" + nl +
                "    </PropertyList>" + nl +
                "  </Properties>" + nl +
                "</Execute>"),

                "<?xml version=\"1.0\"?>" + nl +
                "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "  <SOAP-ENV:Body>" + nl +
                "    <ExecuteResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">" + nl +
                "      <return>" + nl +
                "        <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:mddataset\">" + nl +
                "          <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>" + nl +
                "          <OlapInfo>" + nl +
                "            <CubeInfo>" + nl +
                "              <Cube>" + nl +
                "                <CubeName>HR</CubeName>" + nl +
                "              </Cube>" + nl +
                "            </CubeInfo>" + nl +
                "            <AxesInfo>" + nl +
                "              <AxisInfo name=\"Axis0\">" + nl +
                "                <HierarchyInfo name=\"Measures\">" + nl +
                "                  <UName name=\"[Measures].[MEMBER_UNIQUE_NAME]\"/>" + nl +
                "                  <Caption name=\"[Measures].[MEMBER_CAPTION]\"/>" + nl +
                "                  <LName name=\"[Measures].[LEVEL_UNIQUE_NAME]\"/>" + nl +
                "                  <LNum name=\"[Measures].[LEVEL_NUMBER]\"/>" + nl +
                "                </HierarchyInfo>" + nl +
                "              </AxisInfo>" + nl +
                "              <AxisInfo name=\"Axis1\">" + nl +
                "                <HierarchyInfo name=\"Store\">" + nl +
                "                  <UName name=\"[Store].[MEMBER_UNIQUE_NAME]\"/>" + nl +
                "                  <Caption name=\"[Store].[MEMBER_CAPTION]\"/>" + nl +
                "                  <LName name=\"[Store].[LEVEL_UNIQUE_NAME]\"/>" + nl +
                "                  <LNum name=\"[Store].[LEVEL_NUMBER]\"/>" + nl +
                "                </HierarchyInfo>" + nl +
                "                <HierarchyInfo name=\"Pay Type\">" + nl +
                "                  <UName name=\"[Pay Type].[MEMBER_UNIQUE_NAME]\"/>" + nl +
                "                  <Caption name=\"[Pay Type].[MEMBER_CAPTION]\"/>" + nl +
                "                  <LName name=\"[Pay Type].[LEVEL_UNIQUE_NAME]\"/>" + nl +
                "                  <LNum name=\"[Pay Type].[LEVEL_NUMBER]\"/>" + nl +
                "                </HierarchyInfo>" + nl +
                "              </AxisInfo>" + nl +
                "            </AxesInfo>" + nl +
                "            <CellInfo>" + nl +
                "              <Value name=\"VALUE\"/>" + nl +
                "              <FmtValue name=\"FORMATTED_VALUE\"/>" + nl +
                "              <FormatString name=\"FORMAT_STRING\"/>" + nl +
                "            </CellInfo>" + nl +
                "          </OlapInfo>" + nl +
                "          <Axes>" + nl +
                "            <Axis name=\"Axis0\">" + nl +
                "              <Tuples>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Measures\">" + nl +
                "                    <UName>[Measures].[Org Salary]</UName>" + nl +
                "                    <Caption>Org Salary</Caption>" + nl +
                "                    <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Measures\">" + nl +
                "                    <UName>[Measures].[Count]</UName>" + nl +
                "                    <Caption>Count</Caption>" + nl +
                "                    <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "              </Tuples>" + nl +
                "            </Axis>" + nl +
                "            <Axis name=\"Axis1\">" + nl +
                "              <Tuples>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[CA]</UName>" + nl +
                "                    <Caption>CA</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types]</UName>" + nl +
                "                    <Caption>All Pay Types</Caption>" + nl +
                "                    <LName>[Pay Type].[(All)]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[CA]</UName>" + nl +
                "                    <Caption>CA</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types].[Hourly]</UName>" + nl +
                "                    <Caption>Hourly</Caption>" + nl +
                "                    <LName>[Pay Type].[Pay Type]</LName>" + nl +
                "                    <LNum>1</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[CA]</UName>" + nl +
                "                    <Caption>CA</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types].[Monthly]</UName>" + nl +
                "                    <Caption>Monthly</Caption>" + nl +
                "                    <LName>[Pay Type].[Pay Type]</LName>" + nl +
                "                    <LNum>1</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[OR]</UName>" + nl +
                "                    <Caption>OR</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types]</UName>" + nl +
                "                    <Caption>All Pay Types</Caption>" + nl +
                "                    <LName>[Pay Type].[(All)]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[OR]</UName>" + nl +
                "                    <Caption>OR</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types].[Hourly]</UName>" + nl +
                "                    <Caption>Hourly</Caption>" + nl +
                "                    <LName>[Pay Type].[Pay Type]</LName>" + nl +
                "                    <LNum>1</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[OR]</UName>" + nl +
                "                    <Caption>OR</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types].[Monthly]</UName>" + nl +
                "                    <Caption>Monthly</Caption>" + nl +
                "                    <LName>[Pay Type].[Pay Type]</LName>" + nl +
                "                    <LNum>1</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[WA]</UName>" + nl +
                "                    <Caption>WA</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types]</UName>" + nl +
                "                    <Caption>All Pay Types</Caption>" + nl +
                "                    <LName>[Pay Type].[(All)]</LName>" + nl +
                "                    <LNum>0</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[WA]</UName>" + nl +
                "                    <Caption>WA</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types].[Hourly]</UName>" + nl +
                "                    <Caption>Hourly</Caption>" + nl +
                "                    <LName>[Pay Type].[Pay Type]</LName>" + nl +
                "                    <LNum>1</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "                <Tuple>" + nl +
                "                  <Member Hierarchy=\"Store\">" + nl +
                "                    <UName>[Store].[All Stores].[USA].[WA]</UName>" + nl +
                "                    <Caption>WA</Caption>" + nl +
                "                    <LName>[Store].[Store State]</LName>" + nl +
                "                    <LNum>2</LNum>" + nl +
                "                  </Member>" + nl +
                "                  <Member Hierarchy=\"Pay Type\">" + nl +
                "                    <UName>[Pay Type].[All Pay Types].[Monthly]</UName>" + nl +
                "                    <Caption>Monthly</Caption>" + nl +
                "                    <LName>[Pay Type].[Pay Type]</LName>" + nl +
                "                    <LNum>1</LNum>" + nl +
                "                  </Member>" + nl +
                "                </Tuple>" + nl +
                "              </Tuples>" + nl +
                "            </Axis>" + nl +
                "          </Axes>" + nl +
                "          <CellData>" + nl +
                "            <Cell CellOrdinal=\"0\">" + nl +
                "              <Value>14861.5006</Value>" + nl +
                "              <FmtValue>$14,861.50</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"1\">" + nl +
                "              <Value>2316</Value>" + nl +
                "              <FmtValue>2,316</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"2\">" + nl +
                "              <Value>3261.2206</Value>" + nl +
                "              <FmtValue>$3,261.22</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"3\">" + nl +
                "              <Value>972</Value>" + nl +
                "              <FmtValue>972</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"4\">" + nl +
                "              <Value>11600.28</Value>" + nl +
                "              <FmtValue>$11,600.28</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"5\">" + nl +
                "              <Value>1344</Value>" + nl +
                "              <FmtValue>1,344</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"6\">" + nl +
                "              <Value>7848.9727</Value>" + nl +
                "              <FmtValue>$7,848.97</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"7\">" + nl +
                "              <Value>1632</Value>" + nl +
                "              <FmtValue>1,632</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"8\">" + nl +
                "              <Value>2663.8927</Value>" + nl +
                "              <FmtValue>$2,663.89</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"9\">" + nl +
                "              <Value>792</Value>" + nl +
                "              <FmtValue>792</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"10\">" + nl +
                "              <Value>5185.08</Value>" + nl +
                "              <FmtValue>$5,185.08</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"11\">" + nl +
                "              <Value>840</Value>" + nl +
                "              <FmtValue>840</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"12\">" + nl +
                "              <Value>16721.1979</Value>" + nl +
                "              <FmtValue>$16,721.20</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"13\">" + nl +
                "              <Value>3444</Value>" + nl +
                "              <FmtValue>3,444</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"14\">" + nl +
                "              <Value>5481.6379</Value>" + nl +
                "              <FmtValue>$5,481.64</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"15\">" + nl +
                "              <Value>1632</Value>" + nl +
                "              <FmtValue>1,632</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"16\">" + nl +
                "              <Value>11239.56</Value>" + nl +
                "              <FmtValue>$11,239.56</FmtValue>" + nl +
                "              <FormatString>Currency</FormatString>" + nl +
                "            </Cell>" + nl +
                "            <Cell CellOrdinal=\"17\">" + nl +
                "              <Value>1812</Value>" + nl +
                "              <FmtValue>1,812</FmtValue>" + nl +
                "              <FormatString>#,#</FormatString>" + nl +
                "            </Cell>" + nl +
                "          </CellData>" + nl +
                "        </root>" + nl +
                "      </return>" + nl +
                "    </ExecuteResponse>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    public static void assertEquals(String[] expected, String[] actual) {
        if (!equals(expected, actual)) {
            assertEquals((Object) expected, (Object) actual);
        }
    }

    public static boolean equals(String[] expected, String[] actual) {
        if (expected == null ||
                actual == null ||
                expected.length != actual.length) {
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            if (!Util.equals(expected[i], actual[i])) {
                return false;
            }
        }
        return true;
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
        final HashMap requestMap = getRequests();
        final Set requestKeySet = requestMap.keySet();
        final String[] requestKeys = (String[])
                requestKeySet.toArray(new String[requestKeySet.size()]);
        Arrays.sort(requestKeys);
        ArrayList requestList = new ArrayList();
        for (int i = 0; i < requestKeys.length; i++) {
            String requestKey = requestKeys[i];
            String request = (String) requestMap.get(requestKey);
            requestList.add(requestKey);
            requestList.add(request);
        }
        final String[] requests = (String[])
                requestList.toArray(new String[requestList.size()]);
        final String[] sampleRequests = XmlaUtil.getSampleRequests(catalogName, dataSource);
        if (!equals(requests, sampleRequests)) {
            System.out.println("Java:");
            System.out.println("{");
            for (int i = 0; i < requests.length; i++) {
                System.out.print(toJava(requests[i]));
                System.out.println(",");
            }
            System.out.println("}");
        }
        assertEquals(requests, sampleRequests);
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

}

// End XmlaTest.java
