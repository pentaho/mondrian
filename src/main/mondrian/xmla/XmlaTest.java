/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde <jhyde@users.sf.net>
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    //private static final String dataSource = "Provider=MSOLAP;Data Source=local;";
    private static final String dataSource = "Provider=Mondrian;Jdbc=jdbc:odbc:MondrianFoodMart;Catalog=file:/E:/mondrian/demo/FoodMart.xml;JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver;";
    private static final String catalogName = "file:/E:/mondrian/demo/FoodMart.xml";

    public XmlaTest(String s) {
        super(s);
    }

    private String executeRequest(String request) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new XmlaMediator().process(request, baos);
        try {
            baos.flush();
        } catch (IOException e) {
        }
        return baos.toString();
    }

    private void assertRequestYields(String request, String expected) {
        final String response = executeRequest(request);
        assertEquals(expected, response);
    }

    public void _testDataSources() {
        String s = executeRequest("<Discover>" + nl +
                " <RequestType>");
        assertEquals("foo", s);
    }

    public void testCubes() {
        assertRequestYields(wrap(
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "            SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "            <RequestType>MDSCHEMA_CUBES</RequestType>" + nl +
                "            <Restrictions>" + nl +
                "                <RestrictionList>" + nl +
                "                    <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "                </RestrictionList>" + nl +
                "            </Restrictions>" + nl +
                "            <Properties>" + nl +
                "                <PropertyList>" + nl +
                "                    <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "                    <Catalog>FoodMart</Catalog>" + nl +
                "                    <Format>Tabular</Format>" + nl +
                "                </PropertyList>" + nl +
                "            </Properties>" + nl +
                "        </Discover>"),

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
                "            <CUBE_NAME>Store</CUBE_NAME>" + nl +
                "            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>" + nl +
                "            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>" + nl +
                "            <IS_LINKABLE>false</IS_LINKABLE>" + nl +
                "            <IS_SQL_ALLOWED>false</IS_SQL_ALLOWED>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Warehouse and Sales</CUBE_NAME>" + nl +
                "            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>" + nl +
                "            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>" + nl +
                "            <IS_LINKABLE>false</IS_LINKABLE>" + nl +
                "            <IS_SQL_ALLOWED>false</IS_SQL_ALLOWED>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>" + nl +
                "            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>" + nl +
                "            <IS_LINKABLE>false</IS_LINKABLE>" + nl +
                "            <IS_SQL_ALLOWED>false</IS_SQL_ALLOWED>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>HR</CUBE_NAME>" + nl +
                "            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>" + nl +
                "            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>" + nl +
                "            <IS_LINKABLE>false</IS_LINKABLE>" + nl +
                "            <IS_SQL_ALLOWED>false</IS_SQL_ALLOWED>" + nl +
                "          </row>" + nl +
                "          <row>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <SCHEMA_NAME>FoodMart</SCHEMA_NAME>" + nl +
                "            <CUBE_NAME>Warehouse</CUBE_NAME>" + nl +
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

    private String wrap(String request) {
        return "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                request + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>";
    }

    public void testCubesRestricted() {
        assertRequestYields(wrap(
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "            SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "            <RequestType>MDSCHEMA_CUBES</RequestType>" + nl +
                "            <Restrictions>" + nl +
                "                <RestrictionList>" + nl +
                "                    <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "                </RestrictionList>" + nl +
                "            </Restrictions>" + nl +
                "            <Properties>" + nl +
                "                <PropertyList>" + nl +
                "                    <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "                    <Catalog>FoodMart</Catalog>" + nl +
                "                    <Format>Tabular</Format>" + nl +
                "                </PropertyList>" + nl +
                "            </Properties>" + nl +
                "        </Discover>"),

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

    public void testCubesRestrictedOnUnrestrictableColumnFails() {
        assertRequestMatches(wrap(
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "            SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "            <RequestType>MDSCHEMA_CUBES</RequestType>" + nl +
                "            <Restrictions>" + nl +
                "                <RestrictionList>" + nl +
                "                    <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "                    <IS_WRITE_ENABLED>true</IS_WRITE_ENABLED>" + nl +
                "                </RestrictionList>" + nl +
                "            </Restrictions>" + nl +
                "            <Properties>" + nl +
                "                <PropertyList>" + nl +
                "                    <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "                    <Catalog>FoodMart</Catalog>" + nl +
                "                    <Format>Tabular</Format>" + nl +
                "                </PropertyList>" + nl +
                "            </Properties>" + nl +
                "        </Discover>"),

                "(?s).*Rowset 'MDSCHEMA_CUBES' column 'IS_WRITE_ENABLED' does not allow restrictions.*");
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

    public void testCubesRestrictedOnBadColumn() {
        assertRequestMatches(wrap(
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "            SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "            <RequestType>MDSCHEMA_CUBES</RequestType>" + nl +
                "            <Restrictions>" + nl +
                "                <RestrictionList>" + nl +
                "                    <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "                    <NON_EXISTENT_COLUMN>FooBar</NON_EXISTENT_COLUMN>" + nl +
                "                </RestrictionList>" + nl +
                "            </Restrictions>" + nl +
                "            <Properties>" + nl +
                "                <PropertyList>" + nl +
                "                    <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "                    <Catalog>FoodMart</Catalog>" + nl +
                "                    <Format>Tabular</Format>" + nl +
                "                </PropertyList>" + nl +
                "            </Properties>" + nl +
                "        </Discover>"),

                "(?s).*Rowset 'MDSCHEMA_CUBES' does not contain column 'NON_EXISTENT_COLUMN'.*");
    }

    public void testProperties() {
        assertRequestMatches(wrap(
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "            SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "            <RequestType>DISCOVER_PROPERTIES</RequestType>" + nl +
                "            <Restrictions>" + nl +
                "                <RestrictionList>" + nl +
                "                </RestrictionList>" + nl +
                "            </Restrictions>" + nl +
                "            <Properties>" + nl +
                "                <PropertyList>" + nl +
                "                    <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "                    <Format>Tabular</Format>" + nl +
                "                </PropertyList>" + nl +
                "            </Properties>" + nl +
                "        </Discover>"),

                "(?s).*" + nl +
                "          <row>" + nl +
                "            <PropertyName>Catalog</PropertyName>" + nl +
                "            <PropertyDescription>Specifies the initial catalog or database on which to connect.</PropertyDescription>" + nl +
                "            <PropertyType>String</PropertyType>" + nl +
                "            <PropertyAccessType>Read/Write</PropertyAccessType>" + nl +
                "            <IsRequired/>" + nl +
                "            <Value/>" + nl +
                "          </row>" + nl +
                ".*");
    }

    /**
     * Tests the {@link RowsetDefinition#DISCOVER_LITERALS} rowset, and
     * multiple restrictions.
     */
    public void testLiterals() {
        assertRequestYields(wrap(
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "            SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "            <RequestType>DISCOVER_LITERALS</RequestType>" + nl +
                "            <Restrictions>" + nl +
                "                <RestrictionList>" + nl +
                "                    <LiteralName>" + nl +
                "                        <Value>DBLITERAL_QUOTE_PREFIX</Value>" + nl +
                "                        <Value>DBLITERAL_QUOTE_SUFFIX</Value>" + nl +
                "                    </LiteralName>" + nl +
                "                </RestrictionList>" + nl +
                "            </Restrictions>" + nl +
                "            <Properties>" + nl +
                "                <PropertyList>" + nl +
                "                    <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "                    <Format>Tabular</Format>" + nl +
                "                </PropertyList>" + nl +
                "            </Properties>" + nl +
                "        </Discover>"),

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

    public void testPropertiesUnrestricted() {
        assertRequestYields(wrap(
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "            SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "            <RequestType>DISCOVER_PROPERTIES</RequestType>" + nl +
                "            <Restrictions>" + nl +
                "                <RestrictionList>" + nl +
                "                    <PropertyName>EndRange</PropertyName>" + nl +
                "                </RestrictionList>" + nl +
                "            </Restrictions>" + nl +
                "            <Properties>" + nl +
                "                <PropertyList>" + nl +
                "                    <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "                    <Format>Tabular</Format>" + nl +
                "                </PropertyList>" + nl +
                "            </Properties>" + nl +
                "        </Discover>"),

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
                "    <OLAPInfo>" + nl +
                "      <CubeInfo>" + nl +
                "        <Cube>" + nl +
                "          <CubeName>Sales</CubeName>" + nl +
                "        </Cube>" + nl +
                "      </CubeInfo>" + nl +
                "      <AxesInfo>" + nl +
                "        <AxisInfo name=\"Axis0\">" + nl +
                "          <HierarchyInfo name=\"Measures\">" + nl +
                "            <UName name=\"[Measures].[MEMBER_UNIQUE_NAME]\"/>" + nl +
                "            <Caption name=\"[Measures].[MEMBER_CAPTION]\"/>" + nl +
                "            <LName name=\"[Measures].[LEVEL_UNIQUE_NAME]\"/>" + nl +
                "            <LNum name=\"[Measures].[LEVEL_NUMBER]\"/>" + nl +
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
                "        <Tuples>" + nl +
                "          <Tuple>" + nl +
                "            <Member Hierarchy=\"Measures\">" + nl +
                "              <UName>[Measures].[Unit Sales]</UName>" + nl +
                "              <Caption>Unit Sales</Caption>" + nl +
                "              <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "              <LNum>0</LNum>" + nl +
                "            </Member>" + nl +
                "          </Tuple>" + nl +
                "          <Tuple>" + nl +
                "            <Member Hierarchy=\"Measures\">" + nl +
                "              <UName>[Measures].[Store Cost]</UName>" + nl +
                "              <Caption>Store Cost</Caption>" + nl +
                "              <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "              <LNum>0</LNum>" + nl +
                "            </Member>" + nl +
                "          </Tuple>" + nl +
                "          <Tuple>" + nl +
                "            <Member Hierarchy=\"Measures\">" + nl +
                "              <UName>[Measures].[Store Sales]</UName>" + nl +
                "              <Caption>Store Sales</Caption>" + nl +
                "              <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "              <LNum>0</LNum>" + nl +
                "            </Member>" + nl +
                "          </Tuple>" + nl +
                "          <Tuple>" + nl +
                "            <Member Hierarchy=\"Measures\">" + nl +
                "              <UName>[Measures].[Sales Count]</UName>" + nl +
                "              <Caption>Sales Count</Caption>" + nl +
                "              <LName>[Measures].[MeasuresLevel]</LName>" + nl +
                "              <LNum>0</LNum>" + nl +
                "            </Member>" + nl +
                "          </Tuple>" + nl +
                "        </Tuples>" + nl +
                "      </Axis>" + nl +
                "    </Axes>" + nl +
                "    <CellData>" + nl +
                "      <Cell CellOrdinal=\"0\">" + nl +
                "        <Value>266773.0</Value>" + nl +
                "        <FmtValue>266,773</FmtValue>" + nl +
                "        <FormatString>Standard</FormatString>" + nl +
                "      </Cell>" + nl +
                "      <Cell CellOrdinal=\"1\">" + nl +
                "        <Value>225627.2336</Value>" + nl +
                "        <FmtValue>225,627.23</FmtValue>" + nl +
                "        <FormatString>#,###.00</FormatString>" + nl +
                "      </Cell>" + nl +
                "      <Cell CellOrdinal=\"2\">" + nl +
                "        <Value>565238.1300</Value>" + nl +
                "        <FmtValue>565,238.13</FmtValue>" + nl +
                "        <FormatString>#,###.00</FormatString>" + nl +
                "      </Cell>" + nl +
                "      <Cell CellOrdinal=\"3\">" + nl +
                "        <Value>86837</Value>" + nl +
                "        <FmtValue>86,837</FmtValue>" + nl +
                "        <FormatString>#,###</FormatString>" + nl +
                "      </Cell>" + nl +
                "    </CellData>" + nl +
                "  </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>");
    }

    /**
     * select {[Gender].[F].PrevMember} on columns from Sales
     * If axis is empty, we can't deduce the hierarchies on it.
     */
    public void testSelectEmptyAxis() {

    }
}

// End XmlaTest.java
