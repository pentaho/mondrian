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

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for Mondrian's XML for Analysis API (package
 * <code>mondrian.xmla</code>).
 *
 * @author jhyde
 * @version $Id$
 **/
public class XmlaUtil {

    /**
     * Called from <code>xmlaTest.jsp</code>.
     */
    public static Map getRequestMap() {
        final HashMap map = new HashMap();
        XmlaRequest[] sampleRequests = getSampleRequests("FoodMart", "MondrianFoodMart");
        for (int i = 0; i < sampleRequests.length; i += 2) {
            final XmlaRequest sampleRequest = sampleRequests[i];
            map.put(sampleRequest.name, sampleRequest.request);
        }
        return map;
    }

    /**
     * Returns a list of sample requests. The array is of even length,
     * consisting of a test name (such as "testDiscoverCatalogs") followed by
     * an XML fragment.
     *
     * @param catalogName The URL of the catlaog
     * @param dataSource
     * @return
     */
    public static XmlaRequest[] getSampleRequests(
            String catalogName, String dataSource) {
        return new XmlaRequest[] {
            new XmlaRequest(
                    "testDiscoverCatalogs",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverCubes",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverCubesRestricted",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverCubesRestrictedOnBadColumn",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverCubesRestrictedOnUnrestrictableColumnFails",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverDataSources",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverDimensions",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverEnumerators",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <RequestType>DISCOVER_ENUMERATORS</RequestType>",
                        "    <Restrictions>",
                        "        <RestrictionList/>",
                        "    </Restrictions>",
                        "    <Properties>",
                        "        <PropertyList/>",
                        "    </Properties>",
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverHierarchies",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverKeywords",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverLevels",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest("testDiscoverLiterals",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest("testDiscoverMeasures",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverMembersRestrictedByHierarchy",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverMembersRestrictedByMemberAndTreeop",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <RequestType>MDSCHEMA_MEMBERS</RequestType>",
                        "    <Restrictions>",
                        "        <RestrictionList>",
                        "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>",
                        "            <CUBE_NAME>Sales</CUBE_NAME>",
                        "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3]</MEMBER_UNIQUE_NAME>",
                        "            <TREE_OP>35</TREE_OP>",
                        "        </RestrictionList>",
                        "    </Restrictions>",
                        "    <Properties>",
                        "        <PropertyList>",
                        "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>",
                        "            <Catalog>FoodMart</Catalog>",
                        "            <Format>Tabular</Format>",
                        "        </PropertyList>",
                        "    </Properties>",
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverProperties",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest(
                    "testDiscoverPropertiesUnrestricted",
                    new String[] {
                        "<SOAP-ENV:Envelope",
                        "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                        "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                        "    <SOAP-ENV:Body>",
                        "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                        "</Discover>",
                        "    </SOAP-ENV:Body>",
                        "</SOAP-ENV:Envelope>"}),
            new XmlaRequest("testDiscoverSchemaRowsets", new String[] {
                "<SOAP-ENV:Envelope",
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                "    <SOAP-ENV:Body>",
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"",
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
                "</Discover>",
                "    </SOAP-ENV:Body>",
                "</SOAP-ENV:Envelope>"}),
            new XmlaRequest("testSelect", new String[] {
                "<SOAP-ENV:Envelope",
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                "    <SOAP-ENV:Body>",
                "        <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" ",
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
                "</Execute>",
                "    </SOAP-ENV:Body>",
                "</SOAP-ENV:Envelope>"}),
            new XmlaRequest("testSelect2", new String[] {
                "<SOAP-ENV:Envelope",
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                "    <SOAP-ENV:Body>",
                "        <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" ",
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
                "</Execute>",
                "    </SOAP-ENV:Body>",
                "</SOAP-ENV:Envelope>"}),
            new XmlaRequest("testXmlaError", new String[] {
                "<SOAP-ENV:Envelope",
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"",
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">",
                "    <SOAP-ENV:Body>",
                "        <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" ",
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
                "</Execute>",
                "    </SOAP-ENV:Body>",
                "</SOAP-ENV:Envelope>"}),
        };
    }

    public static class XmlaRequest {
        final String name;
        final String request;

        XmlaRequest(String name, String[] lines) {
            this.name = name;
            this.request = XmlaTest.fold(lines);
        }

        XmlaRequest(String name, String request) {
            this.name = name;
            this.request = request;
        }

        public boolean equals(Object that) {
            return that instanceof XmlaRequest &&
                    this.name.equals(((XmlaRequest) that).name) &&
                    this.request.equals(((XmlaRequest) that).request);
        }

        public int hashCode() {
            return name.hashCode() ^ request.hashCode();
        }

        public String toString() {
            return "XmlaRequest(name=" + name + ", request=" + request + ")";
        }
    }
}

// End XmlaUtil.java
