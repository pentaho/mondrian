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

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;

import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.net.URL;
import java.net.MalformedURLException;

/**
 * Utilities for Mondrian's XML for Analysis API (package
 * <code>mondrian.xmla</code>).
 *
 * @author jhyde
 * @version $Id$
 **/
public class XmlaUtil {
    private static final String nl = System.getProperty("line.separator");

    public static Map getRequestMap() {
        String catalogName = MondrianProperties.instance().getCatalogURL();
        if (catalogName == null) {
            final File file = new File("demo/FoodMart.xml");
            if (!file.exists()) {
                throw new RuntimeException("CatalogURL must be specified");
            }
            URL url = null;
            try {
                url = Util.toURL(file);
            } catch (MalformedURLException e) {
            }
            catalogName = url.toString();
        }

        String driver = MondrianProperties.instance().getJdbcDrivers();
        String url = MondrianProperties.instance().getFoodmartJdbcURL();

        // Deal with embedded & that can be in the JDBC URL
        String dataSource =
                "Provider=Mondrian;"
                + "Jdbc=" + url + ";"
                + "Jdbc=" + url.replaceAll("&", "&amp;") + ";"
                + "JdbcDrivers=" + driver +";";
        final HashMap map = new HashMap();
        String[] sampleRequests = getSampleRequests(catalogName, dataSource);
        for (int i = 0; i < sampleRequests.length; i += 2) {
            map.put(sampleRequests[i], sampleRequests[i + 1]);
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
    public static String[] getSampleRequests(
            String catalogName, String dataSource) {
        return new String[] {
            "testDiscoverCatalogs",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverCubes",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverCubesRestricted",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverCubesRestrictedOnBadColumn",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverCubesRestrictedOnUnrestrictableColumnFails",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverDataSources",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_DATASOURCES</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList/>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList/>" + nl +
                "    </Properties>" + nl +
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverDimensions",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverEnumerators",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>DISCOVER_ENUMERATORS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList/>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList/>" + nl +
                "    </Properties>" + nl +
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverHierarchies",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverKeywords",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverLevels",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverLiterals",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverMembersRestrictedByHierarchy",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverMembersRestrictedByMemberAndTreeop",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <RequestType>MDSCHEMA_MEMBERS</RequestType>" + nl +
                "    <Restrictions>" + nl +
                "        <RestrictionList>" + nl +
                "            <CATALOG_NAME>" + catalogName + "</CATALOG_NAME>" + nl +
                "            <CUBE_NAME>Sales</CUBE_NAME>" + nl +
                "            <MEMBER_UNIQUE_NAME>[Time].[1997].[Q3]</MEMBER_UNIQUE_NAME>" + nl +
                "            <TREE_OP>35</TREE_OP>" + nl +
                "        </RestrictionList>" + nl +
                "    </Restrictions>" + nl +
                "    <Properties>" + nl +
                "        <PropertyList>" + nl +
                "            <DataSourceInfo>" + dataSource + "</DataSourceInfo>" + nl +
                "            <Catalog>FoodMart</Catalog>" + nl +
                "            <Format>Tabular</Format>" + nl +
                "        </PropertyList>" + nl +
                "    </Properties>" + nl +
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverProperties",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverPropertiesUnrestricted",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testDiscoverSchemaRowsets",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"" + nl +
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
                "</Discover>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testSelect",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" " + nl +
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
                "</Execute>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
            "testSelect2",
            "<SOAP-ENV:Envelope" + nl +
                "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"" + nl +
                "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" + nl +
                "    <SOAP-ENV:Body>" + nl +
                "        <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" " + nl +
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
                "</Execute>" + nl +
                "    </SOAP-ENV:Body>" + nl +
                "</SOAP-ENV:Envelope>",
        };
    }
}

// End XmlaUtil.java
