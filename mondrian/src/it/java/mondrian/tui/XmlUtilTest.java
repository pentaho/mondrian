/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2005-2024 Hitachi Vantara and others
 * All Rights Reserved.
 */

package mondrian.tui;

import junit.framework.TestCase;
import mondrian.xmla.XmlaConstants;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.xpath.XPathException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class XmlUtilTest extends TestCase {

  public void testSelectAsNodesWithEmptyNamespace()
    throws IOException, SAXException, XPathException {
    String x = "<doc><foo><bar></bar><baz></baz></foo></doc>";
    Document doc = XmlUtil.parseString( x );
    Node[] nodes = XmlUtil.selectAsNodes(
      doc, "/doc/foo/*",
      Collections.<String, String>emptyMap() );

    assertEquals( 2, nodes.length );
    assertEquals( "bar", nodes[ 0 ].getNodeName() );
    assertEquals( "baz", nodes[ 1 ].getNodeName() );
  }

  public void testSmallWithSingleNamespace()
    throws IOException, SAXException, XPathException {
    String x = "<bop:doc xmlns:bop='http://foo.bar.baz'><bop:foo><bop:bar>"
      + "</bop:bar><bop:baz></bop:baz></bop:foo></bop:doc>";
    Document doc = XmlUtil.parseString( x );
    Node[] nodes = XmlUtil.selectAsNodes(
      doc, "/bop:doc/bop:foo/*",
      Collections.singletonMap( "bop", "http://foo.bar.baz" ) );

    assertEquals( 2, nodes.length );
    assertEquals( "bop:bar", nodes[ 0 ].getNodeName() );
    assertEquals( "bop:baz", nodes[ 1 ].getNodeName() );
  }

  public void testSelectAsNodesDiscover()
    throws IOException, SAXException, XPathException {
    String xml =
      "\n"
        + "<SOAP-ENV:Envelope\n"
        + "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"\n"
        + "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\n"
        + "  <SOAP-ENV:Body>\n"
        + "    <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"\n"
        + "        SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\n"
        + "    <RequestType>DISCOVER_ENUMERATORS</RequestType>\n"
        + "    <Restrictions>\n"
        + "      <RestrictionList>\n"
        + "      </RestrictionList>\n"
        + "    </Restrictions>\n"
        + "    <Properties>\n"
        + "      <PropertyList>\n"
        + "        <DataSourceInfo>FoodMart</DataSourceInfo>\n"
        + "        <Content>SchemaData</Content>\n"
        + "      </PropertyList>\n"
        + "    </Properties>\n"
        + "    </Discover>\n"
        + "</SOAP-ENV:Body>\n"
        + "</SOAP-ENV:Envelope>\n";
    Node[] nodes = XmlUtil.selectAsNodes(
      XmlUtil.parseString( xml ),
      "/SOAP-ENV:Envelope/SOAP-ENV:Body/*",
      Collections.singletonMap(
        XmlaConstants.SOAP_PREFIX,
        XmlaConstants.NS_SOAP_ENV_1_1 ) );
    assertEquals( nodes.length, 1 );
    assertEquals( "Discover", nodes[ 0 ].getNodeName() );
  }

  public void testSelectAsNodes3Namespaces()
    throws IOException, SAXException, XPathException {
    String soapResp =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" "
        + "SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" >\n"
        + "<SOAP-ENV:Header>\n"
        + "</SOAP-ENV:Header>\n"
        + "<SOAP-ENV:Body>\n"
        + "<DiscoverResponse xmlns=\"urn:schemas-microsoft-com:xml-analysis\">\n"
        + "  <return>\n"
        + "    <root xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3"
        + ".org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" "
        + "xmlns:EX=\"urn:schemas-microsoft-com:xml-analysis:exception\">\n"
        + "      <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" "
        + "xmlns=\"urn:schemas-microsoft-com:xml-analysis:rowset\" xmlns:xsi=\"http://www.w3"
        + ".org/2001/XMLSchema-instance\" xmlns:sql=\"urn:schemas-microsoft-com:xml-sql\" "
        + "targetNamespace=\"urn:schemas-microsoft-com:xml-analysis:rowset\" elementFormDefault=\"qualified\">\n"
        + "        <xsd:element name=\"root\">\n"
        + "          <xsd:complexType>\n"
        + "            <xsd:sequence>\n"
        + "              <xsd:element name=\"row\" type=\"row\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n"
        + "            </xsd:sequence>\n"
        + "          </xsd:complexType>\n"
        + "        </xsd:element>\n"
        + "        <xsd:simpleType name=\"uuid\">\n"
        + "          <xsd:restriction base=\"xsd:string\">\n"
        + "            <xsd:pattern value=\"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F"
        + "]{12}\"/>\n"
        + "          </xsd:restriction>\n"
        + "        </xsd:simpleType>\n"
        + "        <xsd:complexType name=\"row\">\n"
        + "          <xsd:sequence>\n"
        + "            <xsd:element sql:field=\"DataSourceName\" name=\"DataSourceName\" type=\"xsd:string\"/>\n"
        + "            <xsd:element sql:field=\"DataSourceDescription\" name=\"DataSourceDescription\" "
        + "type=\"xsd:string\" minOccurs=\"0\"/>\n"
        + "            <xsd:element sql:field=\"URL\" name=\"URL\" type=\"xsd:string\" minOccurs=\"0\"/>\n"
        + "            <xsd:element sql:field=\"DataSourceInfo\" name=\"DataSourceInfo\" type=\"xsd:string\" "
        + "minOccurs=\"0\"/>\n"
        + "            <xsd:element sql:field=\"ProviderName\" name=\"ProviderName\" type=\"xsd:string\" "
        + "minOccurs=\"0\"/>\n"
        + "            <xsd:element sql:field=\"ProviderType\" name=\"ProviderType\" type=\"xsd:string\" "
        + "maxOccurs=\"unbounded\"/>\n"
        + "            <xsd:element sql:field=\"AuthenticationMode\" name=\"AuthenticationMode\" "
        + "type=\"xsd:string\"/>\n"
        + "          </xsd:sequence>\n"
        + "        </xsd:complexType>\n"
        + "      </xsd:schema>\n"
        + "      <row>\n"
        + "        <DataSourceName>FoodMart</DataSourceName>\n"
        + "        <DataSourceDescription>Mondrian FoodMart data source</DataSourceDescription>\n"
        + "        <URL>http://localhost:8080/mondrian/xmla</URL>\n"
        + "        <DataSourceInfo>Catalog=file:/Users/mcampbell/dev/mondrian/demo/FoodMart.xml; Provider=mondrian; "
        + "PoolNeeded=false</DataSourceInfo>\n"
        + "        <ProviderName>Mondrian</ProviderName>\n"
        + "        <ProviderType>MDP</ProviderType>\n"
        + "        <AuthenticationMode>Unauthenticated</AuthenticationMode>\n"
        + "      </row>\n"
        + "    </root>\n"
        + "  </return>\n"
        + "</DiscoverResponse>\n"
        + "</SOAP-ENV:Body>\n"
        + "</SOAP-ENV:Envelope>\n";

    String xpath =
      "/SOAP-ENV:Envelope/SOAP-ENV:Body/xmla:DiscoverResponse/xmla:return/ROW:root/*";
    Map<String, String> namespaces = new HashMap<>();
    namespaces.put(
      XmlaConstants.SOAP_PREFIX, XmlaConstants.NS_SOAP_ENV_1_1 );
    namespaces.put( "xmla", XmlaConstants.NS_XMLA );
    namespaces.put( "ROW", XmlaConstants.NS_XMLA_ROWSET );
    Node[] nodes = XmlUtil.selectAsNodes(
      XmlUtil.parseString( soapResp ),
      xpath,
      namespaces );
    assertEquals( 2, nodes.length );
    assertEquals( "xsd:schema", nodes[ 0 ].getNodeName() );
    assertEquals( "row", nodes[ 1 ].getNodeName() );
  }
}
