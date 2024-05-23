/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2002-2024 Hitachi Vantara
 * All Rights Reserved.
 */

package mondrian.xmla.test;

import mondrian.olap.*;
import mondrian.olap.Util.PropertyList;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.server.StringRepositoryContentFinder;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.xmla.*;
import mondrian.xmla.impl.DefaultXmlaRequest;
import mondrian.xmla.impl.DefaultXmlaResponse;

import junit.framework.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.custommonkey.xmlunit.XMLAssert;
import org.custommonkey.xmlunit.XMLUnit;
import org.w3c.dom.*;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * Unit test for refined Mondrian's XML for Analysis API (package
 * {@link mondrian.xmla}).
 *
 * @author Gang Chen
 */
public class XmlaTest extends TestCase {

  private static final Logger LOGGER =
    LogManager.getLogger( XmlaTest.class );
  private static final XmlaTestContext context = new XmlaTestContext();
  private static final String DATA_SOURCE_INFO_RESPONSE_PROP =
    "data.source.info.response";

  static {
    XMLUnit.setControlParser(
      "org.apache.xerces.jaxp.DocumentBuilderFactoryImpl" );
    XMLUnit.setTestParser(
      "org.apache.xerces.jaxp.DocumentBuilderFactoryImpl" );
    XMLUnit.setIgnoreWhitespace( true );
  }

  private XmlaHandler handler;
  private MondrianServer server;

  public XmlaTest( String name ) {
    super( name );
  }

  private static DiffRepository getDiffRepos() {
    return DiffRepository.lookup( XmlaTest.class );
  }

  public static TestSuite suite() {
    TestSuite suite = new TestSuite();

    DiffRepository diffRepos = getDiffRepos();

    MondrianProperties properties = MondrianProperties.instance();
    String filePattern = properties.QueryFilePattern.get();

    final Pattern pattern =
      filePattern == null
        ? null
        : Pattern.compile( filePattern );

    List<String> testCaseNames = diffRepos.getTestCaseNames();

    if ( pattern != null ) {
      Iterator<String> iter = testCaseNames.iterator();
      while ( iter.hasNext() ) {
        String name = iter.next();
        if ( !pattern.matcher( name ).matches() ) {
          iter.remove();
        }
      }
    }

    LOGGER.debug( "Found " + testCaseNames.size() + " XML/A test cases" );

    for ( String name : testCaseNames ) {
      suite.addTest( new XmlaTest( name ) );
    }

    suite.addTestSuite( OtherTest.class );

    return suite;
  }

  // implement TestCase
  protected void setUp() throws Exception {
    super.setUp();
    DiffRepository diffRepos = getDiffRepos();
    diffRepos.setCurrentTestCaseName( getName() );
    server = MondrianServer.createWithRepository(
      new StringRepositoryContentFinder(
        context.getDataSourcesString() ),
      XmlaTestContext.CATALOG_LOCATOR );
    handler = new XmlaHandler( (XmlaHandler.ConnectionFactory) server );
    XMLUnit.setIgnoreWhitespace( false );
  }

  // implement TestCase
  protected void tearDown() throws Exception {
    DiffRepository diffRepos = getDiffRepos();
    diffRepos.setCurrentTestCaseName( null );
    server.shutdown();
    server = null;
    handler = null;
    super.tearDown();
  }

  protected void runTest() throws Exception {
    if ( !MondrianProperties.instance().SsasCompatibleNaming.get()
      && getName().equals( "mdschemaLevelsCubeDimRestrictions" ) ) {
      // Changes in unique names of hierarchies and levels mean that the
      // output is a different order in the old behavior, and cannot be
      // fixed by a few sed-like comparisons.
      return;
    }
    DiffRepository diffRepos = getDiffRepos();
    String request = diffRepos.expand( null, "${request}" );
    String expectedResponse = diffRepos.expand( null, "${response}" );

    Properties props = new Properties();
    XmlaTestContext s = new XmlaTestContext();
    String con = s.getConnectString().replaceAll( "&amp;", "&" );
    PropertyList pl = Util.parseConnectString( con );
    pl.remove( RolapConnectionProperties.Jdbc.name() );
    pl.remove( RolapConnectionProperties.JdbcUser.name() );
    pl.remove( RolapConnectionProperties.JdbcPassword.name() );
    props.setProperty( DATA_SOURCE_INFO_RESPONSE_PROP, pl.toString() );
    expectedResponse =
      Util.replaceProperties(
        expectedResponse, Util.toMap( props ) );

    Element requestElem = XmlaUtil.text2Element(
      XmlaTestContext.xmlFromTemplate(
        request, XmlaTestContext.ENV ) );
    Element responseElem = executeRequest( requestElem );
    responseElem = ignoreLastUpdateDate( responseElem, "LAST_SCHEMA_UPDATE" );
    responseElem = ignoreLastUpdateDate( responseElem, "LAST_DATA_UPDATE" );

    TransformerFactory factory = TransformerFactory.newInstance();
    Transformer transformer = factory.newTransformer();
    StringWriter bufWriter = new StringWriter();
    transformer.transform(
      new DOMSource( responseElem ), new StreamResult( bufWriter ) );
    bufWriter.write( Util.nl );
    String actualResponse =
      TestContext.instance().upgradeActual(
        bufWriter.getBuffer().toString() );
    try {
      // Start with a purely logical XML diff to avoid test noise
      // from non-determinism in XML generation.
      XMLAssert.assertXMLEqual( expectedResponse, actualResponse );
    } catch ( AssertionFailedError e ) {
      // In case of failure, re-diff using DiffRepository's comparison
      // method. It may have noise due to physical vs logical structure,
      // but it will maintain the expected/actual, and some IDEs can even
      // display visual diffs.
      diffRepos.assertEquals( "response", "${response}", actualResponse );
    }
  }

  private Element ignoreLastUpdateDate( Element element, String nodeName ) {
    NodeList elements = element.getElementsByTagName( nodeName );

    for ( int i = elements.getLength(); i > 0; i-- ) {
      blankNode( elements.item( i - 1 ) );
    }

    return element;
  }

  private void blankNode( Node node ) {
    node.setTextContent( "" );
  }

  private Element executeRequest( Element requestElem ) {
    ByteArrayOutputStream resBuf = new ByteArrayOutputStream();
    XmlaRequest request =
      new DefaultXmlaRequest( requestElem, null, null, null, null );
    XmlaResponse response =
      new DefaultXmlaResponse(
        resBuf, "UTF-8", Enumeration.ResponseMimeType.SOAP );
    handler.process( request, response );

    return XmlaUtil.stream2Element(
      new ByteArrayInputStream( resBuf.toByteArray() ) );
  }

  /**
   * Non diff-based unit tests for XML/A support.
   */
  public static class OtherTest extends TestCase {
    public void testEncodeElementName() {
      final XmlaUtil.ElementNameEncoder encoder =
        XmlaUtil.ElementNameEncoder.INSTANCE;

      assertEquals( "Foo", encoder.encode( "Foo" ) );
      assertEquals( "Foo_x0020_Bar", encoder.encode( "Foo Bar" ) );

      if ( false ) // FIXME:
      {
        assertEquals(
          "Foo_x00xx_Bar", encoder.encode( "Foo_Bar" ) );
      }

      // Caching: decode same string, get same string back
      final String s1 = encoder.encode( "Abc def" );
      final String s2 = encoder.encode( "Abc def" );
      assertSame( s1, s2 );
    }

    /**
     * Unit test for {@link XmlaUtil#chooseResponseMimeType(String)}.
     */
    public void testAccept() {
      // simple
      assertEquals(
        Enumeration.ResponseMimeType.SOAP,
        XmlaUtil.chooseResponseMimeType( "application/xml" ) );

      // deal with ",q=<n>" quality codes by ignoring them
      assertEquals(
        Enumeration.ResponseMimeType.SOAP,
        XmlaUtil.chooseResponseMimeType(
          "text/html,application/xhtml+xml,"
            + "application/xml;q=0.9,*/*;q=0.8" ) );

      // return null if nothing matches
      assertNull(
        XmlaUtil.chooseResponseMimeType(
          "text/html,application/xhtml+xml" ) );

      // quality codes all over the place; return JSON because we see
      // it before application/xml
      assertEquals(
        Enumeration.ResponseMimeType.JSON,
        XmlaUtil.chooseResponseMimeType(
          "text/html;q=0.9,"
            + "application/xhtml+xml;q=0.9,"
            + "application/json;q=0.9,"
            + "application/xml;q=0.9,"
            + "*/*;q=0.8" ) );

      // allow application/soap+xml as synonym for application/xml
      assertEquals(
        Enumeration.ResponseMimeType.SOAP,
        XmlaUtil.chooseResponseMimeType(
          "text/html,application/soap+xml" ) );

      // allow text/xml as synonym for application/xml
      assertEquals(
        Enumeration.ResponseMimeType.SOAP,
        XmlaUtil.chooseResponseMimeType(
          "text/html,application/soap+xml" ) );
    }
  }
}
