/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.xmla.test;

import mondrian.olap.*;
import mondrian.server.StringRepositoryContentFinder;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.xmla.*;
import mondrian.xmla.impl.DefaultXmlaRequest;
import mondrian.xmla.impl.DefaultXmlaResponse;

import junit.framework.*;

import org.apache.log4j.Logger;

import org.custommonkey.xmlunit.XMLAssert;
import org.custommonkey.xmlunit.XMLUnit;
import org.w3c.dom.*;

import java.io.*;
import java.util.Iterator;
import java.util.List;
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
            Logger.getLogger(XmlaTest.class);

    static {
        XMLUnit.setControlParser(
            "org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
        XMLUnit.setTestParser(
            "org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
        XMLUnit.setIgnoreWhitespace(true);
    }

    private static final XmlaTestContext context = new XmlaTestContext();

    private XmlaHandler handler;
    private MondrianServer server;

    public XmlaTest(String name) {
        super(name);
    }

    // implement TestCase
    protected void setUp() throws Exception {
        super.setUp();
        DiffRepository diffRepos = getDiffRepos();
        diffRepos.setCurrentTestCaseName(getName());
        server = MondrianServer.createWithRepository(
            new StringRepositoryContentFinder(
                context.getDataSourcesString()),
            XmlaTestContext.CATALOG_LOCATOR);
        handler = new XmlaHandler(
            (XmlaHandler.ConnectionFactory) server,
            "xmla");
        XMLUnit.setIgnoreWhitespace(false);
    }

    // implement TestCase
    protected void tearDown() throws Exception {
        DiffRepository diffRepos = getDiffRepos();
        diffRepos.setCurrentTestCaseName(null);
        server.shutdown();
        server = null;
        handler = null;
        super.tearDown();
    }

    private static DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaTest.class);
    }

    protected void runTest() throws Exception {
        DiffRepository diffRepos = getDiffRepos();
        String request = diffRepos.expand(null, "${request}");
        String expectedResponse = diffRepos.expand(null, "${response}");
        Element requestElem = XmlaUtil.text2Element(
            XmlaTestContext.xmlFromTemplate(
                request, XmlaTestContext.ENV));
        Element responseElem =
            ignoreLastUpdateDate(executeRequest(requestElem));

        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();
        StringWriter bufWriter = new StringWriter();
        transformer.transform(
            new DOMSource(responseElem), new StreamResult(bufWriter));
        bufWriter.write(Util.nl);
        String actualResponse =
            TestContext.instance().upgradeActual(
                bufWriter.getBuffer().toString());
        try {
            // Start with a purely logical XML diff to avoid test noise
            // from non-determinism in XML generation.
            XMLAssert.assertXMLEqual(expectedResponse, actualResponse);
        } catch (AssertionFailedError e) {
            // In case of failure, re-diff using DiffRepository's comparison
            // method. It may have noise due to physical vs logical structure,
            // but it will maintain the expected/actual, and some IDEs can even
            // display visual diffs.
            diffRepos.assertEquals("response", "${response}", actualResponse);
        }
    }

    private Element ignoreLastUpdateDate(Element element) {
        NodeList elements = element.getElementsByTagName("LAST_SCHEMA_UPDATE");
        for (int i = elements.getLength(); i > 0; i--) {
            blankNode(elements.item(i - 1));
        }
        return element;
    }

    private void blankNode(Node node) {
        node.setTextContent("");
    }

    private Element executeRequest(Element requestElem) {
        ByteArrayOutputStream resBuf = new ByteArrayOutputStream();
        XmlaRequest request =
            new DefaultXmlaRequest(requestElem, null, null, null, null);
        XmlaResponse response =
            new DefaultXmlaResponse(
                resBuf, "UTF-8", Enumeration.ResponseMimeType.SOAP);
        handler.process(request, response);

        return XmlaUtil.stream2Element(
            new ByteArrayInputStream(resBuf.toByteArray()));
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();

        DiffRepository diffRepos = getDiffRepos();

        MondrianProperties properties = MondrianProperties.instance();
        String filePattern = properties.QueryFilePattern.get();

        final Pattern pattern =
            filePattern == null
            ? null
            : Pattern.compile(filePattern);

        List<String> testCaseNames = diffRepos.getTestCaseNames();

        if (pattern != null) {
            Iterator<String> iter = testCaseNames.iterator();
            while (iter.hasNext()) {
                String name = iter.next();
                if (!pattern.matcher(name).matches()) {
                    iter.remove();
                }
            }
        }

        LOGGER.debug("Found " + testCaseNames.size() + " XML/A test cases");

        for (String name : testCaseNames) {
            suite.addTest(new XmlaTest(name));
        }

        suite.addTestSuite(OtherTest.class);

        return suite;
    }

    /**
     * Non diff-based unit tests for XML/A support.
     */
    public static class OtherTest extends TestCase {
        public void testEncodeElementName() {
            final XmlaUtil.ElementNameEncoder encoder =
                XmlaUtil.ElementNameEncoder.INSTANCE;

            assertEquals("Foo", encoder.encode("Foo"));
            assertEquals("Foo_x0020_Bar", encoder.encode("Foo Bar"));

            if (false) // FIXME:
            assertEquals(
                "Foo_x00xx_Bar", encoder.encode("Foo_Bar"));

            // Caching: decode same string, get same string back
            final String s1 = encoder.encode("Abc def");
            final String s2 = encoder.encode("Abc def");
            assertSame(s1, s2);
        }

        /**
         * Unit test for {@link XmlaUtil#chooseResponseMimeType(String)}.
         */
        public void testAccept() {
            // simple
            assertEquals(
                Enumeration.ResponseMimeType.SOAP,
                XmlaUtil.chooseResponseMimeType("application/xml"));

            // deal with ",q=<n>" quality codes by ignoring them
            assertEquals(
                Enumeration.ResponseMimeType.SOAP,
                XmlaUtil.chooseResponseMimeType(
                    "text/html,application/xhtml+xml,"
                    + "application/xml;q=0.9,*/*;q=0.8"));

            // return null if nothing matches
            assertNull(
                XmlaUtil.chooseResponseMimeType(
                    "text/html,application/xhtml+xml"));

            // quality codes all over the place; return JSON because we see
            // it before application/xml
            assertEquals(
                Enumeration.ResponseMimeType.JSON,
                XmlaUtil.chooseResponseMimeType(
                    "text/html;q=0.9,"
                    + "application/xhtml+xml;q=0.9,"
                    + "application/json;q=0.9,"
                    + "application/xml;q=0.9,"
                    + "*/*;q=0.8"));

            // allow application/soap+xml as synonym for application/xml
            assertEquals(
                Enumeration.ResponseMimeType.SOAP,
                XmlaUtil.chooseResponseMimeType(
                    "text/html,application/soap+xml"));

            // allow text/xml as synonym for application/xml
            assertEquals(
                Enumeration.ResponseMimeType.SOAP,
                XmlaUtil.chooseResponseMimeType(
                    "text/html,application/soap+xml"));
        }
    }
}

// End XmlaTest.java
