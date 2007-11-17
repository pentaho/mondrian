/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.test;

import mondrian.xmla.*;
import mondrian.xmla.impl.DefaultXmlaRequest;
import mondrian.xmla.impl.DefaultXmlaResponse;
import mondrian.olap.*;
import mondrian.rolap.RolapSchema;
import mondrian.test.DiffRepository;
import mondrian.util.Bug;

import junit.framework.*;
import org.apache.log4j.Logger;
import org.custommonkey.xmlunit.XMLAssert;
import org.custommonkey.xmlunit.XMLUnit;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.*;
import java.util.*;
import java.util.regex.*;

/**
 * Unit test for refined Mondrian's XML for Analysis API (package
 * {@link mondrian.xmla}).
 *
 * @author Gang Chen
 * @version $Id$
 */
public class XmlaTest extends TestCase {

    private static final Logger LOGGER =
            Logger.getLogger(XmlaTest.class);

    static {
        XMLUnit.setControlParser("org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
        XMLUnit.setTestParser("org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
        XMLUnit.setIgnoreWhitespace(true);
    }

    private final static XmlaTestContext context = new XmlaTestContext();

    public XmlaTest(String name) {
        super(name);
    }

    // implement TestCase
    protected void setUp() throws Exception {
        super.setUp();
        DiffRepository diffRepos = getDiffRepos();
        diffRepos.setCurrentTestCaseName(getName());
    }

    // implement TestCase
    protected void tearDown() throws Exception {
        DiffRepository diffRepos = getDiffRepos();
        diffRepos.setCurrentTestCaseName(null);
        super.tearDown();
    }

    private static DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaTest.class);
    }

    protected void runTest() throws Exception {
        
        if (!Bug.Bug1833526Fixed && this.getName().equals("executeHR")) { // BUG ID HERE
            // without flushing the cache, executeHR()
            // fails right now, due to some form of caching issue. This test passes when 
            // executed on it's own, but fails when ran in the test suite.  Looking into it
            // further, XmlaBasicTest.testMDimensions() calls LevelBase.setApproxRowCount()
            // on the level HR -> [Employee].[Employee Id]. In the old code base, by the 
            // time executeHR() is written, this value is no longer set.  With the new code,
            // this value is still set, causing a diff in the expected Xmla.
            
            // getConnection().getCacheControl(null).flushSchemaCache();
            RolapSchema.clearCache();
        }
        
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
        String actualResponse = bufWriter.getBuffer().toString();

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
            removeNode(elements.item(i-1));
        }
        return element;
    }

    private void removeNode(Node node) {
        Node parentNode = node.getParentNode();
        parentNode.removeChild(node);
    }

    private static Element executeRequest(Element requestElem) {
        ByteArrayOutputStream resBuf = new ByteArrayOutputStream();

        XmlaHandler handler =
            new XmlaHandler(
                context.dataSources(),
                XmlaTestContext.CATALOG_LOCATOR,
                "xmla");
        XmlaRequest request = new DefaultXmlaRequest(requestElem);
        XmlaResponse response = new DefaultXmlaResponse(resBuf, "UTF-8");
        handler.process(request, response);

        return XmlaUtil.stream2Element(new ByteArrayInputStream(resBuf.toByteArray()));
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();

        DiffRepository diffRepos = getDiffRepos();

        MondrianProperties properties = MondrianProperties.instance();
        String filePattern = properties.QueryFilePattern.get();

        final Pattern pattern = filePattern == null ?
            null :
            Pattern.compile(filePattern);

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
            assertEquals("Foo", XmlaUtil.encodeElementName("Foo"));
            assertEquals("Foo_x0020_Bar", XmlaUtil.encodeElementName("Foo Bar"));
            if (false) // FIXME:
            assertEquals("Foo_x00xx_Bar", XmlaUtil.encodeElementName("Foo_Bar"));
        }
    }
}

// End XmlaTest.java
