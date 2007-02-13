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

    private final File testFile;

    public XmlaTest(String name) {
        super(name);
        testFile = findFile(name);;
    }

    public XmlaTest(File file) {
        super(file.getName());
        testFile = file;
    }

    protected File findFile(String filename) {
        File[] files = context.retrieveQueryFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (file.getName().equals(filename)) {
                return file;
            }
        }
        return null;
    }

    protected void runTest() throws Exception {
        Element[] xmlaCyclePair =
            XmlaTestContext.extractXmlaCycle(testFile, XmlaTestContext.ENV);
        Element requestElem = xmlaCyclePair[0];
        Element expectedResponseElem = xmlaCyclePair[1];
        Element responseElem = ignoreLastUpdateDate(executeRequest(requestElem));
        compareElement(expectedResponseElem, responseElem);
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
        XmlaRequest request = new DefaultXmlaRequest(requestElem, null);
        XmlaResponse response = new DefaultXmlaResponse(resBuf, "UTF-8");
        handler.process(request, response);

        return XmlaUtil.stream2Element(new ByteArrayInputStream(resBuf.toByteArray()));
    }

    private static void compareElement(Element expectedElem, Element actualElem) throws Exception {
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();

        StringWriter bufWriter = new StringWriter();
        transformer.transform(new DOMSource(expectedElem), new StreamResult(bufWriter));
        String expectedText = bufWriter.getBuffer().toString();
        bufWriter = new StringWriter();
        transformer.transform(new DOMSource(actualElem), new StreamResult(bufWriter));
        String actualText = bufWriter.getBuffer().toString();
        try {
            XMLAssert.assertXMLEqual(expectedText, actualText);
        } catch (AssertionFailedError e) {
            // Use junit's comparison method. We know it will fail, but it will
            // print the expected/actual, and some IDEs can even display visual
            // diffs.
            assertEquals(expectedText, actualText);
        }
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();

        File[] files = context.retrieveQueryFiles();
        LOGGER.debug("Found " + files.length + " XML/A test files");
        for (int idx = 0; idx < files.length; idx++) {
            suite.addTest(new XmlaTest(files[idx]));
        }
        suite.addTestSuite(OtherTest.class);

        return suite;
    }

    /**
     * Non file-based unit tests for XML/A support.
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
