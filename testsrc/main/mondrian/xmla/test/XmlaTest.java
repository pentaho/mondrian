/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.test;

import mondrian.olap.MondrianProperties;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.CatalogLocatorImpl;
import mondrian.xmla.*;
import mondrian.xmla.impl.DefaultXmlaRequest;
import mondrian.xmla.impl.DefaultXmlaResponse;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;
import org.custommonkey.xmlunit.XMLAssert;
import org.custommonkey.xmlunit.XMLUnit;
import org.eigenbase.xom.*;
import org.w3c.dom.Element;

import javax.servlet.ServletContext;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        testFile = null;
    }

    public XmlaTest(File file) {
        super(file.getName());
        testFile = file;
    }

    protected void runTest() throws Exception {
        Element[] xmlaCyclePair = XmlaTestContext.extractXmlaCycle(testFile, XmlaTestContext.ENV);
        Element requestElem = xmlaCyclePair[0];
        Element expectedResponseElem = xmlaCyclePair[1];
        Element responseElem = executeRequest(requestElem);
        compareElement(expectedResponseElem, responseElem);
    }


    private static Element executeRequest(Element requestElem) {
        ByteArrayOutputStream resBuf = new ByteArrayOutputStream();

        XmlaHandler handler = new XmlaHandler(context.dataSources(), XmlaTestContext.CATALOG_LOCATOR);
        XmlaRequest request = new DefaultXmlaRequest(requestElem, null);
        XmlaResponse response = new DefaultXmlaResponse(resBuf, "UTF-8");
        handler.process(request, response);

        return XmlaUtil.stream2Element(new ByteArrayInputStream(resBuf.toByteArray()));
    }

    private static void compareElement(Element elem1, Element elem2) throws Exception {
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();

        StringWriter bufWriter = new StringWriter();
        transformer.transform(new DOMSource(elem1), new StreamResult(bufWriter));
        String text1 = bufWriter.getBuffer().toString();
        bufWriter = new StringWriter();
        transformer.transform(new DOMSource(elem2), new StreamResult(bufWriter));
        String text2 = bufWriter.getBuffer().toString();
        XMLAssert.assertXMLEqual(text1, text2);
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();

        File[] files = context.retrieveQueryFiles();
        LOGGER.debug("Found " + files.length + " XML/A test files");
        for (int idx = 0; idx < files.length; idx++) {
            suite.addTest(new XmlaTest(files[idx]));
        }

        return suite;
    }

}

// End XmlaTest.java
