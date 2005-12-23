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
import org.custommonkey.xmlunit.XMLAssert;
import org.custommonkey.xmlunit.XMLUnit;
import org.eigenbase.xom.*;
import org.w3c.dom.Element;

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
    private static final String CATALOG_NAME = "FoodMart";
    private static final String DATASOURCE_NAME = "MondrianFoodMart";
    private static final Map ENV = new HashMap() {
        {
            put("catalog", CATALOG_NAME);
            put("datasource", DATASOURCE_NAME);
        }
    };
    private static final DataSourcesConfig.DataSources DATASOURCES;
    private static final CatalogLocator CATALOG_LOCATOR = new CatalogLocatorImpl();
    private static final String[] REQUEST_ELEMENT_NAMES = new String[]{
            "Discover", "Execute"
    };

    static {
        XMLUnit.setControlParser("org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
        XMLUnit.setTestParser("org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
        XMLUnit.setIgnoreWhitespace(true);

        try {
            String connectString = MondrianProperties.instance().TestConnectString.stringValue();
            connectString = connectString.replaceAll("&", "&amp;");

            StringReader dsConfigReader = new StringReader(
                    "<?xml version=\"1.0\"?>" +
                    "<DataSources>" +
                    "   <DataSource>" +
                    "       <DataSourceName>" + DATASOURCE_NAME + "</DataSourceName>" +
                    "       <DataSourceDescription>" + DATASOURCE_NAME + "</DataSourceDescription>" +
                    "       <URL>http://localhost:8080/mondrian/xmla</URL>" +
                    "       <DataSourceInfo>" + connectString + "</DataSourceInfo>" +
                    "       <ProviderName>Mondrian</ProviderName>" +
                    "       <ProviderType>MDP</ProviderType>" +
                    "       <AuthenticationMode>Unauthenticated</AuthenticationMode>" +
                    "   </DataSource>" +
                    "</DataSources>");
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(dsConfigReader);
            DATASOURCES = new DataSourcesConfig.DataSources(def);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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
        Element[] xmlaCyclePair = extractXmlaCycle(testFile, ENV);
        Element requestElem = xmlaCyclePair[0];
        Element expectedResponseElem = xmlaCyclePair[1];
        Element responseElem = executeRequest(requestElem);
        compareElement(expectedResponseElem, responseElem);
    }


    private static Element executeRequest(Element requestElem) {
        ByteArrayOutputStream resBuf = new ByteArrayOutputStream();

        XmlaHandler handler = new XmlaHandler(DATASOURCES, CATALOG_LOCATOR);
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

    private static Element[] extractXmlaCycle(File file, Map env) {
        Element xmlacycleElem = XmlaUtil.text2Element(xmlFromTemplate(file, env));
        for (int i = 0; i < REQUEST_ELEMENT_NAMES.length; i++) {
            String requestElemName = REQUEST_ELEMENT_NAMES[i];
            Element requestElem = XmlaUtil.firstChildElement(xmlacycleElem, null, requestElemName);
            if (requestElem != null) {
                Element responseElem = XmlaUtil.firstChildElement(xmlacycleElem, null, requestElemName + "Response");
                if (responseElem == null) {
                    throw new RuntimeException("Invalid XML/A query file");
                } else {
                    return new Element[]{requestElem, responseElem};
                }
            }
        }
        return null;
    }

    private static String xmlFromTemplate(File file, Map env) {
        StringBuffer buf = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line;
            while ((line = reader.readLine()) != null) {
                buf.append(line).append("\n");
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ignored) {
                }
            }
        }

        String xmlText = buf.toString();
        buf.setLength(0);
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(xmlText);
        while (matcher.find()) {
            String varName = matcher.group(1);
            String varValue = (String) env.get(varName);
            if (varValue != null) {
                matcher.appendReplacement(buf, varValue);
            } else {
                matcher.appendReplacement(buf, "\\${$1}");
            }
        }
        matcher.appendTail(buf);

        return buf.toString();
    }

    private static File[] retrieveQueryFiles() {
        MondrianProperties properties = MondrianProperties.instance();
        String filePattern = properties.QueryFilePattern.get();

        final Pattern pattern = filePattern == null ? null : Pattern.compile(filePattern);

        URL thisPackageUrl = XmlaTest.class.getResource(".");
        File queryFilesDir = new File(thisPackageUrl.getFile(), "queryFiles");
        File[] files = queryFilesDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.endsWith(".xml")) {
                    if (pattern == null) {
                        return true;
                    } else {
                        return pattern.matcher(name).matches();
                    }
                }

                return false;
            }
        });

        return files;
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();

        File[] files = retrieveQueryFiles();
        for (int idx = 0; idx < files.length; idx++) {
            suite.addTest(new XmlaTest(files[idx]));
        }

        return suite;
    }

    /**
     * Retrieve test query files as usable objects.
     * @param env
     * @return new Object[]{fileName:String, request:org.w3c.dom.Element, resposne:org.w3c.dom.Element}
     */
    public Object[] fileRequestResponsePairs(Map env) {
        File[] files = retrieveQueryFiles();

        Object[] pairs = new Object[files.length];
        for (int i = 0; i < pairs.length; i++) {
            Element[] requestAndResponse = extractXmlaCycle(files[i], env);
            pairs[i] = new Object[] {files[i].getName(), requestAndResponse[0], requestAndResponse[1]};
        }

        return pairs;
    }

}

// End XmlaTest.java
