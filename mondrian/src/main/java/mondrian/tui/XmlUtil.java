/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2016 Pentaho Corporation..  All rights reserved.
*/
package mondrian.tui;

import mondrian.olap.Util;
import mondrian.util.XmlParserFactoryProducer;

import org.apache.xerces.dom.DocumentImpl;
import org.apache.xerces.impl.Constants;
import org.apache.xerces.parsers.DOMParser;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;

import org.w3c.dom.*;
import org.xml.sax.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.xpath.*;

/**
 * Some XML parsing, validation and transform utility methods used
 * to valiate XMLA responses.
 *
 * @author Richard M. Emberson
 */
public class XmlUtil {

    public static final String LINE_SEP =
        System.getProperty("line.separator", "\n");

    public static final String SOAP_PREFIX = "SOAP-ENV";
    public static final String XSD_PREFIX = "xsd";

    public static final String XMLNS = "xmlns";

    public static final String NAMESPACES_FEATURE_ID =
        "http://xml.org/sax/features/namespaces";
    public static final String VALIDATION_FEATURE_ID =
        "http://xml.org/sax/features/validation";

    public static final String SCHEMA_VALIDATION_FEATURE_ID =
        "http://apache.org/xml/features/validation/schema";
    public static final String FULL_SCHEMA_VALIDATION_FEATURE_ID =
        "http://apache.org/xml/features/validation/schema-full-checking";

    public static final String DEFER_NODE_EXPANSION =
        "http://apache.org/xml/features/dom/defer-node-expansion";

    public static final String SCHEMA_LOCATION =
        Constants.XERCES_PROPERTY_PREFIX + Constants.SCHEMA_LOCATION;

    /**
     * This is the xslt that can extract the "data" part of a SOAP
     * XMLA response.
     */
    public static final String getSoapXmlaXds2xd(String xmlaPrefix) {
        return
            "<?xml version='1.0'?>" + LINE_SEP
            + "<xsl:stylesheet " + LINE_SEP
            + "xmlns:xsl='http://www.w3.org/1999/XSL/Transform' " + LINE_SEP
            + "xmlns:xalan='http://xml.apache.org/xslt' " + LINE_SEP
            + "xmlns:xsd='http://www.w3.org/2001/XMLSchema' " + LINE_SEP
            + "xmlns:ROW='urn:schemas-microsoft-com:xml-analysis:rowset' "
            + LINE_SEP
            + "xmlns:SOAP-ENV='http://schemas.xmlsoap.org/soap/envelope/' "
            + LINE_SEP
            + "xmlns:xmla='urn:schemas-microsoft-com:xml-analysis' " + LINE_SEP
            + "version='1.0' " + LINE_SEP
            + "> " + LINE_SEP
            + "<xsl:output method='xml'  " + LINE_SEP
            + "encoding='UTF-8' " + LINE_SEP
            + "indent='yes'  " + LINE_SEP
            + "xalan:indent-amount='2'/> " + LINE_SEP
            + "  " + LINE_SEP
            + "<!-- consume '/' and apply --> " + LINE_SEP
            + "<xsl:template match='/'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'Envelope' and apply --> " + LINE_SEP
            + "<xsl:template match='SOAP-ENV:Envelope'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'Header' --> " + LINE_SEP
            + "<xsl:template match='SOAP-ENV:Header'> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'Body' and apply --> " + LINE_SEP
            + "<xsl:template match='SOAP-ENV:Body'> " + LINE_SEP
                + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'DiscoverResponse' and apply --> " + LINE_SEP
            + "<xsl:template match='" + xmlaPrefix + ":DiscoverResponse'> "
            + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'return' and apply --> " + LINE_SEP
            + "<xsl:template match='" + xmlaPrefix + ":return'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'xsd:schema' --> " + LINE_SEP
            + "<xsl:template match='xsd:schema'> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- copy everything else --> " + LINE_SEP
            + "<xsl:template match='*|@*'> " + LINE_SEP
            + "<xsl:copy> " + LINE_SEP
            + "<xsl:apply-templates select='@*|node()'/> " + LINE_SEP
            + "</xsl:copy> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "</xsl:stylesheet>";
    }

    /**
     * This is the xslt that can extract the "schema" part of a SOAP
     * XMLA response.
     */
    public static final String getSoapXmlaXds2xs(String xmlaPrefix) {
        return
            "<?xml version='1.0'?> " + LINE_SEP
            + "<xsl:stylesheet  " + LINE_SEP
            + "xmlns:xsl='http://www.w3.org/1999/XSL/Transform'  " + LINE_SEP
            + "xmlns:xalan='http://xml.apache.org/xslt' " + LINE_SEP
            + "xmlns:xsd='http://www.w3.org/2001/XMLSchema' " + LINE_SEP
            + "xmlns:ROW='urn:schemas-microsoft-com:xml-analysis:rowset' "
            + LINE_SEP
            + "xmlns:SOAP-ENV='http://schemas.xmlsoap.org/soap/envelope/'  "
            + LINE_SEP
            + "xmlns:xmla='urn:schemas-microsoft-com:xml-analysis' " + LINE_SEP
            + "version='1.0' " + LINE_SEP
            + "> " + LINE_SEP
            + "<xsl:output method='xml'  " + LINE_SEP
            + "encoding='UTF-8' " + LINE_SEP
            + "indent='yes'  " + LINE_SEP
            + "xalan:indent-amount='2'/> " + LINE_SEP
            + "<!-- consume '/' and apply --> " + LINE_SEP
            + "<xsl:template match='/'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'Envelope' and apply --> " + LINE_SEP
            + "<xsl:template match='SOAP-ENV:Envelope'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'Header' --> " + LINE_SEP
            + "<xsl:template match='SOAP-ENV:Header'> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'Body' and apply --> " + LINE_SEP
            + "<xsl:template match='SOAP-ENV:Body'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'DiscoverResponse' and apply --> " + LINE_SEP
            + "<xsl:template match='" + xmlaPrefix + ":DiscoverResponse'> "
            + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'return' and apply --> " + LINE_SEP
            + "<xsl:template match='" + xmlaPrefix + ":return'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'root' and apply --> " + LINE_SEP
            + "<xsl:template match='ROW:root'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'row' --> " + LINE_SEP
            + "<xsl:template match='ROW:row'> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- copy everything else --> " + LINE_SEP
            + "<xsl:template match='*|@*'> " + LINE_SEP
            + "<xsl:copy> " + LINE_SEP
            + "<xsl:apply-templates select='@*|node()'/> " + LINE_SEP
            + "</xsl:copy> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "</xsl:stylesheet>";
    }

    /**
     * This is the xslt that can extract the "data" part of a XMLA response.
     */
    public static final String getXmlaXds2xd(String ns) {
        String xmlaPrefix = (ns == null) ? "" : (ns + ":");
        return
            "<?xml version='1.0'?>" + LINE_SEP
            + "<xsl:stylesheet " + LINE_SEP
            + "xmlns:xsl='http://www.w3.org/1999/XSL/Transform' " + LINE_SEP
            + "xmlns:xalan='http://xml.apache.org/xslt' " + LINE_SEP
            + "xmlns:xsd='http://www.w3.org/2001/XMLSchema' " + LINE_SEP
            + "xmlns:ROW='urn:schemas-microsoft-com:xml-analysis:rowset' "
            + LINE_SEP
            + "xmlns:SOAP-ENV='http://schemas.xmlsoap.org/soap/envelope/' "
            + LINE_SEP
            + "xmlns:xmla='urn:schemas-microsoft-com:xml-analysis' " + LINE_SEP
            + "version='1.0' " + LINE_SEP
            + "> " + LINE_SEP
            + "<xsl:output method='xml'  " + LINE_SEP
            + "encoding='UTF-8' " + LINE_SEP
            + "indent='yes'  " + LINE_SEP
            + "xalan:indent-amount='2'/> " + LINE_SEP
            + "  " + LINE_SEP
            + "<!-- consume '/' and apply --> " + LINE_SEP
            + "<xsl:template match='/'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'DiscoverResponse' and apply --> " + LINE_SEP
            + "<xsl:template match='" + xmlaPrefix + "DiscoverResponse'> "
            + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'return' and apply --> " + LINE_SEP
            + "<xsl:template match='" + xmlaPrefix + "return'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'xsd:schema' --> " + LINE_SEP
            + "<xsl:template match='xsd:schema'> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- copy everything else --> " + LINE_SEP
            + "<xsl:template match='*|@*'> " + LINE_SEP
            + "<xsl:copy> " + LINE_SEP
            + "<xsl:apply-templates select='@*|node()'/> " + LINE_SEP
            + "</xsl:copy> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "</xsl:stylesheet>";
    }

    /**
     * This is the xslt that can extract the "schema" part of a XMLA response.
     */
    public static final String getXmlaXds2xs(String ns) {
        String xmlaPrefix = (ns == null) ? "" : (ns + ":");
        return
            "<?xml version='1.0'?> " + LINE_SEP
            + "<xsl:stylesheet  " + LINE_SEP
            + "xmlns:xsl='http://www.w3.org/1999/XSL/Transform'  " + LINE_SEP
            + "xmlns:xalan='http://xml.apache.org/xslt' " + LINE_SEP
            + "xmlns:xsd='http://www.w3.org/2001/XMLSchema' " + LINE_SEP
            + "xmlns:ROW='urn:schemas-microsoft-com:xml-analysis:rowset' "
            + LINE_SEP
            + "xmlns:SOAP-ENV='http://schemas.xmlsoap.org/soap/envelope/'  "
            + LINE_SEP
            + "xmlns:xmla='urn:schemas-microsoft-com:xml-analysis' " + LINE_SEP
            + "version='1.0' " + LINE_SEP
            + "> " + LINE_SEP
            + "<xsl:output method='xml'  " + LINE_SEP
            + "encoding='UTF-8' " + LINE_SEP
            + "indent='yes'  " + LINE_SEP
            + "xalan:indent-amount='2'/> " + LINE_SEP
            + "<!-- consume '/' and apply --> " + LINE_SEP
            + "<xsl:template match='/'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'DiscoverResponse' and apply --> " + LINE_SEP
            + "<xsl:template match='" + xmlaPrefix + "DiscoverResponse'> "
            + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'return' and apply --> " + LINE_SEP
            + "<xsl:template match='" + xmlaPrefix + "return'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'root' and apply --> " + LINE_SEP
            + "<xsl:template match='ROW:root'> " + LINE_SEP
            + "<xsl:apply-templates/> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- consume 'row' --> " + LINE_SEP
            + "<xsl:template match='ROW:row'> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "<!-- copy everything else --> " + LINE_SEP
            + "<xsl:template match='*|@*'> " + LINE_SEP
            + "<xsl:copy> " + LINE_SEP
            + "<xsl:apply-templates select='@*|node()'/> " + LINE_SEP
            + "</xsl:copy> " + LINE_SEP
            + "</xsl:template> " + LINE_SEP
            + "</xsl:stylesheet>";
    }


    /**
     * Error handler plus helper methods.
     */
    public static class SaxErrorHandler implements ErrorHandler {
        public static final String WARNING_STRING        = "WARNING";
        public static final String ERROR_STRING          = "ERROR";
        public static final String FATAL_ERROR_STRING    = "FATAL";

        // DOMError values
        public static final short SEVERITY_WARNING      = 1;
        public static final short SEVERITY_ERROR        = 2;
        public static final short SEVERITY_FATAL_ERROR  = 3;

        public void printErrorInfos(PrintStream out) {
            if (errors != null) {
                for (ErrorInfo error : errors) {
                    out.println(formatErrorInfo(error));
                }
            }
        }

        public static String formatErrorInfos(SaxErrorHandler saxEH) {
            if (! saxEH.hasErrors()) {
                return "";
            }
            StringBuilder buf = new StringBuilder(512);
            for (ErrorInfo error : saxEH.getErrors()) {
                buf.append(formatErrorInfo(error));
                buf.append(LINE_SEP);
            }
            return buf.toString();
        }

        public static String formatErrorInfo(ErrorInfo ei) {
            StringBuilder buf = new StringBuilder(128);
            buf.append("[");
            switch (ei.severity) {
            case SEVERITY_WARNING:
                buf.append(WARNING_STRING);
                break;
            case SEVERITY_ERROR:
                buf.append(ERROR_STRING);
                break;
            case SEVERITY_FATAL_ERROR:
                buf.append(FATAL_ERROR_STRING);
                break;
            }
            buf.append(']');
            String systemId = ei.exception.getSystemId();
            if (systemId != null) {
                int index = systemId.lastIndexOf('/');
                if (index != -1) {
                    systemId = systemId.substring(index + 1);
                }
                buf.append(systemId);
            }
            buf.append(':');
            buf.append(ei.exception.getLineNumber());
            buf.append(':');
            buf.append(ei.exception.getColumnNumber());
            buf.append(": ");
            buf.append(ei.exception.getMessage());
            return buf.toString();
        }

        public static class ErrorInfo {
            public SAXParseException exception;
            public short severity;
            ErrorInfo(short severity, SAXParseException exception) {
                this.severity = severity;
                this.exception = exception;
            }
        }

        private List<ErrorInfo> errors;

        public SaxErrorHandler() {
        }

        public List<ErrorInfo> getErrors() {
            return this.errors;
        }

        public boolean hasErrors() {
            return (this.errors != null);
        }

        public void warning(SAXParseException exception) throws SAXException {
            addError(new ErrorInfo(SEVERITY_WARNING, exception));
        }

        public void error(SAXParseException exception) throws SAXException {
            addError(new ErrorInfo(SEVERITY_ERROR, exception));
        }

        public void fatalError(SAXParseException exception)
            throws SAXException
        {
            addError(new ErrorInfo(SEVERITY_FATAL_ERROR, exception));
        }

        protected void addError(ErrorInfo ei) {
            if (this.errors == null) {
                this.errors = new ArrayList<ErrorInfo>();
            }
            this.errors.add(ei);
        }

        public String getFirstError() {
            return (hasErrors())
                ? formatErrorInfo(errors.get(0))
                : "";
        }
    }

    public static Document newDocument(Node firstElement, boolean deepcopy) {
        Document newDoc = new DocumentImpl();
        newDoc.appendChild(newDoc.importNode(firstElement, deepcopy));
        return newDoc;
    }


    //////////////////////////////////////////////////////////////////////////
    // parse
    //////////////////////////////////////////////////////////////////////////

    /**
     * Get your non-cached DOM parser which can be configured to do schema
     * based validation of the instance Document.
     *
     */
    public static DOMParser getParser(
        String schemaLocationPropertyValue,
        EntityResolver entityResolver,
        boolean validate)
        throws SAXNotRecognizedException, SAXNotSupportedException
    {
        boolean doingValidation =
            (validate || (schemaLocationPropertyValue != null));

        DOMParser parser = new DOMParser();

        parser.setEntityResolver(entityResolver);
        parser.setErrorHandler(new SaxErrorHandler());
        parser.setFeature(DEFER_NODE_EXPANSION, false);
        parser.setFeature(NAMESPACES_FEATURE_ID, true);
        parser.setFeature(SCHEMA_VALIDATION_FEATURE_ID, doingValidation);
        parser.setFeature(VALIDATION_FEATURE_ID, doingValidation);

        if (schemaLocationPropertyValue != null) {
            parser.setProperty(
                SCHEMA_LOCATION,
                schemaLocationPropertyValue.replace('\\', '/'));
        }

        return parser;
    }

    /**
     * See if the DOMParser after parsing a Document has any errors and,
     * if so, throw a RuntimeException exception containing the errors.
     *
     */
    private static void checkForParseError(
        final DOMParser parser,
        Throwable t)
    {
        if (Util.IBM_JVM) {
            // IBM JDK returns lots of errors. Not sure whether they are
            // real errors, but ignore for now.
            return;
        }

        final ErrorHandler errorHandler = parser.getErrorHandler();
        if (errorHandler instanceof SaxErrorHandler) {
            final SaxErrorHandler saxEH = (SaxErrorHandler) errorHandler;
            final List<SaxErrorHandler.ErrorInfo> errors = saxEH.getErrors();

            if (errors != null && errors.size() > 0) {
                String errorStr = SaxErrorHandler.formatErrorInfos(saxEH);
                throw new RuntimeException(errorStr, t);
            }
        } else {
            System.out.println("errorHandler=" + errorHandler);
        }
    }

    private static void checkForParseError(final DOMParser parser) {
        checkForParseError(parser, null);
    }


    /**
     * Parse a String into a Document (no validation).
     *
     */
    public static Document parseString(String s)
        throws SAXException, IOException
    {
        // Hack to workaround bug #622 until 1.4.2_08
        final int length = s.length();

        if (length > 16777216 && length % 4 == 1) {
            final StringBuilder buf = new StringBuilder(length + 1);

            buf.append(s);
            buf.append('\n');
            s = buf.toString();
        }

        return XmlUtil.parse(s.getBytes());
    }

    /**
     * Parse a byte array into a Document (no validation).
     *
     */
    public static Document parse(byte[] bytes)
        throws SAXException, IOException
    {
        return XmlUtil.parse(new ByteArrayInputStream(bytes));
    }

    public static Document parse(File file)
        throws SAXException, IOException
    {
        return parse(new FileInputStream(file));
    }

    /**
     * Parse a stream into a Document (no validation).
     *
     */
    public static Document parse(InputStream in)
        throws SAXException, IOException
    {
        InputSource source = new InputSource(in);

        DOMParser parser = XmlUtil.getParser(null, null, false);
        try {
            parser.parse(source);
            checkForParseError(parser);
        } catch (SAXParseException ex) {
            checkForParseError(parser, ex);
        }

        Document document = parser.getDocument();
        return document;
    }


    //////////////////////////////////////////////////////////////////////////
    // xpath
    //////////////////////////////////////////////////////////////////////////

    public static String makeRootPathInSoapBody() {
        return makeRootPathInSoapBody("xmla", XSD_PREFIX);
    }

    public static String makeRootPathInSoapBody(
        String xmlaPrefix,
        String xsdPrefix)
    {
        StringBuilder buf = new StringBuilder(20);
        buf.append("/").append(xmlaPrefix).append(":DiscoverResponse");
        buf.append("/").append(xmlaPrefix).append(":return");
        buf.append("/ROW:root");
        buf.append('/');
        buf.append('*');
/*
        if (xsdPrefix != null) {
            buf.append(xsdPrefix);
            buf.append(':');
        }
        buf.append("schema");
*/

        return buf.toString();
    }

    public static Node[] selectAsNodes(
        Node node, String xpathExp, Map<String, String> namespaceMap)
        throws XPathException
    {
        XPathFactory fact = XPathFactory.newInstance();
        javax.xml.xpath.XPath xpath  = fact.newXPath();
        xpath.setNamespaceContext(new NamespaceContextImpl(namespaceMap));
        return nodeListToArray(
            (NodeList) xpath.evaluate(xpathExp, node, XPathConstants.NODESET));
    }

    private static Node[] nodeListToArray(NodeList nodeList) {
        Node[] nodes = new Node[nodeList.getLength()];
        for (int i = 0; i < nodeList.getLength(); i++) {
            nodes[i] = nodeList.item(i);
        }
        return nodes;
    }


    /**
     * Convert a Node to a String.
     *
     */
    public static String toString(Node node, boolean prettyPrint) {
        if (node == null) {
            return null;
        }
        try {
            Document doc = node.getOwnerDocument();
            OutputFormat format;
            if (doc != null) {
                format = new OutputFormat(doc, null, prettyPrint);
            } else {
                format = new OutputFormat("xml", null, prettyPrint);
                format.setLineWidth(0); // don't wrap lines
            }
            if (prettyPrint) {
                format.setLineSeparator(LINE_SEP);
            } else {
                format.setLineSeparator("");
            }
            StringWriter writer = new StringWriter(1000);
            XMLSerializer serial = new XMLSerializer(writer, format);
            serial.asDOMSerializer();
            if (node instanceof Document) {
                serial.serialize((Document) node);
            } else if (node instanceof Element) {
                format.setOmitXMLDeclaration(true);
                serial.serialize((Element) node);
            } else if (node instanceof DocumentFragment) {
                format.setOmitXMLDeclaration(true);
                serial.serialize((DocumentFragment) node);
            } else if (node instanceof Text) {
                Text text = (Text) node;
                return text.getData();
            } else if (node instanceof Attr) {
                Attr attr = (Attr) node;
                String name = attr.getName();
                String value = attr.getValue();
                writer.write(name);
                writer.write("=\"");
                writer.write(value);
                writer.write("\"");
                if (prettyPrint) {
                    writer.write(LINE_SEP);
                }
            } else {
                writer.write("node class = " + node.getClass().getName());
                if (prettyPrint) {
                    writer.write(LINE_SEP);
                } else {
                    writer.write(' ');
                }
                writer.write("XmlUtil.toString: fix me: ");
                writer.write(node.toString());
                if (prettyPrint) {
                    writer.write(LINE_SEP);
                }
            }
            return writer.toString();
        } catch (Exception ex) {
            // ignore
            return null;
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // validate
    //////////////////////////////////////////////////////////////////////////

    /**
     * This can be extened to have a map from publicId/systemId
     * to InputSource.
     */
    public static class Resolver implements EntityResolver {
        private InputSource source;

        protected Resolver() {
            this((InputSource) null);
        }
        public Resolver(Document doc) {
            this(XmlUtil.toString(doc, false));
        }
        public Resolver(String str) {
            this(new InputSource(new StringReader(str)));
        }
        public Resolver(InputSource source) {
            this.source = source;
        }
        public InputSource resolveEntity(
            String publicId,
            String systemId)
            throws SAXException, IOException
        {
            return source;
        }
    }

    /**
     * Get the Xerces version being used.
     *
     * @return Xerces version being used
     */
    public static String getXercesVersion() {
        try {
            return org.apache.xerces.impl.Version.getVersion();
        } catch (java.lang.NoClassDefFoundError ex) {
            return "Xerces-J 2.2.0";
        }
    }

    /**
     * Get the number part of the Xerces Version string.
     *
     * @return number part of the Xerces Version string
     */
    public static String getXercesVersionNumberString() {
        String version = getXercesVersion();
        int index = version.indexOf(' ');
        return (index == -1)
            ? "0.0.0" : version.substring(index + 1);
    }

    private static int[] versionNumbers = null;

    /**
     * Gets the Xerces version numbers as a three part array of ints where
     * the first element is the major release number, the second is the
     * minor release number, and the third is the patch number.
     *
     * @return Xerces version number as int array
     */
    public static synchronized int[] getXercesVersionNumbers() {
        if (versionNumbers == null) {
            int[] verNums = new int[3];
            String verNumStr = getXercesVersionNumberString();
            int index = verNumStr.indexOf('.');
            verNums[0] = Integer.parseInt(verNumStr.substring(0, index));

            verNumStr = verNumStr.substring(index + 1);
            index = verNumStr.indexOf('.');
            verNums[1] = Integer.parseInt(verNumStr.substring(0, index));

            verNumStr = verNumStr.substring(index + 1);
            verNums[2] = Integer.parseInt(verNumStr);

            versionNumbers = verNums;
        }

        return versionNumbers;
    }

    /**
     * I could not get Xerces 2.2 to validate. So, I hard coded allowing
     * Xerces 2.6 and above to validate and by setting the following
     * System property one can test validating with earlier versions
     * of Xerces.
     */
    private static final String ALWAYS_ATTEMPT_VALIDATION =
        "mondrian.xml.always.attempt.validation";

    private static final boolean alwaysAttemptValidation;
    static {
        alwaysAttemptValidation =
            Boolean.getBoolean(ALWAYS_ATTEMPT_VALIDATION);
    }

    /**
     * Returns whether the XML parser supports validation.
     *
     * <p>I could not get validation to work with Xerces 2.2 so I put in
     * this check. If you want to test on an earlier version of Xerces
     * simply define the above property:
     *   "mondrian.xml.always.attempt.validation",
     * to true.
     *
     * @return whether the XML parser supports validation
     */
    public static boolean supportsValidation() {
        if (alwaysAttemptValidation) {
            return true;
        } else {
            int[] verNums = getXercesVersionNumbers();
            return (verNums[0] >= 3)
                || ((verNums[0] == 2) && (verNums[1] >= 6));
        }
    }

    public static void validate(
        Document doc,
        String schemaLocationPropertyValue,
        EntityResolver resolver)
        throws IOException,
        SAXException
    {
        OutputFormat format  = new OutputFormat(doc, null, true);
        StringWriter writer = new StringWriter(1000);
        XMLSerializer serial = new XMLSerializer(writer, format);
        serial.asDOMSerializer();
        serial.serialize(doc);
        String docString = writer.toString();

        validate(docString, schemaLocationPropertyValue, resolver);
    }

    public static void validate(
        String docStr,
        String schemaLocationPropertyValue,
        EntityResolver resolver)
        throws IOException,
        SAXException
    {
        if (resolver == null) {
            resolver = new Resolver();
        }
        DOMParser parser =
            getParser(schemaLocationPropertyValue, resolver, true);

        try {
            parser.parse(new InputSource(new StringReader(docStr)));
            checkForParseError(parser);
        } catch (SAXParseException ex) {
            checkForParseError(parser, ex);
        }
    }

    /**
     * This is used to get a Document's namespace attribute value.
     *
     */
    public static String getNamespaceAttributeValue(Document doc) {
        Element el = doc.getDocumentElement();
        String prefix = el.getPrefix();
        Attr attr = (prefix == null)
                    ? el.getAttributeNode(XMLNS)
                    : el.getAttributeNode(XMLNS + ':' + prefix);
        return (attr == null) ? null : attr.getValue();
    }

    //////////////////////////////////////////////////////////////////////////
    // transform
    //////////////////////////////////////////////////////////////////////////

    private static TransformerFactory tfactory = null;

    public static TransformerFactory getTransformerFactory()
        throws TransformerFactoryConfigurationError
    {
        if (tfactory == null) {
            tfactory = TransformerFactory.newInstance();
        }
        return tfactory;
    }

    /**
     * Transform a Document and return the transformed Node.
     *
     */
    public static Node transform(
        Document inDoc,
        String xslFileName,
        String[][] namevalueParameters)
        throws ParserConfigurationException,
               SAXException,
               IOException,
               TransformerConfigurationException,
               TransformerException
    {
        DocumentBuilderFactory dfactory =
            XmlParserFactoryProducer.createSecureDocBuilderFactory();
        dfactory.setNamespaceAware(true);

        DocumentBuilder docBuilder = dfactory.newDocumentBuilder();
        Node xslDOM = docBuilder.parse(new InputSource(xslFileName));

        TransformerFactory tfactory = getTransformerFactory();
        Templates stylesheet = tfactory.newTemplates(
            new DOMSource(xslDOM, xslFileName));
        Transformer transformer = stylesheet.newTransformer();
        if (namevalueParameters != null) {
            for (int i = 0; i < namevalueParameters.length; i++) {
                String name = namevalueParameters[i][0];
                String value = namevalueParameters[i][1];

                transformer.setParameter(name, value);
            }
        }
        DOMResult domResult = new DOMResult();
        transformer.transform(new DOMSource(inDoc, null), domResult);

        return domResult.getNode();
    }

    public static Node transform(
        Document inDoc,
        String xslFileName)
        throws ParserConfigurationException,
               SAXException,
               IOException,
               TransformerConfigurationException,
               TransformerException
    {
        return transform(inDoc, xslFileName, null);
    }

    /**
     * Transform a Document and return the transformed Node.
     *
     */
    public static Node transform(
        Document inDoc,
        Reader xslReader,
        String[][] namevalueParameters)
        throws ParserConfigurationException,
               SAXException,
               IOException,
               TransformerConfigurationException,
               TransformerException
    {
        DocumentBuilderFactory dfactory =
            XmlParserFactoryProducer.createSecureDocBuilderFactory();
        dfactory.setNamespaceAware(true);

        DocumentBuilder docBuilder = dfactory.newDocumentBuilder();
        Node xslDOM = docBuilder.parse(new InputSource(xslReader));

        TransformerFactory tfactory = getTransformerFactory();
        Templates stylesheet = tfactory.newTemplates(new DOMSource(xslDOM));
        Transformer transformer = stylesheet.newTransformer();
        if (namevalueParameters != null) {
            for (int i = 0; i < namevalueParameters.length; i++) {
                String name = namevalueParameters[i][0];
                String value = namevalueParameters[i][1];

                transformer.setParameter(name, value);
            }
        }

        DOMResult domResult = new DOMResult();
        transformer.transform(new DOMSource(inDoc, null), domResult);

        return domResult.getNode();
    }

    public static Node transform(
        Document inDoc,
        Reader xslReader)
        throws ParserConfigurationException,
               SAXException,
               IOException,
               TransformerConfigurationException,
               TransformerException
    {
        return transform(inDoc, xslReader, null);
    }


    private XmlUtil() {
    }
}

// End XmlUtil.java
