/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import mondrian.xmla.SaxWriter;
import mondrian.xmla.XmlaRequest;
import mondrian.xmla.XmlaResponse;
import mondrian.xmla.XmlaServlet;
import mondrian.xmla.XmlaUtil;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Default implementation of XML/A servlet.
 *
 * @author Gang Chen
 */
public class DefaultXmlaServlet extends XmlaServlet {

    private static final Logger LOGGER = Logger.getLogger(DefaultXmlaServlet.class);
    private static final String EOL = System.getProperty("line.separator", "\n");

    private DocumentBuilderFactory domFactory = null;

    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);

        // init: domFactory
        domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setIgnoringComments(true);
        domFactory.setIgnoringElementContentWhitespace(true);
        domFactory.setNamespaceAware(true);
    }

    protected Element[] unmarshallSoapMessage(InputStream inputStream) throws Exception {
        Element[] hdrBodyElems = new Element[2];

        DocumentBuilder domBuilder = domFactory.newDocumentBuilder();
        Document soapDoc = domBuilder.parse(new InputSource(inputStream));

        /* Check SOAP message */
        Element envElem = soapDoc.getDocumentElement();
        if (!(NS_SOAP.equals(envElem.getNamespaceURI()) &&
                "Envelope".equals(envElem.getLocalName()))) {
            throw new SAXException("Invalid SOAP message");
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuffer buf = new StringBuffer();
            buf.append("XML/A request content").append(EOL);
            buf.append(XmlaUtil.element2Text(envElem));
            LOGGER.debug(buf.toString());
        }

        Element[] childs;

        childs = XmlaUtil.filterChildElements(envElem, NS_SOAP, "Header");
        if (childs.length > 1) {
            throw new SAXException("Invalid SOAP message");
        }
        hdrBodyElems[0] = childs.length == 1 ? childs[0] : null;

        childs = XmlaUtil.filterChildElements(envElem, NS_SOAP, "Body");
        if (childs.length != 1) {
            throw new SAXException("Invalid SOAP message");
        }
        hdrBodyElems[1] = childs[0];
        return hdrBodyElems;
    }

    protected byte[] handleSoapHeader(Element bodyElem, String charEncoding, Map context) throws Exception {
        return null; // no header data to return.
    }

    protected byte[] handleSoapBody(Element hdrElem, Element bodyElem, String charEncoding, Map context) throws Exception {
        Element[] dreqs = XmlaUtil.filterChildElements(bodyElem, NS_XMLA, "Discover");
        Element[] ereqs = XmlaUtil.filterChildElements(bodyElem, NS_XMLA, "Execute");
        if (dreqs.length + ereqs.length != 1) {
            throw new SAXException("Invalid XML/A message");
        }

        Element xmlaReqElem = (dreqs.length == 0 ? ereqs[0] : dreqs[0]);

        ByteArrayOutputStream osBuf = new ByteArrayOutputStream();

        XmlaRequest xmlaReq = new DefaultXmlaRequest(xmlaReqElem, (String) context.get("role"));
        XmlaResponse xmlaRes = new DefaultXmlaResponse(osBuf, charEncoding);

        xmlaHandler.process(xmlaReq, xmlaRes);

        return osBuf.toByteArray();
    }

    protected void marshallSoapMessage(OutputStream outputStream, String encoding, byte[] soapHeader, byte[] soapBody) {
        Object[] byteChunks = new Object[5];

        try {
            StringBuffer buf = new StringBuffer();
            buf.append("<?xml version=\"1.0\" encoding=\"").append(encoding).append("\"?>\r\n");
            buf.append("<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"").append(NS_SOAP).append("\" SOAP-ENV:encodingStyle=\"").append(NS_SOAP_ENCODING_STYLE).append("\">\r\n");
            buf.append("<SOAP-ENV:Header>\r\n");
            byteChunks[0] = buf.toString().getBytes(encoding);

            byteChunks[1] = soapHeader;

            buf.setLength(0);
            buf.append("</SOAP-ENV:Header>\r\n<SOAP-ENV:Body>\r\n");
            byteChunks[2] = buf.toString().getBytes(encoding);

            byteChunks[3] = soapBody;

            buf.setLength(0);
            buf.append("</SOAP-ENV:Body>\r\n</SOAP-ENV:Envelope>\r\n");
            byteChunks[4] = buf.toString().getBytes(encoding);

        } catch (UnsupportedEncodingException uee) {
            LOGGER.warn("This should be handled at begin of processing request", uee);
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuffer buf = new StringBuffer();
            buf.append("XML/A response content").append(EOL);
            try {
                for (int i = 0; i < byteChunks.length; i++) {
                    byte[] chunk = (byte[]) byteChunks[i];
                    if (chunk != null && chunk.length > 0) {
                        buf.append(new String(chunk, encoding));
                    }
                }
            } catch (UnsupportedEncodingException uee) {
                LOGGER.warn("This should be handled at begin of processing request", uee);
            }
            LOGGER.debug(buf.toString());
        }

        try {
            int bufferSize = 4096;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            WritableByteChannel wch = Channels.newChannel(outputStream);
            ReadableByteChannel rch = null;
            for (int i = 0; i < byteChunks.length; i++) {
                if (byteChunks[i] == null || ((byte[]) byteChunks[i]).length == 0)
                    continue;
                rch = Channels.newChannel(new ByteArrayInputStream((byte[]) byteChunks[i]));

                int readSize = 0;

                do {
                    buffer.clear();
                    readSize = rch.read(buffer);
                    buffer.flip();

                    int writeSize = 0;
                    while((writeSize += wch.write(buffer)) < readSize);
                } while (readSize == bufferSize);
                rch.close();
            }
            outputStream.flush();
        } catch(IOException ioe) {
            LOGGER.error("Damn exception when transferring bytes over sockets", ioe);
        }
    }

    protected byte[] handleFault(Throwable t, String charEncoding) {
        t = XmlaUtil.rootThrowable(t);

        ByteArrayOutputStream osBuf = new ByteArrayOutputStream();
        try {
            SaxWriter writer = new DefaultSaxWriter(osBuf, charEncoding);
            writer.startDocument();
            writer.startElement("SOAP-ENV:Fault", new String[]{
                    "xmlns:SOAP-ENV", NS_SOAP,
            });

            writer.startElement("faultcode");
            writer.characters(t.getClass().getName());
            writer.endElement();

            writer.startElement("faultstring");
            writer.characters(t.getMessage());
            writer.endElement();

            writer.startElement("faultactor");
            writer.characters("Mondrian");
            writer.endElement();

            writer.startElement("detail");
            writer.endElement();

            writer.endElement();   // </Fault>
            writer.endDocument();
        } catch (UnsupportedEncodingException uee) {
            LOGGER.warn("This should be handled at begin of processing request", uee);
        } catch (Exception e) {
            LOGGER.error("Unexcepted runimt exception when handing SOAP fault :(");
        }

        return osBuf.toByteArray();
    }

}

// End DefaultXmlaServlet.java
