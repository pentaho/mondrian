/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.impl;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import mondrian.xmla.SaxWriter;
import mondrian.xmla.XmlaRequest;
import mondrian.xmla.XmlaResponse;
import mondrian.xmla.XmlaServlet;
import mondrian.xmla.XmlaUtil;
import mondrian.olap.Util;
import mondrian.olap.Role;
import mondrian.xmla.XmlaRequestCallback;
import mondrian.xmla.XmlaException;

import org.apache.log4j.Logger;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Default implementation of XML/A servlet.
 *
 * @author Gang Chen
 */
public class DefaultXmlaServlet extends XmlaServlet {

    private static final Logger LOGGER =
        Logger.getLogger(DefaultXmlaServlet.class);
    protected static final String nl = Util.nl;

    private DocumentBuilderFactory domFactory = null;

    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
        domFactory = getDocumentBuilderFactory();
    }

    protected DocumentBuilderFactory getDocumentBuilderFactory() {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setIgnoringComments(true);
        factory.setIgnoringElementContentWhitespace(true);
        factory.setNamespaceAware(true);
        return factory;
    }

    protected void unmarshallSoapMessage(
        HttpServletRequest request,
        Element[] requestSoapParts)
        throws XmlaException
    {
        try {
            InputStream inputStream;
            try {
                inputStream = request.getInputStream();
            } catch (IllegalStateException ex) {
                throw new XmlaException(
                    SERVER_FAULT_FC,
                    USM_REQUEST_STATE_CODE,
                    USM_REQUEST_STATE_FAULT_FS,
                    ex);
            } catch (IOException ex) {
                // This is either Client or Server
                throw new XmlaException(
                    SERVER_FAULT_FC,
                    USM_REQUEST_INPUT_CODE,
                    USM_REQUEST_INPUT_FAULT_FS,
                    ex);
            }

            DocumentBuilder domBuilder;
            try {
                domBuilder = domFactory.newDocumentBuilder();
            } catch (ParserConfigurationException ex) {
                throw new XmlaException(
                    SERVER_FAULT_FC,
                    USM_DOM_FACTORY_CODE,
                    USM_DOM_FACTORY_FAULT_FS,
                    ex);
            }

            Document soapDoc;
            try {
                soapDoc = domBuilder.parse(new InputSource(inputStream));
            } catch (IOException ex) {
                // This is either Client or Server
                throw new XmlaException(
                    SERVER_FAULT_FC,
                    USM_DOM_PARSE_IO_CODE,
                    USM_DOM_PARSE_IO_FAULT_FS,
                    ex);
            } catch (SAXException ex) {
                // Assume client passed bad xml
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE,
                    USM_DOM_PARSE_FAULT_FS,
                    ex);
            }

            /* Check SOAP message */
            Element envElem = soapDoc.getDocumentElement();

            if (LOGGER.isDebugEnabled()) {
                final StringWriter writer = new StringWriter();
                writer.write("XML/A request content");
                writer.write(nl);
                XmlaUtil.element2Text(envElem, writer);
                LOGGER.debug(writer.toString());
            }

            if ("Envelope".equals(envElem.getLocalName())) {
                if (!(NS_SOAP_ENV_1_1.equals(envElem.getNamespaceURI()))) {
                    throw new XmlaException(
                        CLIENT_FAULT_FC,
                        USM_DOM_PARSE_CODE,
                        USM_DOM_PARSE_FAULT_FS,
                        new SAXException(
                            "Invalid SOAP message: "
                            + "Envelope element not in SOAP namespace"));
                }
            } else {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE,
                    USM_DOM_PARSE_FAULT_FS,
                    new SAXException(
                        "Invalid SOAP message: "
                        + "Top element not Envelope"));
            }

            Element[] childs =
                XmlaUtil.filterChildElements(envElem, NS_SOAP_ENV_1_1, "Header");
            if (childs.length > 1) {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE,
                    USM_DOM_PARSE_FAULT_FS,
                    new SAXException(
                        "Invalid SOAP message: "
                        + "More than one Header elements"));
            }
            requestSoapParts[0] = childs.length == 1 ? childs[0] : null;

            childs =
                XmlaUtil.filterChildElements(envElem, NS_SOAP_ENV_1_1, "Body");
            if (childs.length != 1) {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    USM_DOM_PARSE_CODE,
                    USM_DOM_PARSE_FAULT_FS,
                    new SAXException(
                        "Invalid SOAP message: "
                        + "Does not have one Body element"));
            }
            requestSoapParts[1] = childs[0];
        } catch (XmlaException xex) {
            throw xex;
        } catch (Exception ex) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                USM_UNKNOWN_CODE,
                USM_UNKNOWN_FAULT_FS,
                ex);
        }
    }

    /**
     * See if there is a "mustUnderstand" header element.
     * If there is a BeginSession element, then generate a session id and
     * add to context Map.
     * <p>
     * Excel 2000 and Excel XP generate both a BeginSession, Session and
     * EndSession mustUnderstand==1
     * in the "urn:schemas-microsoft-com:xml-analysis" namespace
     * Header elements and a NamespaceCompatibility mustUnderstand==0
     * in the "http://schemas.microsoft.com/analysisservices/2003/xmla"
     * namespace. Here we handle only the session Header elements
     *
     */
    protected void handleSoapHeader(
        HttpServletResponse response,
        Element[] requestSoapParts,
        byte[][] responseSoapParts,
        Map<String, Object> context) throws XmlaException
    {
        try {
            Element hdrElem = requestSoapParts[0];
            if ((hdrElem == null) || (! hdrElem.hasChildNodes())) {
                return;
            }
            String encoding = response.getCharacterEncoding();

            byte[] bytes = null;

            NodeList nlst = hdrElem.getChildNodes();
            int nlen = nlst.getLength();
            for (int i = 0; i < nlen; i++) {
                Node n = nlst.item(i);
                if (n instanceof Element) {
                    Element e = (Element) n;

                    // does the Element have a mustUnderstand attribute
                    Attr attr = e.getAttributeNode(SOAP_MUST_UNDERSTAND_ATTR);
                    if (attr == null) {
                        continue;
                    }
                    // Is its value "1"
                    String mustUnderstandValue = attr.getValue();
                    if ((mustUnderstandValue == null)
                        || (!mustUnderstandValue.equals("1")))
                    {
                        continue;
                    }

                    // We've got a mustUnderstand attribute

                    // Is it an XMLA element
                    if (! NS_XMLA.equals(e.getNamespaceURI())) {
                        continue;
                    }
                    // So, an XMLA mustUnderstand-er
                    // Do we know what to do with it
                    // We understand:
                    //    BeginSession
                    //    Session
                    //    EndSession

                    String sessionIdStr;
                    String localName = e.getLocalName();
                    if (localName.equals(XMLA_BEGIN_SESSION)) {
                        // generate SessionId

                        sessionIdStr = generateSessionId(context);

                        context.put(CONTEXT_XMLA_SESSION_ID, sessionIdStr);
                        context.put(
                            CONTEXT_XMLA_SESSION_STATE,
                            CONTEXT_XMLA_SESSION_STATE_BEGIN);

                    } else if (localName.equals(XMLA_SESSION)) {
                        // extract the SessionId attrs value and put into
                        // context
                        sessionIdStr = getSessionId(e, context);

                        context.put(CONTEXT_XMLA_SESSION_ID, sessionIdStr);
                        context.put(
                            CONTEXT_XMLA_SESSION_STATE,
                            CONTEXT_XMLA_SESSION_STATE_WITHIN);

                    } else if (localName.equals(XMLA_END_SESSION)) {
                        // extract the SessionId attrs value and put into
                        // context
                        sessionIdStr = getSessionId(e, context);

                        context.put(CONTEXT_XMLA_SESSION_ID, sessionIdStr);
                        context.put(
                            CONTEXT_XMLA_SESSION_STATE,
                            CONTEXT_XMLA_SESSION_STATE_END);

                    } else {
                        // error
                        String msg =
                            "Invalid XML/A message: Unknown "
                            + "\"mustUnderstand\" XMLA Header element \""
                            + localName
                            + "\"";
                        throw new XmlaException(
                            MUST_UNDERSTAND_FAULT_FC,
                            HSH_MUST_UNDERSTAND_CODE,
                            HSH_MUST_UNDERSTAND_FAULT_FS,
                            new RuntimeException(msg));
                    }

                    StringBuilder buf = new StringBuilder(100);
                    buf.append("<Session ");
                    buf.append(XMLA_SESSION_ID);
                    buf.append("=\"");
                    buf.append(sessionIdStr);
                    buf.append("\" ");
                    buf.append("xmlns=\"");
                    buf.append(NS_XMLA);
                    buf.append("\" />");
                    bytes = buf.toString().getBytes(encoding);
                }
            }
            responseSoapParts[0] = bytes;
        } catch (XmlaException xex) {
            throw xex;
        } catch (Exception ex) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSH_UNKNOWN_CODE,
                HSH_UNKNOWN_FAULT_FS,
                ex);
        }
    }

    protected String generateSessionId(Map<String, Object> context) {
        List<XmlaRequestCallback> callbacks = getCallbacks();
        if (callbacks.size() > 0) {
            // get only the first callback if it exists
            XmlaRequestCallback callback = callbacks.get(0);
            return (String) callback.generateSessionId(context);
        } else {
            // what to do here, should Mondrian generate a Session Id?
            // TODO: Maybe Mondrian ought to generate all Session Ids and
            // not the callback.
            return "";
        }
    }

    protected String getSessionId(Element e, Map<String, Object> context)
        throws Exception
    {
        // extract the SessionId attrs value and put into context
        Attr attr = e.getAttributeNode(XMLA_SESSION_ID);
        if (attr == null) {
            throw new SAXException(
                "Invalid XML/A message: "
                + XMLA_SESSION
                + " Header element with no "
                + XMLA_SESSION_ID
                + " attribute");
        }
        String value = attr.getValue();
        if (value == null) {
            throw new SAXException(
                "Invalid XML/A message: "
                + XMLA_SESSION
                + " Header element with "
                + XMLA_SESSION_ID
                + " attribute but no attribute value");
        }
        return value;
    }

    protected void handleSoapBody(
        HttpServletResponse response,
        Element[] requestSoapParts,
        byte[][] responseSoapParts,
        Map<String, Object> context)
        throws XmlaException
    {
        try {
            String encoding = response.getCharacterEncoding();
            Element hdrElem = requestSoapParts[0];
            Element bodyElem = requestSoapParts[1];
            Element[] dreqs =
                XmlaUtil.filterChildElements(bodyElem, NS_XMLA, "Discover");
            Element[] ereqs =
                XmlaUtil.filterChildElements(bodyElem, NS_XMLA, "Execute");
            if (dreqs.length + ereqs.length != 1) {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_BAD_SOAP_BODY_CODE,
                    HSB_BAD_SOAP_BODY_FAULT_FS,
                    new RuntimeException(
                        "Invalid XML/A message: Body has "
                        + dreqs.length + " Discover Requests and "
                        + ereqs.length + " Execute Requests"));
            }

            Element xmlaReqElem = (dreqs.length == 0 ? ereqs[0] : dreqs[0]);

            ByteArrayOutputStream osBuf = new ByteArrayOutputStream();

            // use context variable `role' as this request's XML/A role
            String roleName = (String) context.get(CONTEXT_ROLE_NAME);
            Role role = (Role) context.get(CONTEXT_ROLE);

            XmlaRequest xmlaReq = null;
            if (role != null) {
                xmlaReq = new DefaultXmlaRequest(xmlaReqElem, role);
            } else if (roleName != null) {
                xmlaReq = new DefaultXmlaRequest(xmlaReqElem, roleName);
            } else {
                xmlaReq = new DefaultXmlaRequest(xmlaReqElem);
            }

            XmlaResponse xmlaRes = new DefaultXmlaResponse(osBuf, encoding);

            try {
                getXmlaHandler().process(xmlaReq, xmlaRes);
            } catch (XmlaException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new XmlaException(
                    SERVER_FAULT_FC,
                    HSB_PROCESS_CODE,
                    HSB_PROCESS_FAULT_FS,
                    ex);
            }

            responseSoapParts[1] = osBuf.toByteArray();
        } catch (XmlaException xex) {
            throw xex;
        } catch (Exception ex) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_UNKNOWN_CODE,
                HSB_UNKNOWN_FAULT_FS,
                ex);
        }
    }

    protected void marshallSoapMessage(
        HttpServletResponse response,
        byte[][] responseSoapParts)
        throws XmlaException
    {
        try {
            // If CharacterEncoding was set in web.xml, use this value
            String encoding = (charEncoding != null)
                    ? charEncoding : response.getCharacterEncoding();

            /*
             * Since we just reset response, encoding and content-type were
             * reset too
             */
            if (charEncoding != null) {
                response.setCharacterEncoding(charEncoding);
            }
            response.setContentType("text/xml");

            /*
             * The setCharacterEncoding, setContentType, or setLocale method
             * must be called BEFORE getWriter or getOutputStream and before
             * committing the response for the character encoding to be used.
             *
             * @see javax.servlet.ServletResponse
             */
            OutputStream outputStream = response.getOutputStream();


            byte[] soapHeader = responseSoapParts[0];
            byte[] soapBody = responseSoapParts[1];

            Object[] byteChunks = new Object[5];

            try {
                StringBuilder buf = new StringBuilder(500);
                buf.append("<?xml version=\"1.0\" encoding=\"");
                buf.append(encoding);
                buf.append("\"?>");
                buf.append(nl);

                buf.append("<");
                buf.append(SOAP_PREFIX);
                buf.append(":Envelope xmlns:");
                buf.append(SOAP_PREFIX);
                buf.append("=\"");
                buf.append(NS_SOAP_ENV_1_1);
                buf.append("\" ");
                buf.append(SOAP_PREFIX);
                buf.append(":encodingStyle=\"");
                buf.append(NS_SOAP_ENC_1_1);
                buf.append("\" >");
                buf.append(nl);
                buf.append("<");
                buf.append(SOAP_PREFIX);
                buf.append(":Header>");
                buf.append(nl);
                byteChunks[0] = buf.toString().getBytes(encoding);

                byteChunks[1] = soapHeader;

                buf.setLength(0);
                buf.append("</");
                buf.append(SOAP_PREFIX);
                buf.append(":Header>");
                buf.append(nl);
                buf.append("<");
                buf.append(SOAP_PREFIX);
                buf.append(":Body>");
                buf.append(nl);

                byteChunks[2] = buf.toString().getBytes(encoding);

                byteChunks[3] = soapBody;

                buf.setLength(0);
                buf.append(nl);
                buf.append("</");
                buf.append(SOAP_PREFIX);
                buf.append(":Body>");
                buf.append(nl);
                buf.append("</");
                buf.append(SOAP_PREFIX);
                buf.append(":Envelope>");
                buf.append(nl);

                byteChunks[4] = buf.toString().getBytes(encoding);
            } catch (UnsupportedEncodingException uee) {
                LOGGER.warn(
                    "This should be handled at begin of processing request",
                    uee);
            }

            if (LOGGER.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder(100);
                buf.append("XML/A response content").append(nl);
                try {
                    for (Object byteChunk : byteChunks) {
                        byte[] chunk = (byte[]) byteChunk;
                        if (chunk != null && chunk.length > 0) {
                            buf.append(new String(chunk, encoding));
                        }
                    }
                } catch (UnsupportedEncodingException uee) {
                    LOGGER.warn(
                        "This should be handled at begin of processing request",
                        uee);
                }
                LOGGER.debug(buf.toString());
            }

            if (LOGGER.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder();
                buf.append("XML/A response content").append(nl);
            }
            try {
                int bufferSize = 4096;
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                WritableByteChannel wch = Channels.newChannel(outputStream);
                ReadableByteChannel rch;
                for (Object byteChunk : byteChunks) {
                    if (byteChunk == null || ((byte[]) byteChunk).length == 0) {
                        continue;
                    }
                    rch = Channels.newChannel(
                        new ByteArrayInputStream((byte[]) byteChunk));

                    int readSize;
                    do {
                        buffer.clear();
                        readSize = rch.read(buffer);
                        buffer.flip();

                        int writeSize = 0;
                        while ((writeSize += wch.write(buffer)) < readSize) {
                            ;
                        }
                    } while (readSize == bufferSize);
                    rch.close();
                }
                outputStream.flush();
            } catch (IOException ioe) {
                LOGGER.error(
                    "Damn exception when transferring bytes over sockets",
                    ioe);
            }
        } catch (XmlaException xex) {
            throw xex;
        } catch (Exception ex) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                MSM_UNKNOWN_CODE,
                MSM_UNKNOWN_FAULT_FS,
                ex);
        }
    }

    /**
     * This produces a SOAP 1.1 version Fault element - not a 1.2 version.
     *
     */
    protected void handleFault(
        HttpServletResponse response,
        byte[][] responseSoapParts,
        Phase phase,
        Throwable t)
    {
        // Regardless of whats been put into the response so far, clear
        // it out.
        response.reset();

        // NOTE: if you can think of better/other status codes to use
        // for the various phases, please make changes.
        // I think that XMLA faults always returns OK.
        switch (phase) {
        case VALIDATE_HTTP_HEAD:
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            break;
        case INITIAL_PARSE:
        case CALLBACK_PRE_ACTION:
        case PROCESS_HEADER:
        case PROCESS_BODY:
        case CALLBACK_POST_ACTION:
        case SEND_RESPONSE:
            response.setStatus(HttpServletResponse.SC_OK);
            break;
        }

        String code;
        String faultCode;
        String faultString;
        String detail;
        if (t instanceof XmlaException) {
            XmlaException xex = (XmlaException) t;
            code = xex.getCode();
            faultString = xex.getFaultString();
            faultCode = XmlaException.formatFaultCode(xex);
            detail = XmlaException.formatDetail(xex.getDetail());

        } else {
            // some unexpected Throwable
            t = XmlaException.getRootCause(t);
            code = UNKNOWN_ERROR_CODE;
            faultString = UNKNOWN_ERROR_FAULT_FS;
            faultCode = XmlaException.formatFaultCode(
                            SERVER_FAULT_FC, code);
            detail = XmlaException.formatDetail(t.getMessage());
        }

        String encoding = response.getCharacterEncoding();

        ByteArrayOutputStream osBuf = new ByteArrayOutputStream();
        try {
            SaxWriter writer = new DefaultSaxWriter(osBuf, encoding);
            writer.startDocument();
            writer.startElement(SOAP_PREFIX + ":Fault");

            // The faultcode element is intended for use by software to provide
            // an algorithmic mechanism for identifying the fault. The faultcode
            // MUST be present in a SOAP Fault element and the faultcode value
            // MUST be a qualified name
            writer.startElement("faultcode");
            writer.characters(faultCode);
            writer.endElement();

            // The faultstring element is intended to provide a human readable
            // explanation of the fault and is not intended for algorithmic
            // processing.
            writer.startElement("faultstring");
            writer.characters(faultString);
            writer.endElement();

            // The faultactor element is intended to provide information about
            // who caused the fault to happen within the message path
            writer.startElement("faultactor");
            writer.characters(FAULT_ACTOR);
            writer.endElement();

            // The detail element is intended for carrying application specific
            // error information related to the Body element. It MUST be present
            // if the contents of the Body element could not be successfully
            // processed. It MUST NOT be used to carry information about error
            // information belonging to header entries. Detailed error
            // information belonging to header entries MUST be carried within
            // header entries.
            if (phase != Phase.PROCESS_HEADER) {
                writer.startElement("detail");
                writer.startElement(
                    FAULT_NS_PREFIX + ":error",
                    "xmlns:" + FAULT_NS_PREFIX, MONDRIAN_NAMESPACE);
                writer.startElement("code");
                writer.characters(code);
                writer.endElement(); // code
                writer.startElement("desc");
                writer.characters(detail);
                writer.endElement(); // desc
                writer.endElement(); // error
                writer.endElement(); // detail
            }

            writer.endElement();   // </Fault>
            writer.endDocument();
        } catch (UnsupportedEncodingException uee) {
            LOGGER.warn(
                "This should be handled at begin of processing request",
                uee);
        } catch (Exception e) {
            LOGGER.error(
                "Unexcepted runimt exception when handing SOAP fault :(");
        }

        responseSoapParts[1] = osBuf.toByteArray();
    }

}

// End DefaultXmlaServlet.java
