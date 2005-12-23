/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.ServletContextCatalogLocator;

import org.apache.log4j.Logger;
import org.eigenbase.xom.*;
import org.w3c.dom.Element;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Base XML/A servlet.
 *
 * @author Gang Chen
 * @since December, 2005
 * @version $Id$
 */
public abstract class XmlaServlet extends HttpServlet implements XmlaConstants {

    private static final Logger LOGGER = Logger.getLogger(XmlaServlet.class);

    private static final String PARAM_DATASOURCES_CONFIG = "DataSourcesConfig";
    private static final String PARAM_CHAR_ENCODING = "CharacterEncoding";
    private static final String PARAM_CALLBACKS = "Callbacks";

    protected XmlaHandler xmlaHandler = null;
    private String charEncoding = null;
    private CatalogLocator catalogLocator = null;
    private URL dataSourcesConfigUrl = null;
    private final List callbackList = new ArrayList();

    /**
     * Initializes servlet and XML/A handler.
     *
     * @param servletConfig
     * @throws ServletException
     */
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
        ServletContext servletContext = servletConfig.getServletContext();

        String paramValue = null;

        // init: charEncoding
        paramValue = servletConfig.getInitParameter(PARAM_DATASOURCES_CONFIG);
        if (paramValue == null) paramValue = "datasources.xml"; // fallback to default
        try {
            try {
                dataSourcesConfigUrl = new URL(paramValue);
            } catch (MalformedURLException mue) {
                paramValue = "WEB-INF/" + paramValue;
                LOGGER.warn("Use default datasources config file '" + paramValue + "' in web context direcotry");
                dataSourcesConfigUrl = new File(servletContext.getRealPath(paramValue)).toURL();
            }
        } catch (MalformedURLException mue) {
            throw Util.newError(mue, "invalid URL path '" + paramValue + "'");
        }

        // init: charEncoding
        paramValue = servletConfig.getInitParameter(PARAM_CHAR_ENCODING);
        if (paramValue != null) {
            charEncoding = paramValue;
        } else {
            charEncoding = null;
            LOGGER.warn("Use default character encoding from HTTP client");
        }

        // init: catalogLocator
        catalogLocator = new ServletContextCatalogLocator(servletContext);

        // init: xmlaHandler
        initXmlaHandler();

        // init: callbacks
        initCallbacks(servletConfig);
    }

    /**
     * Registers a callback.
     */
    protected final void addCallback(XmlaRequestCallback callback) {
        callbackList.add(callback);
    }

    /**
     * Main entry for HTTP post method
     *
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {
        if (charEncoding != null) {
            try {
                request.setCharacterEncoding(charEncoding);
                response.setCharacterEncoding(charEncoding);
            } catch (UnsupportedEncodingException uee) {
                charEncoding = null;
                LOGGER.warn("Unsupported character encoding '" + charEncoding + "'");
                LOGGER.warn("Use default character encoding from HTTP client from now");
            }
        }

        response.setContentType("text/xml");

        OutputStream outputStream = response.getOutputStream();
        String soapResCharEncoding = response.getCharacterEncoding();
        byte[] soapResHeader = null;
        byte[] soapResBody = null;
        Element hdrElem = null;
        Element bodyElem = null;

        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unmarshalling SOAP message");
            }

            // check request's content type
            String contentType = request.getContentType();
            if (contentType == null || contentType.indexOf("text/xml") == -1) {
                throw new IllegalArgumentException("Only accepts content type 'text/xml'");
            }

            Element[] soapParts = unmarshallSoapMessage(request.getInputStream());
            hdrElem = soapParts[0];
            bodyElem = soapParts[1];
        } catch (Throwable t) {
            LOGGER.error("Unable to unmarshall SOAP message", t);
            soapResBody = handleFault(t, soapResCharEncoding);
            marshallSoapMessage(outputStream, soapResCharEncoding, soapResHeader, soapResBody);
            return;
        }

        Map context = new HashMap(); // context binding for application's extensibility
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Invoking callbacks");
            }
            for (Iterator it = callbackList.iterator(); it.hasNext();) {
                XmlaRequestCallback callback = (XmlaRequestCallback) it.next();
                callback.invoke(context, request, hdrElem, bodyElem);
            }
        } catch (Throwable t) {
            LOGGER.error("Errors when invoking callbacks", t);
            soapResBody = handleFault(t, soapResCharEncoding);
            marshallSoapMessage(outputStream, soapResCharEncoding, soapResHeader, soapResBody);
            return;
        }
        // freeze context binding
        context = Collections.unmodifiableMap(context);

        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Handling XML/A message");
            }

            // process application specified SOAP header here
            soapResHeader = handleSoapHeader(hdrElem,response.getCharacterEncoding(), context);

            // process XML/A request
            soapResBody = handleSoapBody(hdrElem, bodyElem, response.getCharacterEncoding(), context);

            marshallSoapMessage(outputStream, soapResCharEncoding, soapResHeader, soapResBody);
        } catch (Throwable t) {
            LOGGER.error("Errors when handling XML/A message", t);
            soapResBody = handleFault(t, soapResCharEncoding);
            marshallSoapMessage(outputStream, soapResCharEncoding, soapResHeader, soapResBody);
            return;
        }

    }

    /**
     * Implement to provide application specified SOAP unmarshalling algorithm.
     *
     * @return SOAP header, body as a tow items array.
     */
    protected abstract Element[] unmarshallSoapMessage(
            InputStream inputStream) throws Exception;

    /**
     * Implement to handle application specified SOAP header.
     *
     * @return if no header data in response, please return null or byte[0].
     */
    protected abstract byte[] handleSoapHeader(
            Element bodyElem,
            String charEncoding,
            Map context) throws Exception;

    /**
     * Implement to hanle XML/A request.
     *
     * @return XML/A response.
     */
    protected abstract byte[] handleSoapBody(
            Element hdrElem,
            Element bodyElem,
            String charEncoding,
            Map context) throws Exception;

    /**
     * Implement to privode application specified SOAP marshalling algorithm.
     */
    protected abstract void marshallSoapMessage(
            OutputStream outputStream,
            String encoding,
            byte[] soapHeader,
            byte[] soapBody);

    /**
     * Implement to application specified handler of SOAP fualt.
     */
    protected abstract byte[] handleFault(Throwable t, String charEncoding);

    /**
     * Initialize XML/A handler
     */
    private void initXmlaHandler() {
        try {
            final Parser parser = XOMUtil.createDefaultParser();
            final DOMWrapper doc = parser.parse(dataSourcesConfigUrl);
            DataSourcesConfig.DataSources dataSources = new DataSourcesConfig.DataSources(doc);
            xmlaHandler = new XmlaHandler(dataSources, catalogLocator);
        } catch (XOMException e) {
            throw Util.newError(e, "Failed to parse data sources config '" + dataSourcesConfigUrl.toExternalForm() + "'");
        }
    }

    /**
     * Registers callbacks configured in web.xml.
     */
    private void initCallbacks(ServletConfig servletConfig) {
        String callbacksValue = servletConfig.getInitParameter(PARAM_CALLBACKS);

        if (callbacksValue != null) {
            String[] classNames = callbacksValue.split(";");

            int count = 0;
            nextCallback:
            for (int i = 0; i < classNames.length; i++) {
                String className = classNames[i].trim();

                try {
                    Class cls = Class.forName(className);
                    if (XmlaRequestCallback.class.isAssignableFrom(cls)) {
                        XmlaRequestCallback callback = (XmlaRequestCallback) cls.newInstance();

                        try {
                            callback.init(servletConfig);
                        } catch (Exception e) {
                            LOGGER.warn("Failed to initialize callback '" + className + "'", e);
                            continue nextCallback;
                        }

                        addCallback(callback);
                        count++;

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.info("Register callback '" + className + "'");
                        }
                    } else {
                        LOGGER.warn("'" + className + "' is not an implementation of '" + XmlaRequestCallback.class + "'");
                    }
                } catch (ClassNotFoundException cnfe) {
                    LOGGER.warn("Callback class '" + className + "' not found", cnfe);
                } catch (InstantiationException ie) {
                    LOGGER.warn("Can't instantiate class '" + className + "'", ie);
                } catch (IllegalAccessException iae) {
                    LOGGER.warn("Can't instantiate class '" + className + "'", iae);
                }
            }
            LOGGER.debug("Registered " + count + " callback" + (count > 1 ? "s" : ""));
        }
    }

}

// End XmlaServlet.java
