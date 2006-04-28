/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
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

import javax.servlet.ServletContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
public abstract class XmlaServlet extends HttpServlet
                                  implements XmlaConstants {

    private static final Logger LOGGER = Logger.getLogger(XmlaServlet.class);

    public static final String PARAM_DATASOURCES_CONFIG = "DataSourcesConfig";
    public static final String PARAM_OPTIONAL_DATASOURCE_CONFIG = 
            "OptionalDataSourceConfig";
    public static final String PARAM_CHAR_ENCODING = "CharacterEncoding";
    public static final String PARAM_CALLBACKS = "Callbacks";

    public static final String DEFAULT_DATASOURCE_FILE = "datasources.xml";

    public static final int VALIDATE_HTTP_HEAD_PHASE    = 1;
    public static final int INITIAL_PARSE_PHASE         = 2;
    public static final int CALLBACK_PRE_ACTION_PHASE   = 3;
    public static final int PROCESS_HEADER_PHASE        = 4;
    public static final int PROCESS_BODY_PHASE          = 5;
    public static final int CALLBACK_POST_ACTION_PHASE  = 6;
    public static final int SEND_RESPONSE_PHASE         = 7;
    public static final int SEND_ERROR_PHASE            = 8;

    /** 
     * If paramName's value is not null and 'true', then return true. 
     * 
     * @param servletConfig 
     * @param paramName 
     * @return 
     */
    public static boolean getBooleanInitParameter(
            ServletConfig servletConfig,
            String paramName) {
        String paramValue = servletConfig.getInitParameter(paramName);
        return ((paramValue != null) && 
            Boolean.valueOf(paramValue).booleanValue());
    }
    public static boolean getParameter(
            HttpServletRequest req, 
            String paramName) {
        String paramValue = req.getParameter(paramName);
        return ((paramValue != null) && 
            Boolean.valueOf(paramValue).booleanValue());
    }

    protected CatalogLocator catalogLocator = null;
    protected DataSourcesConfig.DataSources dataSources = null;
    protected XmlaHandler xmlaHandler = null;
    protected String charEncoding = null;
    private final List callbackList = new ArrayList();

    public XmlaServlet() {
    }


    /**
     * Initializes servlet and XML/A handler.
     *
     * @param servletConfig
     * @throws ServletException
     */
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);

        // init: charEncoding
        initCharEncodingHandler(servletConfig);

        // init: callbacks
        initCallbacks(servletConfig);

        // make: catalogLocator
        // A derived class can alter how the calalog locator object is
        // created.
        this.catalogLocator = makeCatalogLocator(servletConfig);

        DataSourcesConfig.DataSources dataSources = 
                makeDataSources(servletConfig);
        addToDataSources(dataSources);
    }

    /** 
     * Get and create if needed the XmlaHandler. 
     * 
     * @return 
     */
    protected XmlaHandler getXmlaHandler() {
        if (this.xmlaHandler == null) {
            this.xmlaHandler = 
                new XmlaHandler(this.dataSources, this.catalogLocator);
        }
        return this.xmlaHandler;
    }

    /**
     * Registers a callback.
     */
    protected final void addCallback(XmlaRequestCallback callback) {
        callbackList.add(callback);
    }

    /** 
     * Return an unmodifiable list of callbacks. 
     * 
     * @return 
     */
    protected final List getCallbacks() {
        return Collections.unmodifiableList(callbackList);
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

        // Request Soap Header and Body
        // header [0] and body [1]
        Element[] requestSoapParts = new Element[2];

        // Response Soap Header and Body
        // An array allows response parts to be passed into callback
        // and possible modifications returned.
        // response header in [0] and response body in [1]
        byte[][] responseSoapParts = new byte[2][];

        int phase = VALIDATE_HTTP_HEAD_PHASE;

        try {

            phase = INITIAL_PARSE_PHASE;


            if (charEncoding != null) {
                try {
                    request.setCharacterEncoding(charEncoding);
                    response.setCharacterEncoding(charEncoding);
                } catch (UnsupportedEncodingException uee) {
                    charEncoding = null;
                    String msg = "Unsupported character encoding '" + 
                        charEncoding + 
                        "': " +
                        "Use default character encoding from HTTP client for now";
                    LOGGER.warn(msg);
                }
            }

            response.setContentType("text/xml");

            Map context = new HashMap(); 

            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Invoking validate http header callbacks");
                }
                for (Iterator it = getCallbacks().iterator(); it.hasNext(); ) {
                    XmlaRequestCallback callback = 
                            (XmlaRequestCallback) it.next();
                    if (!callback.processHttpHeader(request, response, context)) {
                        return;
                    }
                }

            } catch (XmlaException xex) {
                LOGGER.error("Errors when invoking callbacks validateHttpHeader", xex);
                handleFault(response, responseSoapParts, phase, xex);
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;

            } catch (Exception ex) {
                LOGGER.error("Errors when invoking callbacks validateHttpHeader", ex);
                handleFault(response, responseSoapParts, 
                        phase, new XmlaException(
                                SERVER_FAULT_FC,
                                CHH_CODE, 
                                CHH_FAULT_FS,
                                ex));
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;
            }



            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Unmarshalling SOAP message");
                }

                // check request's content type
                String contentType = request.getContentType();
                if (contentType == null || 
                    contentType.indexOf("text/xml") == -1) {
                    throw new IllegalArgumentException("Only accepts content type 'text/xml', not '" + contentType + "'");
                }

                unmarshallSoapMessage(request, requestSoapParts);

            } catch (XmlaException xex) {
                LOGGER.error("Unable to unmarshall SOAP message", xex);
                handleFault(response, responseSoapParts, phase, xex);
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;
            }

            phase = CALLBACK_PRE_ACTION_PHASE;


            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Invoking callbacks preAction");
                }

                for (Iterator it = getCallbacks().iterator(); it.hasNext(); ) {
                    XmlaRequestCallback callback = 
                            (XmlaRequestCallback) it.next();
                    callback.preAction(request, requestSoapParts, context);
                }
            } catch (XmlaException xex) {
                LOGGER.error("Errors when invoking callbacks preaction", xex);
                handleFault(response, responseSoapParts, phase, xex);
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;

            } catch (Exception ex) {
                LOGGER.error("Errors when invoking callbacks preaction", ex);
                handleFault(response, responseSoapParts, 
                        phase, new XmlaException(
                                SERVER_FAULT_FC,
                                CPREA_CODE, 
                                CPREA_FAULT_FS,
                                ex));
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;
            }

            phase = PROCESS_HEADER_PHASE;

            // freeze context binding
            // context = Collections.unmodifiableMap(context);

            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Handling XML/A message header");
                }

                // process application specified SOAP header here
                handleSoapHeader(response,
                                 requestSoapParts,
                                 responseSoapParts,
                                 context);
            } catch (XmlaException xex) {
                LOGGER.error("Errors when handling XML/A message", xex);
                handleFault(response, responseSoapParts, phase, xex);
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;
            }

            phase = PROCESS_BODY_PHASE;

            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Handling XML/A message body");
                }

                // process XML/A request
                handleSoapBody(response,
                               requestSoapParts,
                               responseSoapParts,
                               context);

            } catch (XmlaException xex) {
                LOGGER.error("Errors when handling XML/A message", xex);
                handleFault(response, responseSoapParts, phase, xex);
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;
            }

            phase = CALLBACK_POST_ACTION_PHASE;

            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Invoking callbacks postAction");
                }

                for (Iterator it = getCallbacks().iterator(); it.hasNext(); ) {
                    XmlaRequestCallback callback = 
                            (XmlaRequestCallback) it.next();
                    callback.postAction(request, response, 
                                        responseSoapParts, context);
                }
            } catch (XmlaException xex) {
                LOGGER.error("Errors when invoking callbacks postaction", xex);
                handleFault(response, responseSoapParts, phase, xex);
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;

            } catch (Exception ex) {
                LOGGER.error("Errors when invoking callbacks postaction", ex);
                handleFault(response, responseSoapParts, 
                        phase, new XmlaException(
                                SERVER_FAULT_FC,
                                CPOSTA_CODE, 
                                CPOSTA_FAULT_FS,
                                ex));
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;
            }

            phase = SEND_RESPONSE_PHASE;

            try {

                marshallSoapMessage(response, responseSoapParts);

            } catch (XmlaException xex) {
                LOGGER.error("Errors when handling XML/A message", xex);
                handleFault(response, responseSoapParts, phase, xex);
                phase = SEND_ERROR_PHASE;
                marshallSoapMessage(response, responseSoapParts);
                return;
            }

        } catch (Throwable t) {
            LOGGER.error("Unknown Error when handling XML/A message", t);
            handleFault(response, responseSoapParts, phase, t);
            marshallSoapMessage(response, responseSoapParts);
        }

    }

    /**
     * Implement to provide application specified SOAP unmarshalling algorithm.
     *
     * @return SOAP header, body as a tow items array.
     */
    protected abstract void unmarshallSoapMessage(
            HttpServletRequest request,
            Element[] requestSoapParts) throws XmlaException;

    /**
     * Implement to handle application specified SOAP header.
     *
     * @return if no header data in response, please return null or byte[0].
     */
    protected abstract void handleSoapHeader(
            HttpServletResponse response,
            Element[] requestSoapParts,
            byte[][] responseSoapParts,
            Map context) throws XmlaException;

    /**
     * Implement to hanle XML/A request.
     *
     * @return XML/A response.
     */
    protected abstract void handleSoapBody(
            HttpServletResponse response,
            Element[] requestSoapParts,
            byte[][] responseSoapParts,
            Map context) throws XmlaException;

    /**
     * Implement to privode application specified SOAP marshalling algorithm.
     */
    protected abstract void marshallSoapMessage(
            HttpServletResponse response,
            byte[][] responseSoapParts) throws XmlaException;

    /**
     * Implement to application specified handler of SOAP fualt.
     */
    protected abstract void handleFault(
            HttpServletResponse response,
            byte[][] responseSoapParts,
            int phase,
            Throwable t);



    /**
     * Make catalog locator.  Derived classes can roll their own
     */
    protected CatalogLocator makeCatalogLocator(ServletConfig servletConfig) {
        ServletContext servletContext = servletConfig.getServletContext();
        return new ServletContextCatalogLocator(servletContext);
    }

    /**
     * Make DataSourcesConfig.DataSources instance. Derived classes
     * can roll their own
     * <p>
     * If there is an initParameter called "DataSourcesConfig"
     * get its value, replace any "${key}" content with "value" where
     * "key/value" are System properties, and try to create a URL
     * instance out of it. If that fails, then assume its a 
     * real filepath and if the file exists then create a URL from it
     * (but only if the file exists).
     * If there is no initParameter with that name, then attempt to
     * find the file called "datasources.xml"  under "WEB-INF/"
     * and if it exists, use it.
     */
    protected DataSourcesConfig.DataSources makeDataSources(
                ServletConfig servletConfig) {

        String paramValue = 
                servletConfig.getInitParameter(PARAM_DATASOURCES_CONFIG);
        // if false, then do not throw exception if the file/url
        // can not be found
        boolean optional = 
            getBooleanInitParameter(servletConfig, PARAM_OPTIONAL_DATASOURCE_CONFIG);

        URL dataSourcesConfigUrl = null;
        try {
            if (paramValue == null) {
                // fallback to default
                String defaultDS = "WEB-INF/" + DEFAULT_DATASOURCE_FILE;
                ServletContext servletContext = servletConfig.getServletContext();
                File realPath = new File(servletContext.getRealPath(defaultDS));
                if (realPath.exists()) {
                    // only if it exists
                    dataSourcesConfigUrl = realPath.toURL();
                }
            } else {
                paramValue = Util.replaceProperties(paramValue, 
                                      System.getProperties());
                if (LOGGER.isDebugEnabled()) {
                    String msg = "XmlaServlet.makeDataSources: " +
                            "paramValue="+paramValue;
                    LOGGER.debug(msg);
                }
                // is the parameter a valid URL
                MalformedURLException mue = null;
                try {
                    dataSourcesConfigUrl = new URL(paramValue);
                } catch (MalformedURLException e) {
                    // not a valid url
                    mue = e;
                }
                if (dataSourcesConfigUrl == null) {
                    // see if its a full valid file path
                    File f = new File(paramValue);
                    if (f.exists()) {
                        // yes, a real file path
                        dataSourcesConfigUrl = f.toURL();
                    } else if (mue != null) {
                        // neither url or file, 
                        // is it not optional
                        if (! optional) {
                            throw mue;
                        }
                    }
                }
            }
        } catch (MalformedURLException mue) {
            throw Util.newError(mue, "invalid URL path '" + paramValue + "'");
        }

        if (LOGGER.isDebugEnabled()) {
            String msg = "XmlaServlet.makeDataSources: " +
                    "dataSourcesConfigUrl="+dataSourcesConfigUrl;
            LOGGER.debug(msg);
        }
        // don't try to parse a null 
        return (dataSourcesConfigUrl == null) 
            ? null : parseDataSourcesUrl(dataSourcesConfigUrl);
    }

    protected void addToDataSources(DataSourcesConfig.DataSources dataSources) {
        if (this.dataSources == null) {
            this.dataSources = dataSources;
        } else {
            DataSourcesConfig.DataSource[] ds1 = this.dataSources.dataSources;
            int len1 = ds1.length;
            DataSourcesConfig.DataSource[] ds2 = dataSources.dataSources;
            int len2 = ds2.length;

            DataSourcesConfig.DataSource[] tmp = 
                new DataSourcesConfig.DataSource[len1+len2];

            System.arraycopy(ds1, 0, tmp, 0, len1);
            System.arraycopy(ds2, 0, tmp, len1, len2);

            this.dataSources.dataSources = tmp;
        }
    }

    protected DataSourcesConfig.DataSources parseDataSourcesUrl(
                URL dataSourcesConfigUrl) {

        try {
            String dataSourcesConfigString = 
                Util.readURL(dataSourcesConfigUrl, System.getProperties());
            return parseDataSources(dataSourcesConfigString);

        } catch (Exception e) {
            throw Util.newError(e, "Failed to parse data sources config '" +
                                dataSourcesConfigUrl.toExternalForm() + "'");
        }
    }
    protected DataSourcesConfig.DataSources parseDataSources(
                String dataSourcesConfigString) {

        try {
            dataSourcesConfigString = 
                Util.replaceProperties(dataSourcesConfigString, 
                                      System.getProperties());

        if (LOGGER.isDebugEnabled()) {
            String msg = "XmlaServlet.parseDataSources: " +
                    "dataSources="+dataSourcesConfigString;
            LOGGER.debug(msg);
        }
            final Parser parser = XOMUtil.createDefaultParser();
            final DOMWrapper doc = parser.parse(dataSourcesConfigString);
            return new DataSourcesConfig.DataSources(doc);

        } catch (XOMException e) {
            throw Util.newError(e, "Failed to parse data sources config: " +
                                dataSourcesConfigString);
        }
    }

    /**
     * Initialize character encoding
     */
    protected void initCharEncodingHandler(ServletConfig servletConfig) {
        String paramValue = servletConfig.getInitParameter(PARAM_CHAR_ENCODING);
        if (paramValue != null) {
            this.charEncoding = paramValue;
        } else {
            this.charEncoding = null;
            LOGGER.warn("Use default character encoding from HTTP client");
        }
    }

    /**
     * Registers callbacks configured in web.xml.
     */
    protected void initCallbacks(ServletConfig servletConfig) {
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
                        XmlaRequestCallback callback =
                            (XmlaRequestCallback) cls.newInstance();

                        try {
                            callback.init(servletConfig);
                        } catch (Exception e) {
                            LOGGER.warn("Failed to initialize callback '" +
                                        className + "'", e);
                            continue nextCallback;
                        }

                        addCallback(callback);
                        count++;

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.info("Register callback '" +
                                        className + "'");
                        }
                    } else {
                        LOGGER.warn("'" + className +
                                    "' is not an implementation of '" +
                                    XmlaRequestCallback.class + "'");
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
