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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import mondrian.olap.Util;
import mondrian.util.SAXHandler;
import mondrian.util.SAXWriter;

import org.apache.log4j.Logger;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;
import org.xml.sax.SAXException;

/**
 * An <code>XmlaServlet</code> responds to XML for Analysis SOAP requests.
 *
 * @see mondrian.xmla.XmlaMediator
 *
 * @author jhyde
 * @since 27 April, 2003
 * @version $Id$
 */
public class XmlaServlet extends HttpServlet {
    private static final Logger LOGGER = Logger.getLogger(XmlaServlet.class);

    public static final String DATA_SOURCES_CONFIG = "DataSourcesConfig";

    private final XmlaMediator mediator = new XmlaMediator();

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        String paramValue = config.getInitParameter(DATA_SOURCES_CONFIG);
        if (null == paramValue) {
            Util.newError("No data source has been configured. Please set parameter '" +
                    DATA_SOURCES_CONFIG + "' for this servlet.");
        }

        ServletContext context = config.getServletContext();

        try {
            URL configUrl = null;
            try {
                configUrl = new URL(paramValue);
            } catch (MalformedURLException e) {
                configUrl = new File(context.getRealPath("WEB-INF/" + paramValue)).toURL();
            }

            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(configUrl);
            DataSourcesConfig.DataSources dataSources = new DataSourcesConfig.DataSources(def);
            XmlaMediator.initDataSourcesMap(dataSources);
        } catch (MalformedURLException e) {
            throw Util.newError(e, "while parsing data sources config '" + paramValue + "'");
        } catch (XOMException e) {
            throw Util.newError(e, "while parsing data sources config '" + paramValue + "'");
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        process(request, response);
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        process(request, response);
    }

    public void process(HttpServletRequest request, HttpServletResponse response) {
        String soapRequest = request.getParameter("SOAPRequest");
        if (soapRequest == null) {
            try {
                soapRequest = toString(request.getInputStream());
            } catch (IOException e) {
                return;
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Pathinfo=" + request.getPathInfo());
            final Map map = request.getParameterMap();
            for (Iterator iterator = map.keySet().iterator(); iterator.hasNext();) {
                String att = (String) iterator.next();
                String[] vals = (String[]) map.get(att);
                LOGGER.debug(
                        att + "=" +
                        (vals != null && vals.length > 0 ? vals[0] :
                        "<null>"));
            }
            final Enumeration headerNames = request.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                String header = (String) headerNames.nextElement();
                LOGGER.debug("Header " + header + "=" + request.getHeader(header));
            }
            LOGGER.debug("Request: " + soapRequest);
        }

        StringWriter responseWriter = new StringWriter();

        XmlaMediator.threadServletContext.set(getServletContext());
        response.setContentType("text/xml");

        try {
            mediator.process(soapRequest, responseWriter);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Response:");
                LOGGER.debug(responseWriter.getBuffer().toString());
            }
        } catch (Throwable t) {
            t = XmlaMediator.gotoRootThrowable(t);
            LOGGER.warn("Error while processing XML/A request", t);
            responseWriter = new StringWriter();
            SAXHandler handler = new SAXHandler(new SAXWriter(responseWriter));
            try {
                handler.startDocument();
                handler.startElement("SOAP-ENV:Envelope", new String[] {
                    "xmlns:SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/",
                    "SOAP-ENV:encodingStyle", "http://schemas.xmlsoap.org/soap/encoding/",
                });
                handler.startElement("SOAP-ENV:Body");
                handler.startElement("SOAP-ENV:Fault");

                handler.startElement("faultcode");
                handler.characters(t.getClass().getName());
                handler.endElement();

                handler.startElement("faultstring");
                handler.characters(t.getMessage());
                handler.endElement();

                handler.startElement("faultactor");
                handler.characters("Mondrian");
                handler.endElement();

                handler.startElement("detail");
                // Don't dump stack trace to client
//                handler.startElement("ExceptionStackTrace");
//                StringWriter stackWriter = new StringWriter();
//                t.printStackTrace(new PrintWriter(stackWriter));
//                handler.characters(stackWriter.getBuffer().toString());
//                handler.endElement();
                handler.endElement();

                handler.endElement();   // Fault
                handler.endElement();   // Body
                handler.endElement();   // Envolope
                handler.endDocument();
            } catch (SAXException se) {
                LOGGER.warn("Error while reporting error to client", se);
            }
        }

        try {
            Writer outWriter = response.getWriter();
            outWriter.write(responseWriter.getBuffer().toString());
        } catch (IOException ioe) {
            LOGGER.warn("Error while writing response to client", ioe);
        }
    }

    private static String toString(InputStream is) throws IOException {
        final InputStreamReader reader = new InputStreamReader(is);
        char[] buf = new char[2048];
        StringBuffer sb = new StringBuffer();
        int n;
        while ((n = reader.read(buf)) > 0) {
            sb.append(buf, 0, n);
        }
        return sb.toString();
    }
}

// End XmlaServlet.java
