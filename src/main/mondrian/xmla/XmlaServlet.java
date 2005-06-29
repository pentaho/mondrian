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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

/**
 * An <code>XmlaServlet</code> responds to XML for Analysis SOAP requests.
 *
 * @see XmlaMediator
 *
 * @author jhyde
 * @since 27 April, 2003
 * @version $Id$
 */
public class XmlaServlet extends HttpServlet {
    private static final Logger LOGGER = Logger.getLogger(XmlaServlet.class);

    private final XmlaMediator mediator = new XmlaMediator();

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
        
        PrintWriter printWriter;
        try {
            printWriter = response.getWriter();
        } catch (IOException e) {
            return;
        }
        
        final StringBuffer sb = new StringBuffer();
        if (LOGGER.isDebugEnabled()) {
            printWriter = new PrintWriter(
                    new FilterWriter(printWriter) {

                        public void write(int c) throws IOException {
                            this.out.write(c);
                            sb.append((char) c);
                        }

                        public void write(char cbuf[], int off, int len) throws IOException {
                            this.out.write(cbuf, off, len);
                            sb.append(new String(cbuf, off, len));
                        }

                        public void write(String str, int off, int len) throws IOException {
                            super.write(str, off, len);
                            sb.append(str.substring(off, len));
                        }
                    }
            );
        }
        mediator.threadServletContext.set(getServletContext());
        response.setContentType("text/xml");
        mediator.process(soapRequest, printWriter);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Response:");
            LOGGER.debug(sb.toString());
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
