/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde
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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
        if (true) {
            System.out.println("Pathinfo=" + request.getPathInfo());
            final Map map = request.getParameterMap();
            for (Iterator iterator = map.keySet().iterator(); iterator.hasNext();) {
                String att = (String) iterator.next();
                System.out.println(att + "=" + map.get(att));
            }
            final Enumeration headerNames = request.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                String header = (String) headerNames.nextElement();
                System.out.println("Header " + header + "=" + request.getHeader(header));
            }
            System.out.println("Request: " + soapRequest);
        }
        final PrintWriter printWriter;
        try {
            printWriter = response.getWriter();
        } catch (IOException e) {
            return;
        }
        mediator.threadServletContext.set(getServletContext());
        mediator.process(soapRequest, printWriter);
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
