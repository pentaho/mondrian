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
import java.io.PrintWriter;

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
        final String soapRequest = request.getParameter("SOAPRequest");
        final PrintWriter printWriter;
        try {
            printWriter = response.getWriter();
        } catch (IOException e) {
            return;
        }
        mediator.process(soapRequest, printWriter);
    }
}

// End XmlaServlet.java
