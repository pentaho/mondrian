/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Sean McCullough, 13 February, 2002, 10:25 PM
*/

package mondrian.web.servlet;		

import mondrian.olap.*;
import mondrian.web.taglib.ResultCache;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.StringTokenizer;
import java.io.IOException;

/** 
 * <code>MDXQueryServlet</code> is a servlet which receives MDX queries,
 * executes them, and formats the results in an HTML table.
 *
 * @author  Sean McCullough
 * @since 13 February, 2002
 * @version $Id$
 */
public class MDXQueryServlet extends HttpServlet {
    String connectString;
    
    /** Initializes the servlet.
    */	
    public void init(ServletConfig config) throws ServletException {
	super.init(config);
	connectString = config.getInitParameter("connectString");
	String jdbcDrivers = config.getInitParameter("jdbcDrivers");
	StringTokenizer tok = new java.util.StringTokenizer(jdbcDrivers, ",");
	while (tok.hasMoreTokens()) {
	    String jdbcDriver = tok.nextToken();
	    try {
		Class.forName(jdbcDriver);
	    } catch (ClassNotFoundException e) {
		e.printStackTrace();
	    }
	}
    }

    /** Destroys the servlet.
    */	
    public void destroy() {

    }

    /** Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
    * @param request servlet request
    * @param response servlet response
    */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, java.io.IOException {
	String queryName = request.getParameter("query");
	request.setAttribute("query", queryName);
	if (queryName != null) {
	    processTransform(request,response);
	    return;
	}
	String queryString = request.getParameter("queryString");
	request.setAttribute("queryString", queryString);
	String resultString = "result";
	mondrian.olap.Connection mdxConnection = null;
	StringBuffer html = new StringBuffer();

	//execute the query
	try {
	    mdxConnection = DriverManager.getConnection(connectString, null, false);
	    Query q = mdxConnection.parseQuery(queryString);
	    Result result = mdxConnection.execute(q);
	    
	    Position slicers[] = result.getSlicerAxis().positions;
	    html.append("<table class='resulttable' cellspacing=1 border=0><tr><td nowrap class='slicer'>");
	    for (int i=0; i<slicers.length; i++) {
		Position position = slicers[i];		       
		for (int j = 0; j < position.members.length; j++) {
		    Member member = position.members[j];
		    html.append(member.getUniqueName());
		}
	    }
	    html.append("&nbsp;</td>");

	    Position columns[] = result.getAxes()[0].positions;
	    for (int i=0; i<columns.length; i++) {
		Position position = columns[i];		       
		for (int j = 0; j < position.members.length; j++) {
		    Member member = position.members[j];
		    html.append("<td nowrap class='columnheading'>" + member.getUniqueName() + "</td>");
		}
	    }
	    html.append("</tr>");
			
	    if (result.getAxes().length > 1) {
		for (int i=0; i<result.getAxes()[1].positions.length; i++) {
		    //TODO: fix this to iterate over multiple members if they exist for this position
		    Member m = result.getAxes()[1].positions[i].members[0];
		    html.append("<tr><td nowrap class='rowheading'>" + m.getUniqueName() + "</td>");
		    for (int j=0; j<result.getAxes()[0].positions.length; j++) {
			Cell cell = result.getCell(new int[]{j,i});
			html.append("<td class='cell'>" + cell.getFormattedValue() + "</td>");
		    }
		    html.append("</tr>");
		}
	    } else {
		for (int i=0; i<result.getAxes()[0].positions.length; i++) {
		    Cell cell = result.getCell(new int[]{i});
		    html.append("<tr><td></td><td class='cell'>" + cell.getFormattedValue() + "</td></tr>");
		}
	    }

	    html.append("</table>");
	} catch (Throwable e) {
	    html.append("Error: " + e.getMessage());
	    e.printStackTrace();
	} finally {
	    if (mdxConnection != null) {
		mdxConnection.close();
	    }
	}

	request.setAttribute("result", html.toString());
	getServletContext().getRequestDispatcher("/index.jsp").include(request, response);
    } 
	
    private void processTransform(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String queryName = request.getParameter("query");
		ResultCache rc = ResultCache.getInstance(request.getSession(), getServletContext(), queryName);
		Query query = rc.getQuery();
		query = query.safeClone();
		rc.setDirty();
		String operation = request.getParameter("operation");
		if (operation.equals("expand")) {
			String memberName = request.getParameter("member");
			boolean fail = true;
			Member member = query.getCube().lookupMemberByUniqueName(memberName, fail);
			query.toggleDrillState(member);
		} else {
			throw Util.newInternal("unkown operation '" + operation + "'");
		}
		rc.setQuery(query);
		String redirect = request.getParameter("redirect");
		if (redirect == null) {
			redirect = "/index.jsp";
		}
		getServletContext().getRequestDispatcher(redirect).include(request, response);
	}

    /** Handles the HTTP <code>GET</code> method.
    * @param request servlet request
    * @param response servlet response
    */
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, java.io.IOException {
	processRequest(request, response);
    } 

    /** Handles the HTTP <code>POST</code> method.
    * @param request servlet request
    * @param response servlet response
    */
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, java.io.IOException {
	processRequest(request, response);
    }

    /** Returns a short description of the servlet.
    */
    public String getServletInfo() {
	return "Process an MDX query and return the result formatted as an HTML table";
    }

}
