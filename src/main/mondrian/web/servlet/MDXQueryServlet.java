/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Sean McCullough, 13 February, 2002, 10:25 PM
*/

package mondrian.web.servlet;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import mondrian.olap.Cell;
import mondrian.olap.DriverManager;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.web.taglib.ResultCache;
import mondrian.xom.StringEscaper;

/**
 * <code>MDXQueryServlet</code> is a servlet which receives MDX queries,
 * executes them, and formats the results in an HTML table.
 *
 * @author  Sean McCullough
 * @since 13 February, 2002
 * @version $Id$
 */
public class MDXQueryServlet extends HttpServlet {
	private String connectString;

	/** Initializes the servlet.
	 */
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		connectString = config.getInitParameter("connectString");
		Enumeration initParameterNames = config.getInitParameterNames();
		while (initParameterNames.hasMoreElements()) {
			String name = (String) initParameterNames.nextElement();
			String value = config.getInitParameter(name);
			MondrianProperties.instance().setProperty(name, value);
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
		mondrian.olap.Connection mdxConnection = null;
		StringBuffer html = new StringBuffer();

		//execute the query
		try {
			mdxConnection = DriverManager.getConnection(connectString, getServletContext(), false);
			Query q = mdxConnection.parseQuery(queryString);
			Result result = mdxConnection.execute(q);
			Position slicers[] = result.getSlicerAxis().positions;
			html.append("<table class='resulttable' cellspacing=1 border=0>");
			final String nl = System.getProperty("line.separator");
			html.append(nl);

			Position[] columns = result.getAxes()[0].positions;
			Position[] rows = null;
			if( result.getAxes().length == 2 )
				rows = result.getAxes()[1].positions;

			int columnWidth = columns[0].members.length;
			int rowWidth = 0;
			if( result.getAxes().length == 2 )
					rowWidth = result.getAxes()[1].positions[0].members.length;
	
			for (int j=0; j<columnWidth; j++) {
				if (j == 0) {
					// Print the top-left cell, and fill it with slicer members.
					html.append("<tr><td nowrap class='slicer' rowspan='" +
							columnWidth + "' colspan='" + rowWidth + "'>");
					for (int i=0; i<slicers.length; i++) {
						Position position = slicers[i];
						for (int k = 0; k < position.members.length; k++) {
							if (k > 0) {
								html.append("<br/>");
							}
							Member member = position.members[k];
							html.append(member.getUniqueName());
						}
					}
					html.append("&nbsp;</td>" + nl);
				} else {
					html.append("<tr>");
				}
				// Print the column headings.
				for (int i=0; i<columns.length; i++) {
					Member member = columns[i].members[j];
					int width = 1;
					while ((i + 1) < columns.length &&
							columns[i + 1].members[j] == member) {
						i++;
						width++;
					}
					html.append("<td nowrap class='columnheading' colspan='" +
							width + "'>" + member.getUniqueName() + "</td>");
				}
				html.append("</tr>" + nl);
			}

			if (result.getAxes().length > 1) {
				for (int i=0; i<rows.length; i++) {
					html.append("<tr>");
					final Position row = rows[i];
					for (int j = 0; j < row.members.length; j++) {
						Member member = row.members[j];
						html.append("<td nowrap class='rowheading'>" +
								member.getUniqueName() + "</td>");
					}
					for (int j=0; j<columns.length; j++) {
						Cell cell = result.getCell(new int[]{j,i});
						html.append("<td class='cell'>" + cell.getFormattedValue() + "</td>");
					}
					html.append("</tr>");
				}
			} else {
				for (int i=0; i<columns.length; i++) {
					Cell cell = result.getCell(new int[]{i});
					html.append("<tr><td></td><td class='cell'>" + cell.getFormattedValue() + "</td></tr>");
				}
			}

			html.append("</table>");
		} catch (Throwable e) {
			final String[] strings = Util.convertStackToString(e);
			html.append("Error:<pre><blockquote>");
			for (int i = 0; i < strings.length; i++) {
				StringEscaper.htmlEscaper.appendEscapedString(strings[i], html);
			}
			html.append("</blockquote></pre>");
		} finally {
			if (mdxConnection != null) {
				mdxConnection.close();
			}
		}

		request.setAttribute("result", html.toString());
		response.setHeader("Content-Type", "text/html");
		getServletContext().getRequestDispatcher("/adhoc.jsp").include(request, response);
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
			Member member = query.getSchemaReader().getMemberByUniqueName(
					Util.explode(memberName), fail);
			query.toggleDrillState(member);
		} else {
			throw Util.newInternal("unkown operation '" + operation + "'");
		}
		rc.setQuery(query);
		String redirect = request.getParameter("redirect");
		if (redirect == null) {
			redirect = "/adhoc.jsp";
		}
		response.setHeader("Content-Type", "text/html");
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
