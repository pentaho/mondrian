/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Andreas Voss, 22 March, 2002
*/
package mondrian.web.taglib;

import javax.servlet.http.HttpSession;

import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Parser;
import javax.servlet.ServletContext;
import mondrian.olap.Connection;
import org.w3c.dom.Document;
import javax.xml.parsers.ParserConfigurationException;

/**
 * holds a query/result pair in the users session
 */

public class ResultCache {
	private final static String ATTR_NAME = "mondrian.web.taglib.ResultCache.";
	private mondrian.olap.Query query = null;
	private mondrian.olap.Result result = null;
	private Document document = null;
	private ServletContext servletContext;

	private ResultCache(ServletContext context) {
		this.servletContext = context;
	}

	public static ResultCache getInstance(HttpSession session, String name) {
		String fqname = ATTR_NAME + name;
		ResultCache query = (ResultCache)session.getAttribute(fqname);
		if (query == null) {
			query = new ResultCache(session.getServletContext());
			session.setAttribute(fqname, query);
		}
		return query;
	}


	public void parse(String mdx) {
		Connection con = ApplResources.getInstance(servletContext).getConnection();
		query = con.parseQuery(mdx);
		setDirty();
	}


	public Result getResult() {
		if (result == null) {
			long t1 = System.currentTimeMillis();
			result = ApplResources.getInstance(servletContext).getConnection().execute(query);
			long t2 = System.currentTimeMillis();
			System.out.println("Execute query took " + (t2 - t1) + " millisec");
		}
		return result;
	}

	public Document getDOM() {
		try {
			if (document == null) {
				document = DOMBuilder.build(getResult());
			}
			return document;
		}
		catch (ParserConfigurationException e) {
			e.printStackTrace();
			throw new RuntimeException(e.toString());
		}
	}

	/**
	 * if you modify the query, call setDirty(true)
	 */
	public Query getQuery() {
		return query;
	}

	/**
	 * set to dirty after you have modified the query to force a recalcuation
	 */
	public void setDirty() {
		result = null;
		document = null;
	}


}

// End ResultCache.java
