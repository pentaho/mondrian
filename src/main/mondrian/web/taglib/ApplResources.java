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

import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianResource;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * holds an instance of the mdx connection in the servlet context
 */

public class ApplResources implements Listener.ApplicationContext {

	private static final String ATTRNAME = "mondrian.web.taglib.ApplResources";
	private Connection connection;
	private ServletContext context;

	/**
	 * Creates a <code>ApplResources</code>. Only {@link Listener} calls this;
	 * you should probably call {@link #getInstance}.
	 */
	public ApplResources() {
	}

	/**
	 * Retrieves the one and only instance of <code>ApplResources</code> in
	 * this servlet's context.
	 */
	public static ApplResources getInstance(ServletContext context) {
		return (ApplResources)context.getAttribute(ATTRNAME);
	}

	public Connection getConnection() {
		return connection;
	}

	private HashMap templatesCache = new HashMap();
	public Transformer getTransformer(String xsltURI, boolean useCache) {
		try {
			Templates templates = null;
			if (useCache)
				templates = (Templates)templatesCache.get(xsltURI);
			if (templates == null) {
				TransformerFactory tf = TransformerFactory.newInstance();
				InputStream input = context.getResourceAsStream(xsltURI);
				templates = tf.newTemplates(new StreamSource(input));
				if (useCache)
					templatesCache.put(xsltURI, templates);
			}
			return templates.newTransformer();
		}
		catch (TransformerConfigurationException e) {
			e.printStackTrace();
			throw new RuntimeException(e.toString());
		}
	}

	// implement ApplicationContext
	public void init(ServletContextEvent event) {
		this.context = event.getServletContext();

		try {
			// static initialization taken from MDXQueryServlet
			String resourceURL = context.getInitParameter("resourceURL");
			// the following can cause security exception:
			// System.getProperties().put("mondrian.resourceURL", resourceURL);
			mondrian.olap.Util.setThreadRes(
				new MondrianResource(resourceURL, Locale.ENGLISH));

			String jdbcDrivers = context.getInitParameter("jdbcDrivers");
			StringTokenizer tok = new StringTokenizer(jdbcDrivers, ", ");
			while (tok.hasMoreTokens()) {
				String jdbcDriver = tok.nextToken();
				try {
					Class.forName(jdbcDriver);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}

			String connectString = context.getInitParameter("connectString");
			this.connection = DriverManager.getConnection(connectString, null, false);

		}
		catch (MalformedURLException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		context.setAttribute(ATTRNAME, this);
	}

	// implement ApplicationContext
	public void destroy(ServletContextEvent event) {
		connection.close();
	}

}

// End ApplResources.java
