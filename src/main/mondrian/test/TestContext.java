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
// jhyde, 29 March, 2002
*/
package mondrian.test;

import mondrian.olap.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * <code>TestContext</code> is a singleton class which contains the information
 * necessary to run mondrian tests (otherwise we'd have to pass this
 * information into the constructor of TestCases).
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public class TestContext {
	private static TestContext instance; // the singleton
	private PrintWriter pw;
	/** Connect string for the FoodMart database. Set at {@link #init} time,
	 * but the connection is not created until the first call to {@link
	 * #getFoodMartConnection}.
	 **/
	private String foodMartConnectString;
	/** Connection to the FoodMart database. Set on the first call to {@link
	 * #getFoodMartConnection}. **/
	private Connection foodMartConnection;
	/** Properties, drawn from {@link System#getProperties}, plus the contents
	 * of "mondrian.properties" if it exists.
	 */
	private PropertiesPlus properties;

	/**
	 * Retrieves the singleton (instantiating if necessary).
	 */
	public static TestContext instance() {
		if (instance == null) {
			synchronized (TestContext.class) {
				if (instance == null) {
					instance = new TestContext();
				}
			}
		}
		return instance;
	}

	/** Creates a TestContext. Called only from {@link #init}. **/
	private TestContext() {
		this.pw = new PrintWriter(System.out, true);

		mondrian.olap.Util.setThreadRes(MondrianResource.instance());

		String jdbcDrivers = getProperties().getProperty(
				"mondrian.jdbcDrivers", "org.hsqldb.jdbcDriver");
		StringTokenizer tok = new java.util.StringTokenizer(jdbcDrivers, ",");
		while (tok.hasMoreTokens()) {
			String jdbcDriver = tok.nextToken();
			try {
				Class.forName(jdbcDriver);
			} catch (ClassNotFoundException e) {
				pw.println("Could not find driver " + jdbcDriver);
			}
		}

		foodMartConnectString = System.getProperty("mondrian.test.connectString");
		if (foodMartConnectString == null) {
			URL catalogUrl = mondrian.resource.Util.convertPathToURL(
					new File("demo/FoodMart.xml"));
			String jdbcURL = getProperties().getProperty(
					"mondrian.test.jdbcURL", "jdbc:hsqldb:demo/hsql/FoodMart");
			foodMartConnectString = "Provider=mondrian;" +
					"Jdbc=" + jdbcURL + ";" +
					"Catalog=" + catalogUrl;
		}
	}

	/** Returns a {@link Properties} object. **/
	public synchronized PropertiesPlus getProperties() {
		if (properties == null) {
			properties = new PropertiesPlus();
			// read properties from the file "mondrian.properties", if it
			// exists
			File file = new File("mondrian.properties");
			if (file.exists()) {
				try {
					properties.load(new FileInputStream(file));
				} catch (IOException e) {
					throw Util.newInternal(e, "while reading from " + file);
				}
			}
			// copy in all system properties which start with "mondrian."
			for (Enumeration keys = System.getProperties().keys();
					keys.hasMoreElements(); ) {
				String key = (String) keys.nextElement();
				String value = System.getProperty(key);
				if (key.startsWith("mondrian.")) {
				    properties.setProperty(key, value);
				}
			}
		}
		return properties;
	}

	/** Returns a connection to the FoodMart database. **/
	public synchronized Connection getFoodMartConnection() {
		if (foodMartConnection == null) {
			foodMartConnection = DriverManager.getConnection(
					foodMartConnectString, null, false);
		}
		return foodMartConnection;
	}

	/** Executes a query against the FoodMart database. **/
	public Result executeFoodMart(String queryString) {
		Connection connection = getFoodMartConnection();
		Query query = connection.parseQuery(queryString);
		Result result = connection.execute(query);
		return result;
	}

	/** Executes a query against the FoodMart database, and returns the
	 * exception, or <code>null</code> if there was no exception. **/
	public Throwable executeFoodMartCatch(String queryString) {
		try {
			Result result = executeFoodMart(queryString);
			mondrian.olap.Util.discard(result);
		} catch (Throwable e) {
			return e;
		}
		return null;
	}

	/** Returns the output writer. **/
	public PrintWriter getWriter() {
		return pw;
	}

	/**
	 * <code>PropertiesPlus</code> adds a couple of convenience methods to {@link
	 * Properties}.
	 **/
	public static class PropertiesPlus extends Properties {
		/**
		 * Retrieves an integer property. Returns -1 if the property is not
		 * found, or if its value is not an integer.
		 */
		public int getIntProperty(String key) {
			String value = getProperty(key);
			if (value == null) {
				return -1;
			}
			int i = Integer.valueOf(value).intValue();
			return i;
		}
		/**
		 * Retrieves a boolean property. Returns <code>true</code> if the
		 * property exists, and its value is <code>1</code>, <code>true</code>
		 * or <code>yes</code>; returns <code>false</code> otherwise.
		 */
		public boolean getBooleanProperty(String key) {
			String value = getProperty(key);
			if (value == null) {
				return false;
			}
			return value.equalsIgnoreCase("1") ||
				value.equalsIgnoreCase("true") ||
				value.equalsIgnoreCase("yes");
		}
	}
}

// End TestContext.java
