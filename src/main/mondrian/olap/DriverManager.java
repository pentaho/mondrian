/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 15 January, 2002
*/

package mondrian.olap;
import mondrian.rolap.RolapConnection;

import javax.servlet.ServletContext;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * The basic service for managing a set of OLAP drivers
 *
 * @author jhyde
 * @since 15 January, 2002
 * @version $Id$
 **/
public class DriverManager {

	/**
	 * Creates a connection to a Mondrian OLAP Server.
	 *
	 * @param connectString Connect string of the form
	 *   'property=value;property=value;...'.
	 *   See {@link Util#parseConnectString} for more details of the format.
	 *   See {@link RolapConnection} for a list of allowed properties.
	 * @param servletContext If not null, the <code>catalog</code> is read
	 *   relative to the WAR file of this servlet.
	 * @param fresh If <code>true</code>, a new connection is created;
	 *   if <code>false</code>, the connection may come from a connection pool.
	 * @return A {@link Connection}
	 * @post return != null
	 */
	public static Connection getConnection(
			String connectString, ServletContext servletContext, boolean fresh) {
		Util.PropertyList properties = Util.parseConnectString(connectString);
		if (servletContext != null) {
			fixup(properties, servletContext);
		}
		return getConnection(properties, fresh);
	}

	private static void fixup(Util.PropertyList connectionProperties, ServletContext servletContext) {
		String catalog = connectionProperties.get("catalog");
		// If the catalog is an absolute path, it refers to a resource inside
		// our WAR file, so replace the URL.
		if (catalog != null && catalog.startsWith("/")) {
			try {
				final URL url = servletContext.getResource(catalog);
				if (url != null) {
					catalog = url.toString();
					connectionProperties.put("catalog", catalog);
				}
			} catch (MalformedURLException e) {
				// Ignore the error
			}
		}
	}

	private static Connection getAdomdConnection(String connectString, boolean fresh) {
		try {
			Class clazz = Class.forName("Broadbase.mdx.adomd.AdomdConnection");
			try {
				String sCatalog = null;
				Constructor constructor = clazz.getConstructor(
					new Class[] {String.class, String.class, Boolean.TYPE});
				return (Connection) constructor.newInstance(new Object[] {
					connectString, sCatalog, new Boolean(fresh)});
			} catch (IllegalAccessException e) {
				throw Util.getRes().newInternal("while creating " + clazz, e);
			} catch (NoSuchMethodException e) {
				throw Util.getRes().newInternal("while creating " + clazz, e);
			} catch (InstantiationException e) {
				throw Util.getRes().newInternal("while creating " + clazz, e);
			} catch (InvocationTargetException e) {
				throw Util.getRes().newInternal("while creating " + clazz, e);
			}
		} catch (ClassNotFoundException e) {
			throw Util.getRes().newInternal("while connecting to " + connectString, e);
		}
	}

	/**
	 * Creates a connection to a Mondrian OLAP Server.
	 *
	 * The following properties are
	 *
	 * @param properties Collection of properties which define the location
	 *   of the connection.
	 *   See {@link RolapConnection} for a list of allowed properties.
	 * @param fresh If <code>true</code>, a new connection is created;
	 *   if <code>false</code>, the connection may come from a connection pool.
	 * @return A {@link Connection}
	 * @post return != null
	 */
	public static Connection getConnection(Util.PropertyList properties, boolean fresh) {
		String provider = properties.get("PROVIDER");
		if (!provider.equalsIgnoreCase("mondrian")) {
			String connectString = properties.toString();
			return getAdomdConnection(connectString, fresh);
		}
		return new RolapConnection(properties);
	}
}

// End DriverManager.java
