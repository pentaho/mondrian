/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 15 January, 2002
*/

package mondrian.olap;
import java.lang.reflect.*;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * The basic service for managing a set of OLAP drivers
 *
 * @author jhyde
 * @since 15 January, 2002
 * @version $Id$
 **/
public class DriverManager {

	public static Connection getConnection(
		String sConnect, String sCatalog, boolean fresh)
	{
		if (sCatalog != null && !sCatalog.equals("")) {
			if (!sConnect.endsWith(";")) {
				sConnect += ";";
			}
			sConnect += "Catalog=" + sCatalog;
		}
		Util.PropertyList properties = Util.parseConnectString(sConnect);
		String provider = properties.get("PROVIDER");
		if (provider.equalsIgnoreCase("mondrian")) {
			return getConnection(properties);
		}
		try {
			Class clazz = Class.forName("Broadbase.mdx.adomd.AdomdConnection");
			try {
				Constructor constructor = clazz.getConstructor(
					new Class[] {String.class, String.class, Boolean.TYPE});
				return (Connection) constructor.newInstance(new Object[] {
					sConnect, sCatalog, new Boolean(fresh)});
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
			throw Util.getRes().newInternal("while connecting to " + sConnect, e);
		}
	}

	public static Connection getConnection(Util.PropertyList properties) {
		return new mondrian.rolap.RolapConnection(properties);
	}
}

// End DriverManager.java
