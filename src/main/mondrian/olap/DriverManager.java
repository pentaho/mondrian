/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 15 January, 2002
*/

package mondrian.olap;
import mondrian.rolap.RolapConnection;
import mondrian.resource.MondrianResource;

import javax.servlet.ServletContext;
import javax.sql.DataSource;

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

    public DriverManager() {
    }

    /**
     * Creates a connection to a Mondrian OLAP Server.
     *
     * @param connectString Connect string of the form
     *   'property=value;property=value;...'.
     *   See {@link Util#parseConnectString} for more details of the format.
     *   See {@link mondrian.rolap.RolapConnectionProperties} for a list of
     *   allowed properties.
     * @param servletContext If not null, the <code>catalog</code> is read
     *   relative to the WAR file of this servlet.
     * @param fresh If <code>true</code>, a new connection is created;
     *   if <code>false</code>, the connection may come from a connection pool.
     * @return A {@link Connection}
     * @post return != null
     */
    public static Connection getConnection(String connectString,
                                           ServletContext servletContext,
                                           boolean fresh) {
        Util.PropertyList properties = Util.parseConnectString(connectString);
        return getConnection(properties, servletContext, fresh);
    }

    private static void fixup(Util.PropertyList connectionProperties,
                              ServletContext servletContext) {
        String catalog = connectionProperties.get("catalog");
        // If the catalog is an absolute path, it refers to a resource inside
        // our WAR file, so replace the URL.
        if (catalog != null && catalog.startsWith("/")) {
            try {
                URL url = servletContext.getResource(catalog);
                if (url == null) {
                    // The catalog does not exist, but construct a feasible
                    // URL so that the error message makes sense.
                    url = servletContext.getResource("/");
                    url = new URL(url.getProtocol(), url.getHost(),
                            url.getPort(), url.getFile() + catalog.substring(1));
                }
                if (url != null) {
                    catalog = url.toString();
                    connectionProperties.put("catalog", catalog);
                }
            } catch (MalformedURLException e) {
                // Ignore the error
            }
        }
    }

    private static Connection getAdomdConnection(String connectString,
                                                 boolean fresh) {
        try {
            Class clazz = Class.forName("Broadbase.mdx.adomd.AdomdConnection");
            try {
                String sCatalog = null;
                Constructor constructor = clazz.getConstructor(
                    new Class[] {
                        String.class,
                        String.class,
                        Boolean.TYPE
                    }
                );
                return (Connection) constructor.newInstance(
                    new Object[] {
                        connectString,
                        sCatalog,
                        (fresh) ? Boolean.TRUE : Boolean.FALSE
                    }
                );
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e, "while creating " + clazz);
            } catch (NoSuchMethodException e) {
                throw Util.newInternal(e, "while creating " + clazz);
            } catch (InstantiationException e) {
                throw Util.newInternal(e, "while creating " + clazz);
            } catch (InvocationTargetException e) {
                throw Util.newInternal(e, "while creating " + clazz);
            }
        } catch (ClassNotFoundException e) {
            throw Util.newInternal(e, "while connecting to " + connectString);
        }
    }

    /**
     * Creates a connection to a Mondrian OLAP Server.
     *
     * @deprecated Use {@link #getConnection(Util.PropertyList,ServletContext,boolean)}
     */
    public static Connection getConnection(
            Util.PropertyList properties,
            boolean fresh) {
        return getConnection(properties, null, fresh);
    }

    /**
     * Creates a connection to a Mondrian OLAP Server.
     *
     * @param properties Collection of properties which define the location
     *   of the connection.
     *   See {@link RolapConnection} for a list of allowed properties.
     * @param servletContext If not null, the <code>catalog</code> is read
     *   relative to the WAR file of this servlet.
     * @param fresh If <code>true</code>, a new connection is created;
     *   if <code>false</code>, the connection may come from a connection pool.
     * @return A {@link Connection}
     * @post return != null
     */
    public static Connection getConnection(Util.PropertyList properties,
                                           ServletContext servletContext,
                                           boolean fresh) {
        return getConnection(properties, servletContext, null, fresh);
    }
    /**
     * Creates a connection to a Mondrian OLAP Server.
     *
     * @param properties Collection of properties which define the location
     *   of the connection.
     *   See {@link RolapConnection} for a list of allowed properties.
     * @param servletContext If not null, the <code>catalog</code> is read
     *   relative to the WAR file of this servlet.
     * @param dataSource - if not null an external DataSource to be used
     *        by Mondrian
     * @param fresh If <code>true</code>, a new connection is created;
     *   if <code>false</code>, the connection may come from a connection pool.
     * @return A {@link Connection}
     * @post return != null
     */
    public static Connection getConnection(Util.PropertyList properties,
                                           ServletContext servletContext,
                                           DataSource dataSource,
                                           boolean fresh) {
        String provider = properties.get("PROVIDER");
        if (!provider.equalsIgnoreCase("mondrian")) {
            String connectString = properties.toString();
            return getAdomdConnection(connectString, fresh);
        }
        if (servletContext != null) {
            MondrianProperties.instance().populate(servletContext);
            fixup(properties, servletContext);
        }
        return new RolapConnection(properties, dataSource);
    }
}

// End DriverManager.java
