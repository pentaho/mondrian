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
import mondrian.spi.impl.ServletContextCatalogLocator;

import javax.servlet.ServletContext;
import javax.sql.DataSource;

/**
 * The basic service for managing a set of OLAP drivers.
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
    public static Connection getConnection(
            String connectString,
            ServletContext servletContext,
            boolean fresh) {
        Util.PropertyList properties = Util.parseConnectString(connectString);
        return getConnection(properties, servletContext, fresh);
    }

    private static void fixup(
            Util.PropertyList connectionProperties,
            ServletContext servletContext) {
        String catalog = connectionProperties.get("catalog");
        if (servletContext != null) {
            final ServletContextCatalogLocator locator =
                    new ServletContextCatalogLocator(servletContext);
            final String newCatalog = locator.locate(catalog);
            if (!newCatalog.equals(catalog)) {
                connectionProperties.put("catalog", catalog);
            }
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
    public static Connection getConnection(
            Util.PropertyList properties,
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
    public static Connection getConnection(
            Util.PropertyList properties,
            ServletContext servletContext,
            DataSource dataSource,
            boolean fresh) {
        String provider = properties.get("PROVIDER");
        if (!provider.equalsIgnoreCase("mondrian")) {
            throw Util.newError("Provider not recognized: " + provider);
        }
        if (servletContext != null) {
            MondrianProperties.instance().populate(servletContext);
            fixup(properties, servletContext);
        }
        return new RolapConnection(properties, dataSource);
    }
}

// End DriverManager.java
