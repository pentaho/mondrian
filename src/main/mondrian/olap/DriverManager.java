/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 15 January, 2002
*/

package mondrian.olap;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.CatalogLocatorImpl;

import javax.sql.DataSource;

/**
 * The basic service for managing a set of OLAP drivers.
 *
 * @author jhyde
 * @since 15 January, 2002
 * @version $Id$
 */
public class DriverManager {

    public DriverManager() {
    }

    /**
     * Creates a connection to a Mondrian OLAP Engine
     * using a connect string
     * and a catalog locator.
     *
     * @param connectString Connect string of the form
     *   'property=value;property=value;...'.
     *   See {@link mondrian.olap.Util#parseConnectString} for more details of the format.
     *   See {@link mondrian.rolap.RolapConnectionProperties} for a list of
     *   allowed properties.
     * @param locator Use to locate real catalog url by a customized
     *   configuration value. If <code>null</code>, leave the catalog url
     *   unchanged.
     * @return A {@link Connection}
     * @post return != null
     */
    public static Connection getConnection(
        String connectString,
        CatalogLocator locator)
    {
        Util.PropertyList properties = Util.parseConnectString(connectString);
        return getConnection(properties, locator);
    }

    /**
     * Creates a connection to a Mondrian OLAP Engine.
     *
     * @param properties Collection of properties which define the location
     *   of the connection.
     *   See {@link mondrian.rolap.RolapConnection} for a list of allowed properties.
     * @param locator Use to locate real catalog url by a customized
     *   configuration value. If <code>null</code>, leave the catalog url
     *   unchanged.
     * @return A {@link Connection}
     * @post return != null
     */
    public static Connection getConnection(
        Util.PropertyList properties,
        CatalogLocator locator)
    {
        return getConnection(properties, locator, null);
    }

    /**
     * Creates a connection to a Mondrian OLAP Engine
     * using a list of connection properties,
     * a catalog locator,
     * and a JDBC data source.
     *
     * @param properties Collection of properties which define the location
     *   of the connection.
     *   See {@link mondrian.rolap.RolapConnection} for a list of allowed properties.
     * @param locator Use to locate real catalog url by a customized
     *   configuration value. If <code>null</code>, leave the catalog url
     *   unchanged.
     * @param dataSource - if not null an external DataSource to be used
     *        by Mondrian
     * @return A {@link Connection}
     * @post return != null
     */
    public static Connection getConnection(
        Util.PropertyList properties,
        CatalogLocator locator,
        DataSource dataSource)
    {
        String provider = properties.get("PROVIDER", "mondrian");
        if (!provider.equalsIgnoreCase("mondrian")) {
            throw Util.newError("Provider not recognized: " + provider);
        }
        if (locator != null) {
            String catalog = properties.get(
                RolapConnectionProperties.Catalog.name());
            properties.put(
                RolapConnectionProperties.Catalog.name(),
                locator.locate(catalog));
        }
        return new RolapConnection(properties, dataSource);
    }
}

// End DriverManager.java
