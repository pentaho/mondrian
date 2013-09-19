/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.spi.impl;

import mondrian.spi.CatalogLocator;

import java.net.MalformedURLException;
import java.net.URL;
import javax.servlet.ServletContext;

/**
 * Locates a catalog based upon a {@link ServletContext}.<p/>
 *
 * If the catalog URI is an absolute path, it refers to a resource inside our
 * WAR file, so replace the URL.
 *
 * @author Gang Chen, jhyde
 * @since December, 2005
 */
public class ServletContextCatalogLocator implements CatalogLocator {
    private ServletContext servletContext;

    public ServletContextCatalogLocator(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    public String locate(String catalogPath) {
        // If the catalog is an absolute path, it refers to a resource inside
        // our WAR file, so replace the URL.
        if (catalogPath != null && catalogPath.startsWith("/")) {
            try {
                URL url = servletContext.getResource(catalogPath);
                if (url == null) {
                    // The catalogPath does not exist, but construct a feasible
                    // URL so that the error message makes sense.
                    url = servletContext.getResource("/");
                    url = new URL(
                        url.getProtocol(),
                        url.getHost(),
                        url.getPort(),
                        url.getFile() + catalogPath.substring(1));
                }
                catalogPath = url.toString();
            } catch (MalformedURLException ignored) {
            }
        }
        return catalogPath;
    }
}

// End ServletContextCatalogLocator.java
