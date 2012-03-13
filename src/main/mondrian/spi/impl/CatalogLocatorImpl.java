/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2006 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.spi.CatalogLocator;

/**
 * CatalogLocator which returns the catalog URI unchanged.
 *
 * @author jhyde
 * @since Dec 22, 2005
 */
public class CatalogLocatorImpl implements CatalogLocator {
    public static final CatalogLocator INSTANCE = new CatalogLocatorImpl();

    public String locate(String catalogPath) {
        return catalogPath;
    }
}

// End CatalogLocatorImpl.java
