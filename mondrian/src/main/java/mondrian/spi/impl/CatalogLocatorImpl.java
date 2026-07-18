/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



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
