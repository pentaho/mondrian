/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
