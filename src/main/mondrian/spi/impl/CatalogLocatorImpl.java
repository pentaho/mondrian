/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import mondrian.spi.CatalogLocator;

/**
 * CatalogLocator which returns the catalog URI unchanged.
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 22, 2005
 */
public class CatalogLocatorImpl implements CatalogLocator {
    public String locate(String catalogPath) {
        return catalogPath;
    }
}

// End CatalogLocatorImpl.java
