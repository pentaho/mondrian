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

/**
 * Implementation of {@link mondrian.spi.CatalogLocator} that returns
 * the path unchanged.
 *
 * @author Julian Hyde
 */
public class IdentityCatalogLocator implements CatalogLocator {
    public String locate(String catalogPath) {
        return catalogPath;
    }
}

// End IdentityCatalogLocator.java
