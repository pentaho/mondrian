/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import mondrian.spi.CatalogLocator;

/**
 * Implementation of {@link mondrian.spi.CatalogLocator} that returns
 * the path unchanged.
 *
 * @version $Id$
 * @author Julian Hyde
 */
public class IdentityCatalogLocator implements CatalogLocator {
    public String locate(String catalogPath) {
        return catalogPath;
    }
}

// End IdentityCatalogLocator.java
