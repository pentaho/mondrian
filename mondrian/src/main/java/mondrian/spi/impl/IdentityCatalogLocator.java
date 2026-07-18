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
