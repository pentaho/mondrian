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



package mondrian.spi;

/**
 * Abstract layer for locating catalog schema content.
 *
 * @author Gang Chen
 * @since December, 2005
 */
public interface CatalogLocator {

    /**
     * @return URL complied string representation.
     */
    String locate(String catalogPath);

}

// End CatalogLocator.java
