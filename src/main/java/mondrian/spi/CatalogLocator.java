/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Pentaho
// All Rights Reserved.
*/
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
