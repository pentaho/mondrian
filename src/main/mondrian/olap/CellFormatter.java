/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * An SPI to format the cell value to be displayed.
 *
 * @deprecated Use {@link mondrian.spi.CellFormatter}. This interface
 * exists for temporary backwards compatibility and will be removed
 * in mondrian-4.0.
 */
public interface CellFormatter extends mondrian.spi.CellFormatter {
}

// End CellFormatter.java
