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

package mondrian.spi;

/**
 * An SPI to format the cell values.
 *
 * <p>The user registers the CellFormatter's
 * full class name as an attribute of a Measure in the schema file.
 * A single instance of the CellFormatter is created for the Measure.</p>
 *
 * <p>Since CellFormatters will
 * be used to format different Measures in different ways, you must implement
 * the <code>equals</code> and <code>hashCode</code> methods so that
 * the different CellFormatters are not treated as being the same in
 * a {@link java.util.Collection}.
 */
public interface CellFormatter {
    /**
     * Formats a cell value.
     *
     * @param value Cell value
     * @return the formatted value
     */
    String formatCell(Object value);
}

// End CellFormatter.java
