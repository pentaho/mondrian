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
