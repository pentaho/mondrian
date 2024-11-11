/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
