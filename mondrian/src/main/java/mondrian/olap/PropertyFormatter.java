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



package mondrian.olap;

/**
 * SPI to redefine a member property display string.
 *
 * @deprecated Use {@link mondrian.spi.PropertyFormatter}. This interface
 * exists for temporary backwards compatibility and will be removed
 * in mondrian-4.0.
 */
public interface PropertyFormatter extends mondrian.spi.PropertyFormatter {
}

// End PropertyFormatter.java
