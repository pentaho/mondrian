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

package mondrian.olap;

/**
 * An SPI to redefine the caption displayed for members.
 *
 * @deprecated Use {@link mondrian.spi.MemberFormatter}. This interface
 * exists for temporary backwards compatibility and will be removed
 * in mondrian-4.0.
 *
 * @author hhaas
 * @since 6 October, 2004
 */
public interface MemberFormatter extends mondrian.spi.MemberFormatter {
}

// End MemberFormatter.java
