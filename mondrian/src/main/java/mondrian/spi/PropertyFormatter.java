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

import mondrian.olap.Member;

/**
 * SPI to redefine a member property display string.
 */
public interface PropertyFormatter {
    String formatProperty(
        Member member, String propertyName, Object propertyValue);
}

// End PropertyFormatter.java
