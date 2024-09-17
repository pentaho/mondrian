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
package mondrian.rolap.format;

import mondrian.olap.Member;
import mondrian.spi.PropertyFormatter;

/**
 * Adapter to comply SPI {@link PropertyFormatter}
 * using the default formatter implementation.
 */
class PropertyFormatterAdapter implements PropertyFormatter {
    private DefaultFormatter numberFormatter;

    PropertyFormatterAdapter(DefaultFormatter numberFormatter) {
        this.numberFormatter = numberFormatter;
    }

    @Override
    public String formatProperty(
        Member member,
        String propertyName,
        Object propertyValue)
    {
        return numberFormatter.format(propertyValue);
    }
}
// End PropertyFormatterAdapter.java