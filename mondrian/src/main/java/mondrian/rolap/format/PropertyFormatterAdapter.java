/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2016-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
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