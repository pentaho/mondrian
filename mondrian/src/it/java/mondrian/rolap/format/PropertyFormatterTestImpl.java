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


package mondrian.rolap.format;

import mondrian.olap.Member;
import mondrian.spi.PropertyFormatter;

class PropertyFormatterTestImpl implements PropertyFormatter {

    public PropertyFormatterTestImpl() {
    }

    @Override
    public String formatProperty(
        Member member,
        String propertyName,
        Object propertyValue)
    {
        return null;
    }
}
// End PropertyFormatterTestImpl.java