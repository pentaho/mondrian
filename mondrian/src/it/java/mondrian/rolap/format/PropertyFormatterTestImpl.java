/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2016-2017 Hitachi Vantara.
// All Rights Reserved.
*/
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