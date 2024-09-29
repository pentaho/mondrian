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

import mondrian.olap.MondrianDef;

import junit.framework.TestCase;

public class FormatterCreateContextTest extends TestCase {

    public void testElementDataShouldSupersedeAttributeData() {
        MondrianDef.ElementFormatter elementData =
            new MondrianDef.PropertyFormatter();
        elementData.className = "elementClassName";

        FormatterCreateContext context1 =
            new FormatterCreateContext.Builder("elementName")
            .formatterDef(elementData)
            .formatterAttr("attributeClassName")
            .build();
        FormatterCreateContext context2 =
            new FormatterCreateContext.Builder("elementName")
                .formatterAttr("attributeClassName")
                .build();
        FormatterCreateContext context3 =
            new FormatterCreateContext.Builder("elementName")
                .formatterDef(null)
                .formatterAttr("attributeClassName")
                .build();

        assertEquals("elementClassName", context1.getFormatterClassName());
        assertEquals("attributeClassName", context2.getFormatterClassName());
        assertEquals("attributeClassName", context3.getFormatterClassName());
    }
}
// End FormatterCreateContextTest.java