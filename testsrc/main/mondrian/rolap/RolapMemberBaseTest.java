/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2016-2016 Pentaho Corporation.
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Member;

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit Test for {@link RolapMemberBase}.
 */
public class RolapMemberBaseTest extends TestCase {

    private static final String PROPERTY_NAME = "property";

    // property values and their expected formatted values
    private static final Map<Object, String> VALUES =
            new HashMap<Object, String>() {{
                // string
                put("String", "String");

                // integer
                put(1234567, "1234567");
                put(0, "0");
                put(-0, "0");
                put(-1234567890, "-1234567890");

                // long
                put(1234567L, "1234567");
                put(0L, "0");
                put(-1234567890123456L, "-1234567890123456");
                put(1200000000000000000L, "1200000000000000000");

                // float
                put(1234567f, "1234567");
                put(1234567.0f, "1234567");
                put(123.4567f, "123.4567");
                put(0f, "0");
                put(0.0f, "0");
                put(1.234567e-1f, "0.1234567");
                put(1.234567e-23f, "0.00000000000000000000001234567");
                put(1.234567e20f, "123456700000000000000");

                // double
                put(123.4567, "123.4567");
                put(1.234567e2, "123.4567");
                put(1.234567e25, "12345670000000000000000000");
                put(0.1234567, "0.1234567");
                put(1.234567e-1, "0.1234567");
                put(1200000000000000000.0, "1200000000000000000");
                put(
                    0.00000000000000000001234567,
                    "0.00000000000000000001234567");
                put(1.234567e-20, "0.00000000000000000001234567");
                put(12E2, "1200");
                put(12E20, "1200000000000000000000");
                put(1.2E21, "1200000000000000000000");
                put(1.2E-20, "0.000000000000000000012");
                put(-1.2E-20, "-0.000000000000000000012");
            }};

    private RolapMemberBase rolapMemberBase;

    @Override
    public void setUp() {
        RolapLevel level = mock(RolapLevel.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapDimension dimension = mock(RolapDimension.class);
        RolapProperty property = mock(RolapProperty.class);
        RolapProperty[] properties = {property};
        when(level.getHierarchy()).thenReturn(hierarchy);
        when(hierarchy.getDimension()).thenReturn(dimension);
        when(level.getProperties()).thenReturn(properties);
        when(property.getName()).thenReturn(PROPERTY_NAME);
        rolapMemberBase = new RolapMemberBase(
            mock(RolapMember.class),
            level,
            1,
            null,
            Member.MemberType.REGULAR);
    }

    /**
     * Test for {@link RolapMemberBase#getPropertyFormattedValue(String)}.
     * <p>
     * Given that the property value is a number.
     * </p>
     * When the formatted value is requested, then the output should not contain
     * any unwanted decimal digits due to floating point representation,
     * as well as E notations.
     */
    public void testPropertyValuesFormattingNumber() {
        for (Map.Entry<Object, String> entry : VALUES.entrySet()) {
            rolapMemberBase.setProperty(PROPERTY_NAME, entry.getKey());

            String formattedValue =
                    rolapMemberBase.getPropertyFormattedValue(PROPERTY_NAME);

            assertEquals(
                "Value type: " + entry.getKey().getClass().toString(),
                entry.getValue(), formattedValue);
        }
    }

    /**
     * Test for {@link RolapMemberBase#setCaption(Object)}.
     * <p>
     * Given that the caption column value is a number.
     * </p>
     * When this value is passed to be set as a caption,
     * then it should be properly formatted before, and should not contain
     * any unwanted decimal digits due to floating point representation,
     * as well as E notations.
     */
    public void testCaptionColumnValuesFormattingNumber() {
        for (Map.Entry<Object, String> entry : VALUES.entrySet()) {
            rolapMemberBase.setCaption(entry.getKey());

            String formattedValue =
                    rolapMemberBase.getCaption();

            assertEquals(
                "Value type: " + entry.getKey().getClass().toString(),
                entry.getValue(), formattedValue);
        }
    }
}
// End RolapMemberBaseTest.java