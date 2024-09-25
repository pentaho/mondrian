/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2016-2024 Hitachi Vantara.
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Member;

import mondrian.olap.Property;
import mondrian.spi.MemberFormatter;
import mondrian.spi.PropertyFormatter;

import junit.framework.TestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit Test for {@link RolapMemberBase}.
 */
public class RolapMemberBaseTest extends TestCase {

    private static final String PROPERTY_NAME_1 = "property1";
    private static final String PROPERTY_NAME_2 = "property2";
    private static final String PROPERTY_NAME_3 = "property3";
    private static final Object PROPERTY_VALUE_TO_FORMAT =
            "propertyValueToFormat";
    private static final String FORMATTED_PROPERTY_VALUE =
            "formattedPropertyValue";
    private static final Object MEMBER_NAME = "memberName";
    private static final String FORMATTED_CAPTION = "formattedCaption";
    private static final String ROLAP_FORMATTED_CAPTION =
            "rolapFormattedCaption";

    private RolapMemberBase rolapMemberBase;

    // mocks
    private Object memberKey;
    private RolapLevel level;
    private PropertyFormatter propertyFormatter;

    @Override
    public void setUp() {
        level = mock(RolapLevel.class);
        propertyFormatter = mock(PropertyFormatter.class);
        memberKey = Integer.MAX_VALUE;
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapDimension dimension = mock(RolapDimension.class);

        when(level.getHierarchy()).thenReturn(hierarchy);
        when(hierarchy.getDimension()).thenReturn(dimension);

        rolapMemberBase = new RolapMemberBase(
            mock(RolapMember.class),
            level,
            memberKey,
            null,
            Member.MemberType.REGULAR);
    }

    /**
     * <p>
     * Given rolap member with properties.
     * </p>
     * When the property formatted value is requested,
     * then property formatter should be used to return the value.
     */
    public void testShouldUsePropertyFormatterWhenPropertyValuesAreRequested() {
        RolapProperty property1 = mock(RolapProperty.class);
        RolapProperty property2 = mock(RolapProperty.class);
        when(property1.getName()).thenReturn(PROPERTY_NAME_1);
        when(property2.getName()).thenReturn(PROPERTY_NAME_2);
        when(property1.getFormatter()).thenReturn(propertyFormatter);
        RolapProperty[] properties = {property1, property2};
        when(level.getProperties()).thenReturn(properties);
        when(propertyFormatter.formatProperty(
            any(Member.class),
            anyString(),
            eq(PROPERTY_VALUE_TO_FORMAT)))
            .thenReturn(FORMATTED_PROPERTY_VALUE);
        rolapMemberBase.setProperty(PROPERTY_NAME_1, PROPERTY_VALUE_TO_FORMAT);
        rolapMemberBase.setProperty(PROPERTY_NAME_2, PROPERTY_VALUE_TO_FORMAT);

        String formatted1 =
                rolapMemberBase.getPropertyFormattedValue(PROPERTY_NAME_1);
        String formatted2 =
                rolapMemberBase.getPropertyFormattedValue(PROPERTY_NAME_2);
        String formatted3 =
                rolapMemberBase.getPropertyFormattedValue(PROPERTY_NAME_3);

        assertEquals(FORMATTED_PROPERTY_VALUE, formatted1); // formatted
        assertEquals(PROPERTY_VALUE_TO_FORMAT, formatted2); // unformatted
        assertEquals(null, formatted3);                     // not found
    }

    /**
     * <p>
     * Given rolap member.
     * </p>
     * When caption is requested,
     * then member formatter should be used
     * to return the formatted caption value.
     */
    public void testShouldUseMemberFormatterForCaption() {
        MemberFormatter memberFormatter = mock(MemberFormatter.class);
        when(level.getMemberFormatter()).thenReturn(memberFormatter);
        when(memberFormatter.formatMember(rolapMemberBase))
            .thenReturn(FORMATTED_CAPTION);

        String caption = rolapMemberBase.getCaption();

        assertEquals(FORMATTED_CAPTION, caption);
    }

    /**
     * <p>
     * Given rolap member with no member formatter
     * (This shouldn't happen actually, but just in case).
     * </p>
     * When caption is requested,
     * then member key should be returned.
     */
    public void testShouldNotFailIfMemberFormatterIsNotPresent() {
        String caption = rolapMemberBase.getCaption();

        assertEquals(String.valueOf(Integer.MAX_VALUE), caption);
    }

    /**
     * <p>
     * Given rolap member with neither caption value nor name specified.
     * </p>
     * When caption raw value is requested,
     * then member key should be returned.
     */
    public void testShouldReturnMemberKeyIfNoCaptionValueAndNoNamePresent() {
        Object captionValue = rolapMemberBase.getCaptionValue();

        assertNotNull(captionValue);
        assertEquals(memberKey, captionValue);
    }

    /**
     * <p>
     * Given rolap member with no caption value, but with name specified.
     * </p>
     * When caption raw value is requested,
     * then member name should be returned.
     */
    public void testShouldReturnMemberNameIfCaptionValueIsNotPresent() {
        rolapMemberBase.setProperty(Property.NAME.name, MEMBER_NAME);

        Object captionValue = rolapMemberBase.getCaptionValue();

        assertNotNull(captionValue);
        assertEquals(MEMBER_NAME, captionValue);
    }

    /**
     * <p>
     * Given rolap member with caption value specified.
     * </p>
     * When caption raw value is requested,
     * then the caption value should be returned.
     */
    public void testShouldReturnCaptionValueIfPresent() {
        rolapMemberBase.setCaptionValue(Integer.MIN_VALUE);

        Object captionValue = rolapMemberBase.getCaptionValue();

        assertNotNull(captionValue);
        assertEquals(Integer.MIN_VALUE, captionValue);
    }
}
// End RolapMemberBaseTest.java