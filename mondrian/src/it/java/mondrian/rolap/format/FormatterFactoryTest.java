/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.rolap.format;

import mondrian.spi.*;

import junit.framework.TestCase;

public class FormatterFactoryTest extends TestCase {

    private FormatterFactory factory = FormatterFactory.instance();

    /**
     * Given that custom formatter class name is specified.
     * <p>
     * When formatter creating is requested,
     * factory should instantiate an object of specified class.
     * </p>
     */
    public void testShouldCreateFormatterByClassName() {
        FormatterCreateContext cellFormatterContext =
            new FormatterCreateContext.Builder("name")
                .formatterAttr("mondrian.rolap.format.CellFormatterTestImpl")
                .build();
        FormatterCreateContext memberFormatterContext =
            new FormatterCreateContext.Builder("name")
                .formatterAttr("mondrian.rolap.format.MemberFormatterTestImpl")
                .build();
        FormatterCreateContext propertyFormatterContext =
            new FormatterCreateContext.Builder("name")
                .formatterAttr(
                    "mondrian.rolap.format.PropertyFormatterTestImpl")
                .build();

        CellFormatter cellFormatter =
            factory.createCellFormatter(cellFormatterContext);
        MemberFormatter memberFormatter =
            factory.createRolapMemberFormatter(memberFormatterContext);
        PropertyFormatter propertyFormatter =
            factory.createPropertyFormatter(propertyFormatterContext);

        assertNotNull(cellFormatter);
        assertNotNull(memberFormatter);
        assertNotNull(propertyFormatter);
        assertTrue(cellFormatter instanceof CellFormatterTestImpl);
        assertTrue(memberFormatter instanceof MemberFormatterTestImpl);
        assertTrue(propertyFormatter instanceof PropertyFormatterTestImpl);
    }

    /**
     * Given that custom formatter script is specified.
     * <p>
     * When formatter creating is requested,
     * factory should instantiate an object of script based implementation.
     * </p>
     */
    public void testShouldCreateFormatterByScript() {
        FormatterCreateContext context =
            new FormatterCreateContext.Builder("name")
                .script("return null;", "JavaScript")
                .build();

        CellFormatter cellFormatter =
            factory.createCellFormatter(context);
        MemberFormatter memberFormatter =
            factory.createRolapMemberFormatter(context);
        PropertyFormatter propertyFormatter =
            factory.createPropertyFormatter(context);

        assertNotNull(cellFormatter);
        assertNotNull(memberFormatter);
        assertNotNull(propertyFormatter);
    }

    /**
     * Given that custom formatter's both class name and script are specified.
     * <p>
     * When formatter creating is requested,
     * factory should instantiate an object of <b>specified class</b>.
     * </p>
     */
    public void testShouldCreateFormatterByClassNameIfBothSpecified() {
        FormatterCreateContext cellFormatterContext =
            new FormatterCreateContext.Builder("name")
                .formatterAttr("mondrian.rolap.format.CellFormatterTestImpl")
                .script("return null;", "JavaScript")
                .build();
        FormatterCreateContext memberFormatterContext =
            new FormatterCreateContext.Builder("name")
                .formatterAttr("mondrian.rolap.format.MemberFormatterTestImpl")
                .script("return null;", "JavaScript")
                .build();
        FormatterCreateContext propertyFormatterContext =
            new FormatterCreateContext.Builder("name")
                .formatterAttr(
                    "mondrian.rolap.format.PropertyFormatterTestImpl")
                .script("return null;", "JavaScript")
                .build();

        CellFormatter cellFormatter =
            factory.createCellFormatter(cellFormatterContext);
        MemberFormatter memberFormatter =
            factory.createRolapMemberFormatter(memberFormatterContext);
        PropertyFormatter propertyFormatter =
            factory.createPropertyFormatter(propertyFormatterContext);

        assertNotNull(cellFormatter);
        assertNotNull(memberFormatter);
        assertNotNull(propertyFormatter);
        assertTrue(cellFormatter instanceof CellFormatterTestImpl);
        assertTrue(memberFormatter instanceof MemberFormatterTestImpl);
        assertTrue(propertyFormatter instanceof PropertyFormatterTestImpl);
    }

    /**
     * Given that no custom formatter is specified.
     * <p>
     * When formatter creating is requested,
     * factory should return NULL for:
     * <li>{@link CellFormatter}</li>
     * </p>
     */
    public void testShouldReturnNullIfEmptyContext() {
        FormatterCreateContext context =
            new FormatterCreateContext.Builder("name").build();

        CellFormatter cellFormatter =
            factory.createCellFormatter(context);

        assertNull(cellFormatter);
    }

    /**
     * Given that no custom formatter is specified.
     * <p>
     * When formatter creating is requested,
     * factory should return a default implementation
     * for:
     * <li>{@link PropertyFormatter}</li>
     * <li>{@link MemberFormatter}</li>
     * </p>
     */
    public void testShouldReturnDefaultFormatterIfEmptyContext() {
        FormatterCreateContext context =
            new FormatterCreateContext.Builder("name").build();

        PropertyFormatter propertyFormatter =
            factory.createPropertyFormatter(context);
        MemberFormatter memberFormatter =
            factory.createRolapMemberFormatter(context);

        assertNotNull(propertyFormatter);
        assertNotNull(memberFormatter);
        assertTrue(propertyFormatter instanceof PropertyFormatterAdapter);
        assertTrue(memberFormatter instanceof DefaultRolapMemberFormatter);
    }
}
// End FormatterFactoryTest.java