/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.*;

/**
 * Tests intrinsic member and cell properties as specified in OLE DB for OLAP
 * specification.
 *
 * @author anikitin
 * @since 5 July, 2005
 */
public class PropertiesTest extends FoodMartTestCase {

    public PropertiesTest(String name) {
        super(name);
    }

    /**
     * Tests existence and values of mandatory member properties.
     */
    public void testMandatoryMemberProperties() {
        Cube salesCube = getConnection().getSchema().lookupCube("Sales",true);
        SchemaReader scr = salesCube.getSchemaReader(null);
        Member member = scr.getMemberByUniqueName(new String[] {"Customers", "All Customers", "USA", "CA"}, true);

        String stringPropValue = null;
        Integer intPropValue = null;

        // I'm not sure this property has to store the same value
        // getConnection().getCatalogName() returns.

        // todo:
//        stringPropValue = (String)member.getPropertyValue("CATALOG_NAME");
//        assertEquals(getConnection().getCatalogName(), stringPropValue);

        stringPropValue = (String)member.getPropertyValue("SCHEMA_NAME");
        assertEquals(getConnection().getSchema().getName(), stringPropValue);

        // todo:
//        stringPropValue = (String)member.getPropertyValue("CUBE_NAME");
//        assertEquals(salesCube.getName(), stringPropValue);

        stringPropValue = (String)member.getPropertyValue("DIMENSION_UNIQUE_NAME");
        assertEquals(member.getDimension().getUniqueName(), stringPropValue);

        stringPropValue = (String)member.getPropertyValue("HIERARCHY_UNIQUE_NAME");
        assertEquals(member.getHierarchy().getUniqueName(), stringPropValue);

        // This property works in Mondrian 1.1.5 (due to XMLA support)
        stringPropValue = (String)member.getPropertyValue("LEVEL_UNIQUE_NAME");
        assertEquals(member.getLevel().getUniqueName(), stringPropValue);

        // This property works in Mondrian 1.1.5 (due to XMLA support)
        intPropValue = (Integer)member.getPropertyValue("LEVEL_NUMBER");
        assertEquals(new Integer(member.getLevel().getDepth()), intPropValue);

        // This property works in Mondrian 1.1.5 (due to XMLA support)
        stringPropValue = (String)member.getPropertyValue("MEMBER_UNIQUE_NAME");
        assertEquals(member.getUniqueName(), stringPropValue);

        stringPropValue = (String)member.getPropertyValue("MEMBER_NAME");
        assertEquals(member.getName(), stringPropValue);

        intPropValue = (Integer)member.getPropertyValue("MEMBER_TYPE");
        assertEquals(new Integer(member.getMemberType()), intPropValue);

        stringPropValue = (String)member.getPropertyValue("MEMBER_GUID");
        assertNull(stringPropValue);

        // This property works in Mondrian 1.1.5 (due to XMLA support)
        stringPropValue = (String)member.getPropertyValue("MEMBER_CAPTION");
        assertEquals(member.getCaption(), stringPropValue);

        intPropValue = (Integer)member.getPropertyValue("MEMBER_ORDINAL");
        assertEquals(new Integer(member.getOrdinal()), intPropValue);

//        intPropValue = (Integer)member.getPropertyValue("CHILDREN_CARDINALITY");
//        assertEquals(new Integer(scr.getMemberChildren(member).length), intPropValue);

        intPropValue = (Integer)member.getPropertyValue("PARENT_LEVEL");
        assertEquals(new Integer(member.getParentMember().getLevel().getDepth()), intPropValue);

        stringPropValue = (String)member.getPropertyValue("PARENT_UNIQUE_NAME");
        assertEquals(member.getParentUniqueName(), stringPropValue);

        intPropValue = (Integer)member.getPropertyValue("PARENT_COUNT");
        assertEquals(new Integer(1), intPropValue);

        stringPropValue = (String)member.getPropertyValue("DESCRIPTION");
        assertEquals(member.getDescription(), stringPropValue);
    }

    /**
     * Tests the ability of MDX parser to pass requested member properties
     * to Result object.
     *
     * <p>Unfortunately this feature is not implemented in Mondrian 1.2.
     * Test disabled.
     */
    public void _testPropertiesMDX() {
        Result result = executeQuery("SELECT {[Customers].[All Customers].[USA].[CA]} DIMENSION PROPERTIES "+nl+
                        " CATALOG_NAME, SCHEMA_NAME, CUBE_NAME, DIMENSION_UNIQUE_NAME, " + nl +
                        " HIERARCHY_UNIQUE_NAME, LEVEL_UNIQUE_NAME, LEVEL_NUMBER, MEMBER_UNIQUE_NAME, " + nl +
                        " MEMBER_NAME, MEMBER_TYPE, MEMBER_GUID, MEMBER_CAPTION, MEMBER_ORDINAL, CHILDREN_CARDINALITY," + nl +
                        " PARENT_LEVEL, PARENT_UNIQUE_NAME, PARENT_COUNT, DESCRIPTION ON COLUMNS" + nl +
                        "FROM [Sales]");
        Axis[] axes = result.getAxes();
        Object[] axesProperties = null;
        // Commented out because axis properties are not implemented as of 1.2.
//        axesProperties = axes[0].properties;
        assertEquals(axesProperties[0],"CATALOG_NAME");
        assertEquals(axesProperties[1],"SCHEMA_NAME");
        assertEquals(axesProperties[2],"CUBE_NAME");
        assertEquals(axesProperties[3],"DIMENSION_UNIQUE_NAME");
        assertEquals(axesProperties[4],"HIERARCHY_UNIQUE_NAME");
        assertEquals(axesProperties[5],"LEVEL_UNIQUE_NAME");
        assertEquals(axesProperties[6],"LEVEL_NUMBER");
        assertEquals(axesProperties[7],"MEMBER_UNIQUE_NAME");
        assertEquals(axesProperties[8],"MEMBER_NAME");
        assertEquals(axesProperties[9],"MEMBER_TYPE");
        assertEquals(axesProperties[10],"MEMBER_GUID");
        assertEquals(axesProperties[11],"MEMBER_CAPTION");
        assertEquals(axesProperties[12],"MEMBER_ORDINAL");
        assertEquals(axesProperties[13],"CHILDREN_CARDINALITY");
        assertEquals(axesProperties[14],"PARENT_LEVEL");
        assertEquals(axesProperties[15],"PARENT_UNIQUE_NAME");
        assertEquals(axesProperties[16],"PARENT_COUNT");
        assertEquals(axesProperties[17],"DESCRIPTION");
    }

    public void testMandatoryCellProperties() {
        Connection connection = getConnection();
        Query salesCube = connection.parseQuery(
                "select " + nl +
                " {[Measures].[Store Sales], [Measures].[Unit Sales]} on columns, " + nl +
                " {[Gender].members} on rows " + nl +
                "from [Sales]");
        Result result = connection.execute(salesCube);
        int x = 1;
        int y = 2;
        Cell cell = result.getCell(new int[] {x, y});

        assertNull(cell.getPropertyValue("BACK_COLOR"));
        assertNull(cell.getPropertyValue("CELL_EVALUATION_LIST"));
        assertEquals(new Integer(y * 2 + x),
                cell.getPropertyValue("CELL_ORDINAL"));
        assertNull(cell.getPropertyValue("FORE_COLOR"));
        assertNull(cell.getPropertyValue("FONT_NAME"));
        assertNull(cell.getPropertyValue("FONT_SIZE"));
        assertEquals(new Integer(0), cell.getPropertyValue("FONT_FLAGS"));
        assertEquals("Standard", cell.getPropertyValue("FORMAT_STRING"));
        assertEquals("135,215", cell.getPropertyValue("FORMATTED_VALUE"));
        assertNull(cell.getPropertyValue("NON_EMPTY_BEHAVIOR"));
        assertEquals(new Integer(0), cell.getPropertyValue("SOLVE_ORDER"));
        assertEquals(135215.0, ((Number) cell.getPropertyValue("VALUE")).doubleValue(), 0.1);

    }
}

// End PropertiesTest.java
