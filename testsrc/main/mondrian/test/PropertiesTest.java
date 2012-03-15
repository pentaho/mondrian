/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
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
        Cube salesCube = getConnection().getSchema().lookupCube("Sales", true);
        SchemaReader scr = salesCube.getSchemaReader(null).withLocus();
        Member member =
            scr.getMemberByUniqueName(
                Id.Segment.toList("Customers", "All Customers", "USA", "CA"),
                true);
        final boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();

        String stringPropValue;
        Integer intPropValue;

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

        stringPropValue =
            (String)member.getPropertyValue("DIMENSION_UNIQUE_NAME");
        assertEquals(member.getDimension().getUniqueName(), stringPropValue);

        // Case sensitivity.
        stringPropValue = (String)member.getPropertyValue(
            "dimension_unique_name", caseSensitive);
        if (caseSensitive) {
            assertNull(stringPropValue);
        } else {
            assertEquals(
                member.getDimension().getUniqueName(),
                stringPropValue);
        }

        // Non-existent property.
        stringPropValue =
            (String)member.getPropertyValue("DIMENSION_UNIQUE_NAME_XXXX");
        assertNull(stringPropValue);

        // Leading spaces.
        stringPropValue =
            (String)member.getPropertyValue(" DIMENSION_UNIQUE_NAME");
        assertNull(stringPropValue);

        // Trailing spaces.
        stringPropValue =
            (String)member.getPropertyValue("DIMENSION_UNIQUE_NAME  ");
        assertNull(stringPropValue);

        stringPropValue =
            (String)member.getPropertyValue("HIERARCHY_UNIQUE_NAME");
        assertEquals(member.getHierarchy().getUniqueName(), stringPropValue);

        // This property works in Mondrian 1.1.5 (due to XMLA support)
        stringPropValue = (String)member.getPropertyValue("LEVEL_UNIQUE_NAME");
        assertEquals(member.getLevel().getUniqueName(), stringPropValue);

        // This property works in Mondrian 1.1.5 (due to XMLA support)
        intPropValue = (Integer)member.getPropertyValue("LEVEL_NUMBER");
        assertEquals(
            Integer.valueOf(member.getLevel().getDepth()),
            intPropValue);

        // This property works in Mondrian 1.1.5 (due to XMLA support)
        stringPropValue = (String)member.getPropertyValue("MEMBER_UNIQUE_NAME");
        assertEquals(member.getUniqueName(), stringPropValue);

        stringPropValue = (String)member.getPropertyValue("MEMBER_NAME");
        assertEquals(member.getName(), stringPropValue);

        intPropValue = (Integer)member.getPropertyValue("MEMBER_TYPE");
        assertEquals(
            Integer.valueOf(member.getMemberType().ordinal()),
            intPropValue);

        stringPropValue = (String)member.getPropertyValue("MEMBER_GUID");
        assertNull(stringPropValue);

        // This property works in Mondrian 1.1.5 (due to XMLA support)
        stringPropValue = (String)member.getPropertyValue("MEMBER_CAPTION");
        assertEquals(member.getCaption(), stringPropValue);

        stringPropValue = (String)member.getPropertyValue("CAPTION");
        assertEquals(member.getCaption(), stringPropValue);

        // It's worth checking case-sensitivity for CAPTION because it is a
        // synonym, not a true property.
        stringPropValue = (String) member.getPropertyValue(
            "caption", caseSensitive);
        if (caseSensitive) {
            assertNull(stringPropValue);
        } else {
            assertEquals(member.getCaption(), stringPropValue);
        }

        intPropValue = (Integer)member.getPropertyValue("MEMBER_ORDINAL");
        assertEquals(Integer.valueOf(member.getOrdinal()), intPropValue);

        if (false) {
            intPropValue =
                (Integer)member.getPropertyValue("CHILDREN_CARDINALITY");
            assertEquals(
                Integer.valueOf(scr.getMemberChildren(member).size()),
                intPropValue);
        }

        intPropValue = (Integer)member.getPropertyValue("PARENT_LEVEL");
        assertEquals(
            Integer.valueOf(member.getParentMember().getLevel().getDepth()),
            intPropValue);

        stringPropValue = (String)member.getPropertyValue("PARENT_UNIQUE_NAME");
        assertEquals(member.getParentUniqueName(), stringPropValue);

        intPropValue = (Integer)member.getPropertyValue("PARENT_COUNT");
        assertEquals(Integer.valueOf(1), intPropValue);

        stringPropValue = (String)member.getPropertyValue("DESCRIPTION");
        assertEquals(member.getDescription(), stringPropValue);

        // Case sensitivity.
        stringPropValue =
            (String)member.getPropertyValue("desCription", caseSensitive);
        if (caseSensitive) {
            assertNull(stringPropValue);
        } else {
            assertEquals(member.getDescription(), stringPropValue);
        }
    }

    public void testGetChildCardinalityPropertyValue() {
        Cube salesCube = getConnection().getSchema().lookupCube("Sales", true);
        SchemaReader scr = salesCube.getSchemaReader(null).withLocus();
        Member memberForCardinalityTest =
            scr.getMemberByUniqueName(
                Id.Segment.toList("Marital Status", "All Marital Status"),
                true);
        Integer intPropValue =
            (Integer) memberForCardinalityTest.getPropertyValue(
                "CHILDREN_CARDINALITY");
        assertEquals(Integer.valueOf(111), intPropValue);
    }

    /**
     * Tests the ability of MDX parser to pass requested member properties
     * to Result object.
     */
    public void testPropertiesMDX() {
        Result result = executeQuery(
            "SELECT {[Customers].[All Customers].[USA].[CA]} DIMENSION PROPERTIES \n"
            + " CATALOG_NAME, SCHEMA_NAME, CUBE_NAME, DIMENSION_UNIQUE_NAME, \n"
            + " HIERARCHY_UNIQUE_NAME, LEVEL_UNIQUE_NAME, LEVEL_NUMBER, MEMBER_UNIQUE_NAME, \n"
            + " MEMBER_NAME, MEMBER_TYPE, MEMBER_GUID, MEMBER_CAPTION, MEMBER_ORDINAL, CHILDREN_CARDINALITY,\n"
            + " PARENT_LEVEL, PARENT_UNIQUE_NAME, PARENT_COUNT, DESCRIPTION ON COLUMNS\n"
            + "FROM [Sales]");
        QueryAxis[] axes = result.getQuery().getAxes();
        Id[] axesProperties = axes[0].getDimensionProperties();
        String[] props = {
            "CATALOG_NAME",
            "SCHEMA_NAME",
            "CUBE_NAME",
            "DIMENSION_UNIQUE_NAME",
            "HIERARCHY_UNIQUE_NAME",
            "LEVEL_UNIQUE_NAME",
            "LEVEL_NUMBER",
            "MEMBER_UNIQUE_NAME",
            "MEMBER_NAME",
            "MEMBER_TYPE",
            "MEMBER_GUID",
            "MEMBER_CAPTION",
            "MEMBER_ORDINAL",
            "CHILDREN_CARDINALITY",
            "PARENT_LEVEL",
            "PARENT_UNIQUE_NAME",
            "PARENT_COUNT",
            "DESCRIPTION"
        };

        assertEquals(axesProperties.length, props.length);
        int i = 0;
        for (String prop : props) {
            assertEquals(prop, axesProperties[i++].toString());
        }
    }

    /**
     * Tests the ability to project non-standard member properties.
     */
    public void testMemberProperties() {
        Result result = executeQuery(
            "SELECT {[Stores].Children} DIMENSION PROPERTIES\n"
            + " CATALOG_NAME, PARENT_UNIQUE_NAME, [Store Type], FORMAT_EXP\n"
            + " ON COLUMNS\n"
            + "FROM [Sales]");
        QueryAxis[] axes = result.getQuery().getAxes();
        Id[] axesProperties = axes[0].getDimensionProperties();

        assertEquals(4, axesProperties.length);
    }

    /**
     * Tests the ability to project non-standard member properties.
     */
    public void testMemberPropertiesBad() {
        Result result = executeQuery(
            "SELECT {[Stores].Children} DIMENSION PROPERTIES\n"
            + " CATALOG_NAME, PARENT_UNIQUE_NAME, [Store Type], BAD\n"
            + " ON COLUMNS\n"
            + "FROM [Sales]");
        QueryAxis[] axes = result.getQuery().getAxes();
        Id[] axesProperties = axes[0].getDimensionProperties();

        assertEquals(4, axesProperties.length);
    }

    public void testMandatoryCellProperties() {
        Connection connection = getConnection();
        Query salesCube = connection.parseQuery(
            "select \n"
            + " {[Measures].[Store Sales], [Measures].[Unit Sales]} on columns, \n"
            + " {[Gender].members} on rows \n"
            + "from [Sales]");
        Result result = connection.execute(salesCube);
        int x = 1;
        int y = 2;
        Cell cell = result.getCell(new int[] {x, y});

        assertNull(cell.getPropertyValue("BACK_COLOR"));
        assertNull(cell.getPropertyValue("CELL_EVALUATION_LIST"));
        assertEquals(y * 2 + x, cell.getPropertyValue("CELL_ORDINAL"));
        assertNull(cell.getPropertyValue("FORE_COLOR"));
        assertNull(cell.getPropertyValue("FONT_NAME"));
        assertNull(cell.getPropertyValue("FONT_SIZE"));
        assertEquals(0, cell.getPropertyValue("FONT_FLAGS"));
        assertEquals("Standard", cell.getPropertyValue("FORMAT_STRING"));
        // FORMAT is a synonym for FORMAT_STRING
        assertEquals("Standard", cell.getPropertyValue("FORMAT"));
        assertEquals("135,215", cell.getPropertyValue("FORMATTED_VALUE"));
        assertNull(cell.getPropertyValue("NON_EMPTY_BEHAVIOR"));
        assertEquals(0, cell.getPropertyValue("SOLVE_ORDER"));
        assertEquals(
            135215.0,
            ((Number) cell.getPropertyValue("VALUE")).doubleValue(),
            0.1);

        // Case sensitivity.
        if (MondrianProperties.instance().CaseSensitive.get()) {
            assertNull(cell.getPropertyValue("cell_ordinal"));
            assertNull(cell.getPropertyValue("font_flags"));
            assertNull(cell.getPropertyValue("format_string"));
            assertNull(cell.getPropertyValue("format"));
            assertNull(cell.getPropertyValue("formatted_value"));
            assertNull(cell.getPropertyValue("solve_order"));
            assertNull(cell.getPropertyValue("value"));
        } else {
            assertEquals(y * 2 + x, cell.getPropertyValue("cell_ordinal"));
            assertEquals(0, cell.getPropertyValue("font_flags"));
            assertEquals("Standard", cell.getPropertyValue("format_string"));
            assertEquals("Standard", cell.getPropertyValue("format"));
            assertEquals("135,215", cell.getPropertyValue("formatted_value"));
            assertEquals(0, cell.getPropertyValue("solve_order"));
            assertEquals(
                135215.0,
                ((Number) cell.getPropertyValue("value")).doubleValue(),
                0.1);
        }
    }

    public void testPropertyDescription() throws Exception {
        TestContext context = getTestContext().legacy().create(
            null,
            "<Cube name=\"Foo\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Promotions\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
            + "      <Table name=\"promotion\"/>\n"
            + "      <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\">\n"
            + "   <Property name=\"BarProp\" column=\"promotion_name\" description=\"BaconDesc\"/>\n"
            + "   </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "</Cube>\n",
            null, null, null, null);
        assertEquals(
            "BaconDesc",
            context.getOlap4jConnection().getOlapSchema()
                .getCubes().get("Foo")
                .getDimensions().get("Promotions")
                .getHierarchies().get(0)
                .getLevels().get(1)
                .getProperties().get("BarProp")
                .getDescription());
    }
}

// End PropertiesTest.java
