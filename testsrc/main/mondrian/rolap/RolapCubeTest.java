/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// mkambol, 25 January, 2007
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.olap.*;

import java.util.List;
import java.util.Arrays;

/**
 * Unit test for {@link RolapCube}.
 *
 * @author mkambol
 * @since 25 January, 2007
 * @version $Id$
 */
public class RolapCubeTest extends FoodMartTestCase {

    public void testProcessFormatStringAttributeToIgnoreNullFormatString(){
        RolapCube cube = (RolapCube) getConnection().getSchema().lookupCube("Sales", false);
        StringBuilder builder = new StringBuilder();
        cube.processFormatStringAttribute(new MondrianDef.CalculatedMember(), builder);
        assertEquals(0, builder.length());
    }

    public void testProcessFormatStringAttribute(){
        RolapCube cube = (RolapCube) getConnection().getSchema().lookupCube("Sales", false);
        StringBuilder builder = new StringBuilder();
        MondrianDef.CalculatedMember xmlCalcMember = new MondrianDef.CalculatedMember();
        String format = "FORMAT";
        xmlCalcMember.formatString = format;
        cube.processFormatStringAttribute(xmlCalcMember, builder);
        assertEquals(","+ Util.nl+"FORMAT_STRING = \""+format+"\"", builder.toString());
    }

    public void testGetCalculatedMembersWithNoRole() {
        String[] expectedCalculatedMembers = new String[] {
                "[Measures].[Profit]", "[Measures].[Average Warehouse Sale]",
                "[Measures].[Profit Growth]", "[Measures].[Profit Per Unit Shipped]"
        };
        Connection connection = getTestContext().getConnection();
        try {
            SchemaReader reader = connection.getSchemaReader();

            Cube[] cubes = reader.getCubes();
            Cube warehouseAndSalesCube =
                    getCubeWithName("Warehouse and Sales", cubes);
            SchemaReader schemaReader = warehouseAndSalesCube.getSchemaReader(null);

            List<Member> calculatedMembers = schemaReader.getCalculatedMembers();

            assertEquals(expectedCalculatedMembers.length,
                    calculatedMembers.size());

            assertCalculatedMemberExists(expectedCalculatedMembers,
                    calculatedMembers);
        }
        finally {
            connection.close();
        }
    }

    public void testGetCalculatedMembersForCaliforniaManager() {

        String[] expectedCalculatedMembers = new String[] {
                "[Measures].[Profit]", "[Measures].[Profit last Period]",
                "[Measures].[Profit Growth]"
        };

        Connection connection = getTestContext().withRole("California manager")
                .getConnection();

        try {
            SchemaReader reader = connection.getSchemaReader();

            Cube[] cubes = reader.getCubes();
            Cube salesCube = getCubeWithName("Sales", cubes);
            SchemaReader schemaReader = salesCube
                    .getSchemaReader(connection.getRole());

            List<Member> calculatedMembers = schemaReader.getCalculatedMembers();

            assertEquals(expectedCalculatedMembers.length, calculatedMembers.size());

            assertCalculatedMemberExists(expectedCalculatedMembers,
                    calculatedMembers);
        }
        finally {
            connection.close();
        }
    }

    public void testGetCalculatedMembersReturnsOnlyAccessibleMembers() {

        String[] expectedCalculatedMembers = new String[]{
                "[Measures].[Profit]", "[Measures].[Profit last Period]",
                "[Measures].[Profit Growth]", "[Product].[~Missing]"
        };

        TestContext testContext = createTestContextWithAdditionalMembersAndARole();

        Connection connection = testContext.getConnection();

        try {
            SchemaReader reader = connection.getSchemaReader();

            Cube[] cubes = reader.getCubes();
            Cube salesCube = getCubeWithName("Sales", cubes);
            SchemaReader schemaReader =
                    salesCube.getSchemaReader(connection.getRole());

            List<Member> calculatedMembers = schemaReader.getCalculatedMembers();

            assertEquals(expectedCalculatedMembers.length,
                    calculatedMembers.size());

            assertCalculatedMemberExists(expectedCalculatedMembers,
                    calculatedMembers);
        }
        finally {
            connection.close();
        }
    }

    public void testGetCalculatedMembersReturnsOnlyAccessibleMembersForHierarchy() {

        String[] expectedCalculatedMembersFromProduct = new String[]{
                "[Product].[~Missing]"
        };

        TestContext testContext = createTestContextWithAdditionalMembersAndARole();

        Connection connection = testContext.getConnection();

        try {
            SchemaReader reader = connection.getSchemaReader();

            Cube[] cubes = reader.getCubes();
            Cube salesCube = getCubeWithName("Sales", cubes);
            SchemaReader schemaReader =
                    salesCube.
                            getSchemaReader(connection.getRole());

            // Product.~Missing accessible
            List<Member> calculatedMembers =
                    schemaReader.getCalculatedMembers(
                            getDimensionWithName("Product",
                                    salesCube.getDimensions()).getHierarchy());

            assertEquals(expectedCalculatedMembersFromProduct.length,
                    calculatedMembers.size());

            assertCalculatedMemberExists(expectedCalculatedMembersFromProduct,
                    calculatedMembers);

            // Gender.~Missing not accessible
            calculatedMembers =
                    schemaReader.getCalculatedMembers(
                            getDimensionWithName("Gender",
                                    salesCube.getDimensions()).getHierarchy());
            assertEquals(0, calculatedMembers.size());
        }
        finally {
            connection.close();
        }
    }

    public void testGetCalculatedMembersReturnsOnlyAccessibleMembersForLevel() {

        String[] expectedCalculatedMembersFromProduct = new String[]{
                "[Product].[~Missing]"
        };

        TestContext testContext = createTestContextWithAdditionalMembersAndARole();

        Connection connection = testContext.getConnection();

        try {
            SchemaReader reader = connection.getSchemaReader();

            Cube[] cubes = reader.getCubes();
            Cube salesCube = getCubeWithName("Sales", cubes);
            SchemaReader schemaReader =
                    salesCube.
                            getSchemaReader(connection.getRole());

            // Product.~Missing accessible
            List<Member> calculatedMembers =
                    schemaReader.getCalculatedMembers(
                            getDimensionWithName("Product",
                                    salesCube.getDimensions()).
                                    getHierarchy().getLevels()[0]);

            assertEquals(expectedCalculatedMembersFromProduct.length,
                    calculatedMembers.size());

            assertCalculatedMemberExists(expectedCalculatedMembersFromProduct,
                    calculatedMembers);

            // Gender.~Missing not accessible
            calculatedMembers =
                    schemaReader.getCalculatedMembers(
                            getDimensionWithName("Gender",
                                    salesCube.getDimensions()).
                                    getHierarchy().getLevels()[0]);
            assertEquals(0, calculatedMembers.size());
        }
        finally {
            connection.close();
        }
    }

    private TestContext createTestContextWithAdditionalMembersAndARole() {
        String nonAccessibleMember =
                "  <CalculatedMember name=\"~Missing\" dimension=\"Gender\">\n" +
                "    <Formula>100</Formula>\n" +
                "  </CalculatedMember>\n";
        String accessibleMember =
                "  <CalculatedMember name=\"~Missing\" dimension=\"Product\">\n" +
                "    <Formula>100</Formula>\n" +
                "  </CalculatedMember>\n";
        TestContext testContext = TestContext.createSubstitutingCube(
                "Sales",
                null,
                nonAccessibleMember +
                accessibleMember
        );
        return testContext.withRole("California manager");
    }

    private Dimension getDimensionWithName(String name, Dimension[] dimensions) {
        Dimension resultDimension = null;
        for (Dimension dimension : dimensions) {
            if (dimension.getName().equals(name)) {
                resultDimension = dimension;
                break;
            }
        }
        return resultDimension;
    }

    private void assertCalculatedMemberExists(String[] expectedCalculatedMembers,
                                              List<Member> calculatedMembers) {
        List expectedCalculatedMemberNames = Arrays.asList(expectedCalculatedMembers);
        for (Member calculatedMember : calculatedMembers) {
            String calculatedMemberName = calculatedMember.getUniqueName();
            assertTrue("Calculated member name not found: " + calculatedMemberName,
                expectedCalculatedMemberNames.contains(calculatedMemberName));
        }
    }

    private Cube getCubeWithName(String cubeName, Cube[] cubes) {
        Cube resultCube = null;
        for (Cube cube : cubes) {
            if (cubeName.equals(cube.getName()))
            {
                resultCube = cube;
                break;
            }
        }
        return resultCube;
    }

}

// End RolapCubeTest.java
