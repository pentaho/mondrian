/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import java.util.*;

/**
 * Unit test for {@link RolapCube}.
 *
 * @author mkambol
 * @since 25 January, 2007
 */
public class RolapCubeTest extends FoodMartTestCase {

    public void testProcessFormatStringAttributeToIgnoreNullFormatString() {
        RolapCube cube =
            (RolapCube) getConnection().getSchema().lookupCube("Sales", false);
        StringBuilder builder = new StringBuilder();
        cube.processFormatStringAttribute(
            new MondrianDef.CalculatedMember(), builder);
        assertEquals(0, builder.length());
    }

    public void testProcessFormatStringAttribute() {
        RolapCube cube =
            (RolapCube) getConnection().getSchema().lookupCube("Sales", false);
        StringBuilder builder = new StringBuilder();
        MondrianDef.CalculatedMember xmlCalcMember =
            new MondrianDef.CalculatedMember();
        String format = "FORMAT";
        xmlCalcMember.formatString = format;
        cube.processFormatStringAttribute(xmlCalcMember, builder);
        assertEquals(
            "," + Util.nl + "FORMAT_STRING = \"" + format + "\"",
            builder.toString());
    }

    public void testGetCalculatedMembersWithNoRole() {
        String[] expectedCalculatedMembers = {
            "[Measures].[Profit]",
            "[Measures].[Average Warehouse Sale]",
            "[Measures].[Profit Growth]",
            "[Measures].[Profit Per Unit Shipped]"
        };
        Connection connection = getTestContext().getConnection();
        try {
            Cube warehouseAndSalesCube =
                cubeByName(connection, "Warehouse and Sales");
            SchemaReader schemaReader =
                warehouseAndSalesCube.getSchemaReader(null);

            List<Member> calculatedMembers =
                schemaReader.getCalculatedMembers();
            assertEquals(
                expectedCalculatedMembers.length,
                calculatedMembers.size());
            assertCalculatedMemberExists(
                expectedCalculatedMembers,
                calculatedMembers);
        } finally {
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
            Cube salesCube = cubeByName(connection, "Sales");
            SchemaReader schemaReader = salesCube
                .getSchemaReader(connection.getRole());

            List<Member> calculatedMembers =
                schemaReader.getCalculatedMembers();
            assertEquals(
                expectedCalculatedMembers.length,
                calculatedMembers.size());
            assertCalculatedMemberExists(
                expectedCalculatedMembers,
                calculatedMembers);
        } finally {
            connection.close();
        }
    }

    public void testGetCalculatedMembersReturnsOnlyAccessibleMembers() {
        String[] expectedCalculatedMembers = {
            "[Measures].[Profit]",
            "[Measures].[Profit last Period]",
            "[Measures].[Profit Growth]",
            "[Product].[~Missing]"
        };

        TestContext testContext =
            createTestContextWithAdditionalMembersAndARole();

        Connection connection = testContext.getConnection();

        try {
            Cube salesCube = cubeByName(connection, "Sales");
            SchemaReader schemaReader =
                salesCube.getSchemaReader(connection.getRole());
            List<Member> calculatedMembers =
                schemaReader.getCalculatedMembers();
            assertEquals(
                expectedCalculatedMembers.length,
                calculatedMembers.size());
            assertCalculatedMemberExists(
                expectedCalculatedMembers,
                calculatedMembers);
        } finally {
            connection.close();
        }
    }

    public void
        testGetCalculatedMembersReturnsOnlyAccessibleMembersForHierarchy()
    {
        String[] expectedCalculatedMembersFromProduct = {
            "[Product].[~Missing]"
        };
        TestContext testContext =
            createTestContextWithAdditionalMembersAndARole();
        Connection connection = testContext.getConnection();

        try {
            Cube salesCube = cubeByName(connection, "Sales");
            SchemaReader schemaReader =
                salesCube.getSchemaReader(connection.getRole());

            // Product.~Missing accessible
            List<Member> calculatedMembers =
                schemaReader.getCalculatedMembers(
                    getDimensionWithName(
                        "Product",
                        salesCube.getDimensions()).getHierarchy());

            assertEquals(
                expectedCalculatedMembersFromProduct.length,
                calculatedMembers.size());

            assertCalculatedMemberExists(
                expectedCalculatedMembersFromProduct,
                calculatedMembers);

            // Gender.~Missing not accessible
            calculatedMembers =
                schemaReader.getCalculatedMembers(
                    getDimensionWithName(
                        "Gender",
                        salesCube.getDimensions()).getHierarchy());
            assertEquals(0, calculatedMembers.size());
        } finally {
            connection.close();
        }
    }

    public void testGetCalculatedMembersReturnsOnlyAccessibleMembersForLevel() {
        String[] expectedCalculatedMembersFromProduct = new String[]{
            "[Product].[~Missing]"
        };

        TestContext testContext =
            createTestContextWithAdditionalMembersAndARole();
        Connection connection = testContext.getConnection();

        try {
            Cube salesCube = cubeByName(connection, "Sales");
            SchemaReader schemaReader =
                salesCube.getSchemaReader(connection.getRole());

            // Product.~Missing accessible
            List<Member> calculatedMembers =
                schemaReader.getCalculatedMembers(
                    getDimensionWithName(
                        "Product",
                        salesCube.getDimensions())
                    .getHierarchy().getLevels()[0]);

            assertEquals(
                expectedCalculatedMembersFromProduct.length,
                calculatedMembers.size());
            assertCalculatedMemberExists(
                expectedCalculatedMembersFromProduct,
                calculatedMembers);

            // Gender.~Missing not accessible
            calculatedMembers =
                schemaReader.getCalculatedMembers(
                    getDimensionWithName(
                        "Gender",
                        salesCube.getDimensions())
                    .getHierarchy().getLevels()[0]);
            assertEquals(0, calculatedMembers.size());
        } finally {
            connection.close();
        }
    }

    public void testNonJoiningDimensions() {
        TestContext testContext = this.getTestContext();

        Connection connection = testContext.getConnection();

        try {
            RolapCube salesCube = (RolapCube) cubeByName(connection, "Sales");

            RolapCube warehouseAndSalesCube =
                (RolapCube) cubeByName(connection, "Warehouse and Sales");
            SchemaReader readerWarehouseAndSales =
                warehouseAndSalesCube.getSchemaReader().withLocus();

            List<Member> members = new ArrayList<Member>();
            List<Member> warehouseMembers =
                warehouseMembersCanadaMexicoUsa(readerWarehouseAndSales);
            Dimension warehouseDim = warehouseMembers.get(0).getDimension();
            members.addAll(warehouseMembers);

            List<Member> storeMembers =
                storeMembersCAAndOR(readerWarehouseAndSales).slice(0);
            Dimension storeDim = storeMembers.get(0).getDimension();
            members.addAll(storeMembers);

            Set<Dimension> nonJoiningDims =
                salesCube.nonJoiningDimensions(members.toArray(new Member[0]));
            assertFalse(nonJoiningDims.contains(storeDim));
            assertTrue(nonJoiningDims.contains(warehouseDim));
        } finally {
            connection.close();
        }
    }

    public void testRolapCubeDimensionEquality() {
        TestContext testContext = getTestContext();

        Connection connection1 = testContext.getConnection();
        Connection connection2 =
            TestContext.instance().withSchema(null).getConnection();

        try {
            RolapCube salesCube1 = (RolapCube) cubeByName(connection1, "Sales");
            SchemaReader readerSales1 =
                salesCube1.getSchemaReader().withLocus();
            List<Member> storeMembersSales =
                storeMembersCAAndOR(readerSales1).slice(0);
            Dimension storeDim1 = storeMembersSales.get(0).getDimension();
            assertEquals(storeDim1, storeDim1);

            RolapCube salesCube2 = (RolapCube) cubeByName(connection2, "Sales");
            SchemaReader readerSales2 =
                salesCube2.getSchemaReader().withLocus();
            List<Member> storeMembersSales2 =
                storeMembersCAAndOR(readerSales2).slice(0);
            Dimension storeDim2 = storeMembersSales2.get(0).getDimension();
            assertEquals(storeDim1, storeDim2);


            RolapCube warehouseAndSalesCube =
                (RolapCube) cubeByName(connection1, "Warehouse and Sales");
            SchemaReader readerWarehouseAndSales =
                warehouseAndSalesCube.getSchemaReader().withLocus();
            List<Member> storeMembersWarehouseAndSales =
                storeMembersCAAndOR(readerWarehouseAndSales).slice(0);
            Dimension storeDim3 =
                storeMembersWarehouseAndSales.get(0).getDimension();
            assertFalse(storeDim1.equals(storeDim3));

            List<Member> warehouseMembers =
                warehouseMembersCanadaMexicoUsa(readerWarehouseAndSales);
            Dimension warehouseDim = warehouseMembers.get(0).getDimension();
            assertFalse(storeDim3.equals(warehouseDim));
        } finally {
            connection1.close();
            connection2.close();
        }
    }

    private TestContext createTestContextWithAdditionalMembersAndARole() {
        String nonAccessibleMember =
            "  <CalculatedMember name=\"~Missing\" dimension=\"Gender\">\n"
            + "    <Formula>100</Formula>\n"
            + "  </CalculatedMember>\n";
        String accessibleMember =
            "  <CalculatedMember name=\"~Missing\" dimension=\"Product\">\n"
            + "    <Formula>100</Formula>\n"
            + "  </CalculatedMember>\n";
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            nonAccessibleMember
            + accessibleMember);
        return testContext.withRole("California manager");
    }

    private void assertCalculatedMemberExists(
        String[] expectedCalculatedMembers,
        List<Member> calculatedMembers)
    {
        List expectedCalculatedMemberNames =
            Arrays.asList(expectedCalculatedMembers);
        for (Member calculatedMember : calculatedMembers) {
            String calculatedMemberName = calculatedMember.getUniqueName();
            assertTrue(
                "Calculated member name not found: " + calculatedMemberName,
                expectedCalculatedMemberNames.contains(calculatedMemberName));
        }
    }

    public void testBasedCubesForVirtualCube() {
      RolapCube cubeSales =
          (RolapCube) getConnection().getSchema().lookupCube("Sales", false);
      RolapCube cubeWarehouse =
          (RolapCube) getConnection().getSchema().lookupCube(
              "Warehouse", false);
      RolapCube cube =
          (RolapCube) getConnection().getSchema().lookupCube(
              "Warehouse and Sales", false);
      assertNotNull(cube);
      assertNotNull(cubeSales);
      assertNotNull(cubeWarehouse);
      assertEquals(true, cube.isVirtual());
      List<RolapCube> baseCubes = cube.getBaseCubes();
      assertNotNull(baseCubes);
      assertEquals(2, baseCubes.size());
      assertSame(cubeSales, baseCubes.get(0));
      assertEquals(cubeWarehouse, baseCubes.get(1));
    }

    public void testBasedCubesForNotVirtualCubeIsThisCube() {
      RolapCube cubeSales =
          (RolapCube) getConnection().getSchema().lookupCube("Sales", false);
      assertNotNull(cubeSales);
      assertEquals(false, cubeSales.isVirtual());
      List<RolapCube> baseCubes = cubeSales.getBaseCubes();
      assertNotNull(baseCubes);
      assertEquals(1, baseCubes.size());
      assertSame(cubeSales, baseCubes.get(0));
    }

}

// End RolapCubeTest.java
