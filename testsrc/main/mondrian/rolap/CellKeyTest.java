/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2008 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * Test that the implementations of the CellKey interface are correct.
 *
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 */
public class CellKeyTest extends FoodMartTestCase {
    public CellKeyTest() {
    }

    public CellKeyTest(String name) {
        super(name);
    }

    public void testMany() {
        CellKey key = CellKey.Generator.newManyCellKey(5);

        assertTrue("CellKey size" , (key.size() == 5));

        CellKey copy = key.copy();
        assertTrue("CellKey equals" , (key.equals(copy)));

        int[] ordinals = key.getOrdinals();
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , (key.equals(copy)));

        boolean gotException = false;
        try {
            key.setAxis(6, 1);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey axis too big" , (gotException));

        gotException = false;
        try {
            key.setOrdinals(new int[6]);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey array too big" , (gotException));

        gotException = false;
        try {
            key.setOrdinals(new int[4]);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey array too small" , (gotException));

        key.setAxis(0, 1);
        key.setAxis(1, 3);
        key.setAxis(2, 5);
        key.setAxis(3, 7);
        key.setAxis(4, 13);
        assertTrue("CellKey not equals" , (! key.equals(copy)));

        copy = key.copy();
        assertTrue("CellKey equals" , (key.equals(copy)));

        ordinals = key.getOrdinals();
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , (key.equals(copy)));
    }

    public void testZero() {
        CellKey key = CellKey.Generator.newCellKey(new int[0]);
        CellKey key2 = CellKey.Generator.newCellKey(new int[0]);
        assertTrue(key == key2); // all 0-dimensional keys have same singleton
        assertEquals(0, key.size());

        CellKey keyMany = CellKey.Generator.newManyCellKey(0);
        assertEquals(keyMany, key);

        CellKey copy = key.copy();
        assertEquals(copy, key);

        boolean gotException = false;
        try {
            key.setAxis(0, 0);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey axis too big" , gotException);

        int[] ordinals = key.getOrdinals();
        assertEquals(ordinals.length, 0);
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , key.equals(copy));
    }

    public void testOne() {
        CellKey keyMany = CellKey.Generator.newManyCellKey(1);
        CellKey key = CellKey.Generator.newCellKey(1);

        assertTrue("CellKey size" , (key.size() == 1));
        assertTrue("CellKey size" , (keyMany.size() == 1));
        assertTrue("CellKey equals" , (key.equals(keyMany)));

        CellKey copy = key.copy();
        assertTrue("CellKey equals" , (key.equals(copy)));

        int[] ordinals = key.getOrdinals();
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , (key.equals(copy)));

        boolean gotException = false;
        try {
            key.setAxis(3, 1);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey axis too big" , (gotException));

        gotException = false;
        try {
            key.setOrdinals(new int[3]);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey array too big" , (gotException));

        gotException = false;
        try {
            key.setOrdinals(new int[0]);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey array too small" , (gotException));

        key.setAxis(0, 1);
        assertTrue("CellKey not equals" , (! key.equals(keyMany)));

        keyMany.setAxis(0, 1);
        assertTrue("CellKey equals" , (key.equals(keyMany)));

        copy = key.copy();
        assertTrue("CellKey equals" , (key.equals(copy)));

        ordinals = key.getOrdinals();
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , (key.equals(copy)));
    }

    public void testTwo() {
        CellKey keyMany = CellKey.Generator.newManyCellKey(2);
        CellKey key = CellKey.Generator.newCellKey(2);

        assertTrue("CellKey size" , (key.size() == 2));
        assertTrue("CellKey size" , (keyMany.size() == 2));
        assertTrue("CellKey equals" , (key.equals(keyMany)));

        CellKey copy = key.copy();
        assertTrue("CellKey equals" , (key.equals(copy)));

        int[] ordinals = key.getOrdinals();
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , (key.equals(copy)));

        boolean gotException = false;
        try {
            key.setAxis(3, 1);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey axis too big" , (gotException));

        gotException = false;
        try {
            key.setOrdinals(new int[3]);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey array too big" , (gotException));

        gotException = false;
        try {
            key.setOrdinals(new int[1]);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey array too small" , (gotException));

        key.setAxis(0, 1);
        key.setAxis(1, 3);
        assertTrue("CellKey not equals" , (! key.equals(keyMany)));

        keyMany.setAxis(0, 1);
        keyMany.setAxis(1, 3);
        assertTrue("CellKey equals" , (key.equals(keyMany)));

        copy = key.copy();
        assertTrue("CellKey equals" , (key.equals(copy)));

        ordinals = key.getOrdinals();
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , (key.equals(copy)));
    }

    public void testThree() {
        CellKey keyMany = CellKey.Generator.newManyCellKey(3);
        CellKey key = CellKey.Generator.newCellKey(3);

        assertTrue("CellKey size" , (key.size() == 3));
        assertTrue("CellKey size" , (keyMany.size() == 3));
        assertTrue("CellKey equals" , (key.equals(keyMany)));

        CellKey copy = key.copy();
        assertTrue("CellKey equals" , (key.equals(copy)));

        int[] ordinals = key.getOrdinals();
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , (key.equals(copy)));

        boolean gotException = false;
        try {
            key.setAxis(3, 1);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey axis too big" , (gotException));

        gotException = false;
        try {
            key.setOrdinals(new int[4]);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey array too big" , (gotException));

        gotException = false;
        try {
            key.setOrdinals(new int[1]);
        } catch (Exception ex) {
            gotException = true;
        }
        assertTrue("CellKey array too small" , (gotException));

        key.setAxis(0, 1);
        key.setAxis(1, 3);
        key.setAxis(2, 5);
        assertTrue("CellKey not equals" , (! key.equals(keyMany)));

        keyMany.setAxis(0, 1);
        keyMany.setAxis(1, 3);
        keyMany.setAxis(2, 5);
        assertTrue("CellKey equals" , (key.equals(keyMany)));

        copy = key.copy();
        assertTrue("CellKey equals" , (key.equals(copy)));

        ordinals = key.getOrdinals();
        copy = CellKey.Generator.newCellKey(ordinals);
        assertTrue("CellKey equals" , (key.equals(copy)));
    }

    public void testCellLookup() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        String cubeDef =
            "<Cube name = \"SalesTest\" defaultMeasure=\"Unit Sales\">\n" +
            "  <Table name=\"sales_fact_1997\"/>\n" +
            "  <Dimension name=\"City\" foreignKey=\"customer_id\">\n" +
            "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
            "      <Table name=\"customer\"/>\n" +
            "      <Level name=\"city\" column=\"city\" uniqueMembers=\"true\"/>\n" +
            "    </Hierarchy>\n" +
            "  </Dimension>\n" +
            "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n" +
            "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
            "      <Table name=\"customer\"/>\n" +
            "      <Level name=\"gender\" column=\"gender\" uniqueMembers=\"true\"/>\n" +
            "    </Hierarchy>\n" +
            "  </Dimension>\n" +
            "  <Dimension name=\"Address2\" foreignKey=\"customer_id\">\n" +
            "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
            "      <Table name=\"customer\"/>\n" +
            "      <Level name=\"addr\" column=\"address2\" uniqueMembers=\"true\"/>\n" +
            "    </Hierarchy>\n" +
            "  </Dimension>\n" +
            "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n" +
            "</Cube>";

        String query =
            "With Set [*NATIVE_CJ_SET] as NonEmptyCrossJoin([Gender].Children, [Address2].Children) " +
            "Select Generate([*NATIVE_CJ_SET], {([Gender].CurrentMember, [Address2].CurrentMember)}) on columns " +
            "From [SalesTest] where ([City].[Redwood City])";

        String result =
            "Axis #0:\n" +
            "{[City].[All Citys].[Redwood City]}\n" +
            "Axis #1:\n" +
            "{[Gender].[All Genders].[F], [Address2].[All Address2s].[#null]}\n" +
            "{[Gender].[All Genders].[F], [Address2].[All Address2s].[#2]}\n" +
            "{[Gender].[All Genders].[F], [Address2].[All Address2s].[Unit H103]}\n" +
            "{[Gender].[All Genders].[M], [Address2].[All Address2s].[#null]}\n" +
            "{[Gender].[All Genders].[M], [Address2].[All Address2s].[#208]}\n" +
            "Row #0: 71\n" +
            "Row #0: 10\n" +
            "Row #0: 3\n" +
            "Row #0: 52\n" +
            "Row #0: 8\n";

        /*
         * Make sure ExpandNonNative is not set. Otherwise, the query is evaluated
         * natively. For the given data set(which contains NULL members), native
         * evaluation produces results in a different order from the non-native
         * evaluation.
         */
        boolean origExpandNonNative =
            MondrianProperties.instance().ExpandNonNative.get();
        MondrianProperties.instance().ExpandNonNative.set(false);

        TestContext testContext =
            TestContext.create(
                null,
                cubeDef,
                null,
                null,
                null,
                null);

        testContext.assertQueryReturns(query, fold(result));
        MondrianProperties.instance().ExpandNonNative.set(origExpandNonNative);
    }
}

// End CellKeyTest.java
