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

package mondrian.rolap;

import mondrian.test.FoodMartTestCase;

public class RolapEvaluatorTest extends FoodMartTestCase {

    public void testGetSlicerPredicateInfo() throws Exception {
        RolapResult result = (RolapResult) executeQuery(
            "select  from sales "
            + "WHERE {[Time].[1997].Q1, [Time].[1997].Q2} "
            + "* { Store.[USA].[CA], Store.[USA].[WA]}");
        RolapEvaluator evalulator = (RolapEvaluator) result.getRootEvaluator();
        final CompoundPredicateInfo slicerPredicateInfo =
            evalulator.getSlicerPredicateInfo();
        assertEquals(
            "(((`store`.`store_state`, `time_by_day`.`the_year`, `time_by_day`.`quarter`) in "
            + "(('CA', 1997, 'Q1'), ('WA', 1997, 'Q1'), ('CA', 1997, 'Q2'), ('WA', 1997, 'Q2'))))",
            slicerPredicateInfo.getPredicateString());
        assertTrue(slicerPredicateInfo.isSatisfiable());
    }

    /*
    public void testSlicerPredicateUnsatisfiable() {
        assertQueryReturns(
            "select measures.[Customer Count] on 0 from [warehouse and sales] "
            + "WHERE {[Time].[1997].Q1, [Time].[1997].Q2} "
            + "*{[Warehouse].[USA].[CA], Warehouse.[USA].[WA]}", "");
        RolapResult result = (RolapResult) executeQuery(
            "select  from [warehouse and sales] "
            + "WHERE {[Time].[1997].Q1, [Time].[1997].Q2} "
            + "* Head([Warehouse].[Country].members, 2)");
        RolapEvaluator evalulator = (RolapEvaluator) result.getRootEvaluator();
        assertFalse(evalulator.getSlicerPredicateInfo().isSatisfiable());
        assertNull(evalulator.getSlicerPredicateInfo().getPredicate());
    }
    */
    
    public void testListColumnPredicateInfo() throws Exception {
      RolapResult result = (RolapResult) executeQuery(
          "select  from sales "
          + "WHERE {[Product].[Drink],[Product].[Non-Consumable]} ");
      RolapEvaluator evalulator = (RolapEvaluator) result.getRootEvaluator();
      final CompoundPredicateInfo slicerPredicateInfo =
          evalulator.getSlicerPredicateInfo();
      assertEquals(
          "`product_class`.`product_family` in ('Drink', 'Non-Consumable')",
          slicerPredicateInfo.getPredicateString());
      assertTrue(slicerPredicateInfo.isSatisfiable());
    }
    
    public void testOrPredicateInfo() throws Exception {
      RolapResult result = (RolapResult) executeQuery(
          "select  from sales "
          + "WHERE {[Product].[Drink].[Beverages],[Product].[Food].[Produce],[Product].[Non-Consumable]} ");
      RolapEvaluator evalulator = (RolapEvaluator) result.getRootEvaluator();
      final CompoundPredicateInfo slicerPredicateInfo =
          evalulator.getSlicerPredicateInfo();
      assertEquals(
          "((((`product_class`.`product_family`, `product_class`.`product_department`) in "
        + "(('Drink', 'Beverages'), ('Food', 'Produce')))) or `product_class`.`product_family` = 'Non-Consumable')",
          slicerPredicateInfo.getPredicateString());
      assertTrue(slicerPredicateInfo.isSatisfiable());
    }
    
}

// End RolapEvaluatorTest.java