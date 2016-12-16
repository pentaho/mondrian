/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation.
// All Rights Reserved.
*/
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
}

// End RolapEvaluatorTest.java