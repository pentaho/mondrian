/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;


public class DeadlockTest extends FoodMartTestCase {

    public void testSegmentLoadDeadlock() {
        // http://jira.pentaho.com/browse/MONDRIAN-1726
        // Deadlock can occur if a cardinality query is fired after
        // all available database connections have been consumed and active
        // segment load queries have not registered.
        // The query below can cause this issue. Each aggregate() member
        // results in a separate segment load (which will exceed the available
        // 20), and cardinality is checked as a part of segment loads.
        propSaver.set(
            MondrianProperties.instance().QueryLimit, 20);
        Thread bigQueryThread = new Thread(
            new Runnable() {
            public void run() {
                executeQuery(
                    "With\n"
                    + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Store],NonEmptyCrossJoin([*BASE_MEMBERS_Product],[*BASE_MEMBERS_Time]))'\n"
                    + "Set [*BASE_MEMBERS_Product] as '[Product].[Product Subcategory].Members'\n"
                    + "Set [*NATIVE_MEMBERS_Time] as 'Generate([*NATIVE_CJ_SET], {[Time].CurrentMember})'\n"
                    + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
                    + "Set [*CJ_SLICER_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Time].currentMember)})'\n"
                    + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Store].currentMember,[Product].currentMember)})'\n"
                    + "Set [*BASE_MEMBERS_Time] as '{[Time].[1997].[Q1].[2],[Time].[1998].[Q1].[2],[Time].[1997].[Q1].[3],[Time].[1998].[Q1].[3],[Time].[1997].[Q2].[4],[Time].[1998].[Q2].[4],[Time].[1997].[Q2].[5],[Time].[1998].[Q2].[5]}'\n"
                    + "Set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS],[Store].CurrentMember.OrderKey,BASC,Ancestor([Store].CurrentMember,[Store].[Store Country]).OrderKey,BASC,Ancestor([Product].CurrentMember, [Product].[Product Family]).OrderKey,BASC,Ancestor([Product].CurrentMember, [Product].[Product Department]).OrderKey,BASC,Ancestor([Product].CurrentMember, [Product].[Product Category]).OrderKey,BASC,[Product].CurrentMember.OrderKey,BASC)'\n"
                    + "Set [*BASE_MEMBERS_Store] as '[Store].[Store State].Members'\n"
                    + "Member [Product].[Food].[Frozen Foods].[Meat].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Frozen Foods].[Meat]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Frozen Foods].[Vegetables].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Frozen Foods].[Vegetables]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Household].[Cleaning Supplies].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Household].[Cleaning Supplies]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Foods].[Canned Sardines].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Foods].[Canned Sardines]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Produce].[Packaged Vegetables].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Produce].[Packaged Vegetables]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Health and Hygiene].[Decongestants].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Health and Hygiene].[Decongestants]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Foods].[Canned Soup].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Foods].[Canned Soup]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Health and Hygiene].[Hygiene].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Health and Hygiene].[Hygiene]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Household].[Plastic Products].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Household].[Plastic Products]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Drink].[Beverages].[Hot Beverages].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Drink].[Beverages].[Hot Beverages]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Baking Goods].[Jams and Jellies].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Baking Goods].[Jams and Jellies]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Dairy].[Dairy].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Dairy].[Dairy]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Baking Goods].[Baking Goods].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Baking Goods].[Baking Goods]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Drink].[Dairy].[Dairy].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Drink].[Dairy].[Dairy]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Frozen Foods].[Frozen Entrees].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Frozen Foods].[Frozen Entrees]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Checkout].[Miscellaneous].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Checkout].[Miscellaneous]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Health and Hygiene].[Bathroom Products]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Household].[Kitchen Products].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Household].[Kitchen Products]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Meat].[Meat].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Meat].[Meat]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Breakfast Foods].[Breakfast Foods].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Breakfast Foods].[Breakfast Foods]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Periodicals].[Magazines].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Periodicals].[Magazines]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Eggs].[Eggs].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Eggs].[Eggs]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Foods].[Canned Oysters].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Foods].[Canned Oysters]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Frozen Foods].[Pizza].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Frozen Foods].[Pizza]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Household].[Hardware].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Household].[Hardware]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Foods].[Canned Anchovies].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Foods].[Canned Anchovies]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Household].[Electrical].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Household].[Electrical]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Drink].[Beverages].[Carbonated Beverages].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Drink].[Beverages].[Carbonated Beverages]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Produce].[Specialty].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Produce].[Specialty]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Frozen Foods].[Frozen Desserts].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Frozen Foods].[Frozen Desserts]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Drink].[Beverages].[Pure Juice Beverages].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Drink].[Beverages].[Pure Juice Beverages]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Baked Goods].[Bread].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Baked Goods].[Bread]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Starchy Foods].[Starchy Foods].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Starchy Foods].[Starchy Foods]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Deli].[Side Dishes].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Deli].[Side Dishes]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Products].[Fruit].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Products].[Fruit]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Carousel].[Specialty].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Carousel].[Specialty]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Deli].[Meat].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Deli].[Meat]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Produce].[Vegetables].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Produce].[Vegetables]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Household].[Paper Products].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Household].[Paper Products]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Household].[Bathroom Products].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Household].[Bathroom Products]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Foods].[Vegetables].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Foods].[Vegetables]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Snack Foods].[Snack Foods].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Snack Foods].[Snack Foods]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Foods].[Canned Shrimp].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Foods].[Canned Shrimp]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Snacks].[Candy].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Snacks].[Candy]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Foods].[Canned Tuna].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Foods].[Canned Tuna]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Household].[Candles].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Household].[Candles]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Frozen Foods].[Breakfast Foods].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Frozen Foods].[Breakfast Foods]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]})', SOLVE_ORDER=-101\n"
                    + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Customer Count]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
                    + "Member [Product].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Health and Hygiene].[Pain Relievers]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Drink].[Beverages].[Drinks].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Drink].[Beverages].[Drinks]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Checkout].[Hardware].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Checkout].[Hardware]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Produce].[Fruit].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Produce].[Fruit]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Non-Consumable].[Health and Hygiene].[Cold Remedies]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Canned Foods].[Canned Clams].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Canned Foods].[Canned Clams]})', SOLVE_ORDER=-101\n"
                    + "Member [Product].[Food].[Seafood].[Seafood].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[Food].[Seafood].[Seafood]})', SOLVE_ORDER=-101\n"
                    + "Select\n"
                    + "[*BASE_MEMBERS_Measures] on columns,\n"
                    + "Non Empty Union(CrossJoin(Generate([*SORTED_ROW_AXIS], {([Store].currentMember)}),{[Product].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Canned Foods].[Canned Anchovies].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Periodicals].[Magazines].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Baked Goods].[Bread].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Household].[Electrical].[*TOTAL_MEMBER_SEL~AGG],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Baking Goods].[Baking Goods].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Deli].[Side Dishes].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Eggs].[Eggs].[*TOTAL_MEMBER_SEL~AGG],[Product].[Drink].[Beverages].[Drinks].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Frozen Foods].[Vegetables].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Seafood].[Seafood].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Produce].[Fruit].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Produce].[Vegetables].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Frozen Foods].[Meat].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Meat].[Meat].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Frozen Foods].[Frozen Desserts].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Baking Goods].[Jams and Jellies].[*TOTAL_MEMBER_SEL~AGG],[Product].[Drink].[Beverages].[Pure Juice Beverages].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Checkout].[Miscellaneous].[*TOTAL_MEMBER_SEL~AGG],[Product].[Drink].[Dairy].[Dairy].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Health and Hygiene].[Decongestants].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Produce].[Specialty].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Canned Foods].[Canned Oysters].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Frozen Foods].[Breakfast Foods].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Household].[Paper Products].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Starchy Foods].[Starchy Foods].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Health and Hygiene].[Hygiene].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Frozen Foods].[Pizza].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Household].[Plastic Products].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Household].[Kitchen Products].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Canned Foods].[Canned Sardines].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Checkout].[Hardware].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Canned Foods].[Canned Shrimp].[*TOTAL_MEMBER_SEL~AGG],[Product].[Drink].[Beverages].[Carbonated Beverages].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Canned Foods].[Canned Soup].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Carousel].[Specialty].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Frozen Foods].[Frozen Entrees].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Produce].[Packaged Vegetables].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Household].[Bathroom Products].[*TOTAL_MEMBER_SEL~AGG],[Product].[Non-Consumable].[Household].[Hardware].[*TOTAL_MEMBER_SEL~AGG],[Product].[Food].[Canned Foods].[Canned Tuna].[*TOTAL_MEMBER_SEL~AGG]}),[*SORTED_ROW_AXIS]) on rows\n"
                    + "From [Sales]\n"
                    + "Where ([*CJ_SLICER_AXIS])");
            }
        });
        try {
            bigQueryThread.start();
            // give the query 10 seconds to complete.  Likely deadlock if
            // it hasn't finished.
            bigQueryThread.join(10000);
            assertEquals(
                "Possible deadlock, query thread is still alive.",
                false, bigQueryThread.isAlive());
        } catch (InterruptedException e) {
            fail();
        }
        propSaver.reset();
    }
}

// End DeadlockTest.java