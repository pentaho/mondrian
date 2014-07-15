/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2006 Thomson Medstat, Inc, Ann Arbor, MI.
// Copyright (C) 2006-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * Tests the {@link MondrianProperties#EnableNonEmptyOnAllAxis} property.
 */
public class NonEmptyPropertyForAllAxisTest extends FoodMartTestCase {
    public void testNonEmptyForAllAxesWithPropertySet() {
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, true);
        assertQueryReturns(
            "select {[Country].[USA].[OR].Children} on 0,"
            + " {[Promotions].Members} on 1 "
            + "from [Sales] "
            + "where [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]",
            "Axis #0:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[OR].[Albany]}\n"
            + "{[Customer].[Customers].[USA].[OR].[Corvallis]}\n"
            + "{[Customer].[Customers].[USA].[OR].[Lake Oswego]}\n"
            + "{[Customer].[Customers].[USA].[OR].[Lebanon]}\n"
            + "{[Customer].[Customers].[USA].[OR].[Portland]}\n"
            + "{[Customer].[Customers].[USA].[OR].[Woodburn]}\n"
            + "Axis #2:\n"
            + "{[Promotion].[Promotions].[All Promotions]}\n"
            + "{[Promotion].[Promotions].[Cash Register Lottery]}\n"
            + "{[Promotion].[Promotions].[No Promotion]}\n"
            + "{[Promotion].[Promotions].[Saving Days]}\n"
            + "Row #0: 4\n"
            + "Row #0: 6\n"
            + "Row #0: 5\n"
            + "Row #0: 10\n"
            + "Row #0: 6\n"
            + "Row #0: 3\n"
            + "Row #1: \n"
            + "Row #1: 2\n"
            + "Row #1: \n"
            + "Row #1: 2\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: 4\n"
            + "Row #2: 4\n"
            + "Row #2: 3\n"
            + "Row #2: 8\n"
            + "Row #2: 6\n"
            + "Row #2: 3\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 2\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: \n");
    }

    public void testNonEmptyForAllAxesWithOutPropertySet() {
        assertQueryReturns(
            "SELECT {customers.USA.CA.[Santa Cruz].[Brian Merlo]} on 0, "
            + "[product].[product category].members on 1 FROM [sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA].[Santa Cruz].[Brian Merlo]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Drinks]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Hot Beverages]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread]}\n"
            + "{[Product].[Products].[Food].[Baking Goods].[Baking Goods]}\n"
            + "{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies]}\n"
            + "{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods]}\n"
            + "{[Product].[Products].[Food].[Canned Foods].[Canned Anchovies]}\n"
            + "{[Product].[Products].[Food].[Canned Foods].[Canned Clams]}\n"
            + "{[Product].[Products].[Food].[Canned Foods].[Canned Oysters]}\n"
            + "{[Product].[Products].[Food].[Canned Foods].[Canned Sardines]}\n"
            + "{[Product].[Products].[Food].[Canned Foods].[Canned Shrimp]}\n"
            + "{[Product].[Products].[Food].[Canned Foods].[Canned Soup]}\n"
            + "{[Product].[Products].[Food].[Canned Foods].[Canned Tuna]}\n"
            + "{[Product].[Products].[Food].[Canned Foods].[Vegetables]}\n"
            + "{[Product].[Products].[Food].[Canned Products].[Fruit]}\n"
            + "{[Product].[Products].[Food].[Dairy].[Dairy]}\n"
            + "{[Product].[Products].[Food].[Deli].[Meat]}\n"
            + "{[Product].[Products].[Food].[Deli].[Side Dishes]}\n"
            + "{[Product].[Products].[Food].[Eggs].[Eggs]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[Meat]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[Pizza]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[Vegetables]}\n"
            + "{[Product].[Products].[Food].[Meat].[Meat]}\n"
            + "{[Product].[Products].[Food].[Produce].[Fruit]}\n"
            + "{[Product].[Products].[Food].[Produce].[Packaged Vegetables]}\n"
            + "{[Product].[Products].[Food].[Produce].[Specialty]}\n"
            + "{[Product].[Products].[Food].[Produce].[Vegetables]}\n"
            + "{[Product].[Products].[Food].[Seafood].[Seafood]}\n"
            + "{[Product].[Products].[Food].[Snack Foods].[Snack Foods]}\n"
            + "{[Product].[Products].[Food].[Snacks].[Candy]}\n"
            + "{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods]}\n"
            + "{[Product].[Products].[Non-Consumable].[Carousel].[Specialty]}\n"
            + "{[Product].[Products].[Non-Consumable].[Checkout].[Hardware]}\n"
            + "{[Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous]}\n"
            + "{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products]}\n"
            + "{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies]}\n"
            + "{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants]}\n"
            + "{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene]}\n"
            + "{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household].[Bathroom Products]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household].[Candles]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household].[Electrical]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household].[Hardware]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household].[Paper Products]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household].[Plastic Products]}\n"
            + "{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines]}\n"
            + "Row #0: 2\n"
            + "Row #1: 2\n"
            + "Row #2: \n"
            + "Row #3: \n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #7: \n"
            + "Row #8: \n"
            + "Row #9: \n"
            + "Row #10: \n"
            + "Row #11: \n"
            + "Row #12: \n"
            + "Row #13: \n"
            + "Row #14: \n"
            + "Row #15: \n"
            + "Row #16: \n"
            + "Row #17: \n"
            + "Row #18: \n"
            + "Row #19: \n"
            + "Row #20: \n"
            + "Row #21: \n"
            + "Row #22: 1\n"
            + "Row #23: \n"
            + "Row #24: \n"
            + "Row #25: \n"
            + "Row #26: \n"
            + "Row #27: \n"
            + "Row #28: 1\n"
            + "Row #29: \n"
            + "Row #30: \n"
            + "Row #31: \n"
            + "Row #32: \n"
            + "Row #33: \n"
            + "Row #34: \n"
            + "Row #35: \n"
            + "Row #36: \n"
            + "Row #37: \n"
            + "Row #38: \n"
            + "Row #39: \n"
            + "Row #40: \n"
            + "Row #41: \n"
            + "Row #42: \n"
            + "Row #43: \n"
            + "Row #44: \n"
            + "Row #45: \n"
            + "Row #46: \n"
            + "Row #47: \n"
            + "Row #48: \n"
            + "Row #49: \n"
            + "Row #50: 1\n"
            + "Row #51: \n"
            + "Row #52: \n"
            + "Row #53: \n"
            + "Row #54: 2\n");
    }

    public void testSlicerAxisDoesNotGetNonEmptyApplied() {
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, true);
        String mdxQuery = "select from [Sales]\n"
            + "where [Time].[Time].[1997]\n";
        Query query = getConnection().parseQuery(mdxQuery);
        TestContext.assertEqualsVerbose(mdxQuery, query.toString());
     }
}

// End NonEmptyPropertyForAllAxisTest.java
