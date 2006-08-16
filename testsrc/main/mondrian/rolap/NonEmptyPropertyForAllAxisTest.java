/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde and others
// Copyright (C) 2005, Thomson Medstat, Inc, Ann Arbor, MI.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartTestCase;

/**
 * Tests the {@link MondrianProperties#EnableNonEmptyOnAllAxis} property.
 *
 * @version $Id$
 */
public class NonEmptyPropertyForAllAxisTest extends FoodMartTestCase {

    public void testNonEmptyForAllAxesWithPropertySet() {

        try {
            MondrianProperties.instance().EnableNonEmptyOnAllAxis.set(true);
            final String MDX_QUERY = "select {[Country].[USA].[OR].Children} on 0, {[Promotions].Members} on 1 from [Sales] where [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]";
            final String EXPECTED_RESULT = fold("Axis #0:\n" +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                "Axis #1:\n" +
                "{[Customers].[All Customers].[USA].[OR].[Albany]}\n" +
                "{[Customers].[All Customers].[USA].[OR].[Corvallis]}\n" +
                "{[Customers].[All Customers].[USA].[OR].[Lake Oswego]}\n" +
                "{[Customers].[All Customers].[USA].[OR].[Lebanon]}\n" +
                "{[Customers].[All Customers].[USA].[OR].[Portland]}\n" +
                "{[Customers].[All Customers].[USA].[OR].[Woodburn]}\n" +
                "Axis #2:\n" +
                "{[Promotions].[All Promotions]}\n" +
                "{[Promotions].[All Promotions].[Cash Register Lottery]}\n" +
                "{[Promotions].[All Promotions].[No Promotion]}\n" +
                "{[Promotions].[All Promotions].[Saving Days]}\n" +
                "Row #0: 4\n" +
                "Row #0: 6\n" +
                "Row #0: 5\n" +
                "Row #0: 10\n" +
                "Row #0: 6\n" +
                "Row #0: 3\n" +
                "Row #1: \n" +
                "Row #1: 2\n" +
                "Row #1: \n" +
                "Row #1: 2\n" +
                "Row #1: \n" +
                "Row #1: \n" +
                "Row #2: 4\n" +
                "Row #2: 4\n" +
                "Row #2: 3\n" +
                "Row #2: 8\n" +
                "Row #2: 6\n" +
                "Row #2: 3\n" +
                "Row #3: \n" +
                "Row #3: \n" +
                "Row #3: 2\n" +
                "Row #3: \n" +
                "Row #3: \n" +
                "Row #3: \n");
            assertQueryReturns(MDX_QUERY, EXPECTED_RESULT);
        } finally {
            MondrianProperties.instance().EnableNonEmptyOnAllAxis.set(false);
        }
    }

    public void testNonEmptyForAllAxesWithOutPropertySet() {
        final String MDX_QUERY = "SELECT {customers.USA.CA.[Santa Cruz].[Brian Merlo]} on 0, [product].[product category].members on 1 FROM [sales]";
        final String EXPECTED_RESULT = fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Customers].[All Customers].[USA].[CA].[Santa Cruz].[Brian Merlo]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n" +
                "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages]}\n" +
                "{[Product].[All Products].[Drink].[Beverages].[Drinks]}\n" +
                "{[Product].[All Products].[Drink].[Beverages].[Hot Beverages]}\n" +
                "{[Product].[All Products].[Drink].[Beverages].[Pure Juice Beverages]}\n" +
                "{[Product].[All Products].[Drink].[Dairy].[Dairy]}\n" +
                "{[Product].[All Products].[Food].[Baked Goods].[Bread]}\n" +
                "{[Product].[All Products].[Food].[Baking Goods].[Baking Goods]}\n" +
                "{[Product].[All Products].[Food].[Baking Goods].[Jams and Jellies]}\n" +
                "{[Product].[All Products].[Food].[Breakfast Foods].[Breakfast Foods]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods].[Canned Anchovies]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods].[Canned Clams]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods].[Canned Oysters]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods].[Canned Sardines]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods].[Canned Shrimp]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods].[Canned Soup]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods].[Canned Tuna]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods].[Vegetables]}\n" +
                "{[Product].[All Products].[Food].[Canned Products].[Fruit]}\n" +
                "{[Product].[All Products].[Food].[Dairy].[Dairy]}\n" +
                "{[Product].[All Products].[Food].[Deli].[Meat]}\n" +
                "{[Product].[All Products].[Food].[Deli].[Side Dishes]}\n" +
                "{[Product].[All Products].[Food].[Eggs].[Eggs]}\n" +
                "{[Product].[All Products].[Food].[Frozen Foods].[Breakfast Foods]}\n" +
                "{[Product].[All Products].[Food].[Frozen Foods].[Frozen Desserts]}\n" +
                "{[Product].[All Products].[Food].[Frozen Foods].[Frozen Entrees]}\n" +
                "{[Product].[All Products].[Food].[Frozen Foods].[Meat]}\n" +
                "{[Product].[All Products].[Food].[Frozen Foods].[Pizza]}\n" +
                "{[Product].[All Products].[Food].[Frozen Foods].[Vegetables]}\n" +
                "{[Product].[All Products].[Food].[Meat].[Meat]}\n" +
                "{[Product].[All Products].[Food].[Produce].[Fruit]}\n" +
                "{[Product].[All Products].[Food].[Produce].[Packaged Vegetables]}\n" +
                "{[Product].[All Products].[Food].[Produce].[Specialty]}\n" +
                "{[Product].[All Products].[Food].[Produce].[Vegetables]}\n" +
                "{[Product].[All Products].[Food].[Seafood].[Seafood]}\n" +
                "{[Product].[All Products].[Food].[Snack Foods].[Snack Foods]}\n" +
                "{[Product].[All Products].[Food].[Snacks].[Candy]}\n" +
                "{[Product].[All Products].[Food].[Starchy Foods].[Starchy Foods]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Carousel].[Specialty]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Checkout].[Hardware]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Checkout].[Miscellaneous]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Health and Hygiene].[Decongestants]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Health and Hygiene].[Hygiene]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household].[Bathroom Products]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household].[Candles]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household].[Cleaning Supplies]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household].[Electrical]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household].[Hardware]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household].[Kitchen Products]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household].[Paper Products]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household].[Plastic Products]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Periodicals].[Magazines]}\n" +
                "Row #0: 2\n" +
                "Row #1: 2\n" +
                "Row #2: \n" +
                "Row #3: \n" +
                "Row #4: \n" +
                "Row #5: \n" +
                "Row #6: \n" +
                "Row #7: \n" +
                "Row #8: \n" +
                "Row #9: \n" +
                "Row #10: \n" +
                "Row #11: \n" +
                "Row #12: \n" +
                "Row #13: \n" +
                "Row #14: \n" +
                "Row #15: \n" +
                "Row #16: \n" +
                "Row #17: \n" +
                "Row #18: \n" +
                "Row #19: \n" +
                "Row #20: \n" +
                "Row #21: \n" +
                "Row #22: 1\n" +
                "Row #23: \n" +
                "Row #24: \n" +
                "Row #25: \n" +
                "Row #26: \n" +
                "Row #27: \n" +
                "Row #28: 1\n" +
                "Row #29: \n" +
                "Row #30: \n" +
                "Row #31: \n" +
                "Row #32: \n" +
                "Row #33: \n" +
                "Row #34: \n" +
                "Row #35: \n" +
                "Row #36: \n" +
                "Row #37: \n" +
                "Row #38: \n" +
                "Row #39: \n" +
                "Row #40: \n" +
                "Row #41: \n" +
                "Row #42: \n" +
                "Row #43: \n" +
                "Row #44: \n" +
                "Row #45: \n" +
                "Row #46: \n" +
                "Row #47: \n" +
                "Row #48: \n" +
                "Row #49: \n" +
                "Row #50: 1\n" +
                "Row #51: \n" +
                "Row #52: \n" +
                "Row #53: \n" +
                "Row #54: 2\n");
        assertQueryReturns(MDX_QUERY, EXPECTED_RESULT);
    }
}

// End NonEmptyPropertyForAllAxisTest.java
