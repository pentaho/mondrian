/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Pentaho Corporation..  All rights reserved.
*/

package mondrian.test;

public class BasicSteelWheelsQueryTest extends SteelWheelsTestCase {

    public BasicSteelWheelsQueryTest( String name ) {
      super( name );
    }

    public BasicSteelWheelsQueryTest() {
    }

    public void testCrossJoinContextChangedNonNative() {
      propSaver.set(propSaver.properties.EnableNativeCrossJoin, false);

      String mdx = "with member [Measures].[YTD Sales]"
              + " as 'Aggregate(Ytd([Time].CurrentMember),"
              + " [Measures].[Sales])'\n"
              + "select \n "
              + "NON EMPTY {[Measures].[YTD Sales]} ON COLUMNS, \n"
              + "NON EMPTY Crossjoin({[Customers].[Danish Wholesale Imports]},"
              + " Crossjoin({[Time].[2003].[QTR1].[Mar]}, \n"
              + "Crossjoin({[Markets].[All Markets]}, "
              + "[Product].[All Products].Children))) ON ROWS\n"
              + "from [SteelWheelsSales]";

      getTestContext().assertQueryReturns(mdx, "Axis #0:\n"
              + "{}\n"
              + "Axis #1:\n"
              + "{[Measures].[YTD Sales]}\n"
              + "Axis #2:\n"
              + "{[Customers].[Danish Wholesale Imports]," +
              " [Time].[2003].[QTR1].[Mar], [Markets].[All Markets]," +
              " [Product].[Classic Cars]}\n"
              + "{[Customers].[Danish Wholesale Imports], [Time].[2003].[QTR1].[Mar],"
              + " [Markets].[All Markets], [Product].[Ships]}\n"
              + "{[Customers].[Danish Wholesale Imports], [Time].[2003].[QTR1].[Mar],"
              + " [Markets].[All Markets], [Product].[Trains]}\n"
              + "{[Customers].[Danish Wholesale Imports], [Time].[2003].[QTR1].[Mar],"
              + " [Markets].[All Markets], [Product].[Vintage Cars]}\n"
              + "Row #0: 20,464\n"
              + "Row #1: 20,452\n"
              + "Row #2: 4,330\n"
              + "Row #3: 13,625\n");
    }

    public void testCrossJoinContextChangedNative() {
      propSaver.set(propSaver.properties.EnableNativeCrossJoin, true);

      String mdx = "with member [Measures].[YTD Sales]"
              + " as 'Aggregate(Ytd([Time].CurrentMember),"
              + " [Measures].[Sales])'\n"
              + "select \n "
              + "NON EMPTY {[Measures].[YTD Sales]} ON COLUMNS, \n"
              + "NON EMPTY Crossjoin({[Customers].[Danish Wholesale Imports]},"
              + " Crossjoin({[Time].[2003].[QTR1].[Mar]}, \n"
              + "Crossjoin({[Markets].[All Markets]}, "
              + "[Product].[All Products].Children))) ON ROWS\n"
              + "from [SteelWheelsSales]";

      getTestContext().assertQueryReturns(mdx, "Axis #0:\n"
              + "{}\n"
              + "Axis #1:\n"
              + "{[Measures].[YTD Sales]}\n"
              + "Axis #2:\n"
              + "{[Customers].[Danish Wholesale Imports]," +
              " [Time].[2003].[QTR1].[Mar], [Markets].[All Markets]," +
              " [Product].[Classic Cars]}\n"
              + "{[Customers].[Danish Wholesale Imports], [Time].[2003].[QTR1].[Mar],"
              + " [Markets].[All Markets], [Product].[Ships]}\n"
              + "{[Customers].[Danish Wholesale Imports], [Time].[2003].[QTR1].[Mar],"
              + " [Markets].[All Markets], [Product].[Trains]}\n"
              + "{[Customers].[Danish Wholesale Imports], [Time].[2003].[QTR1].[Mar],"
              + " [Markets].[All Markets], [Product].[Vintage Cars]}\n"
              + "Row #0: 20,464\n"
              + "Row #1: 20,452\n"
              + "Row #2: 4,330\n"
              + "Row #3: 13,625\n");
    }
}
