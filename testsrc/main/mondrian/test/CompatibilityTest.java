/*
 // $Id$
 // This software is subject to the terms of the Common Public License
 // Agreement, available at the following URL:
 // http://www.opensource.org/licenses/cpl.html.
 // Copyright (C) 2005 SAS Institute, Inc.
 // All Rights Reserved.
 // You must accept the terms of that agreement to use this software.
 //
 // sasebb, March 30, 2005
 */
package mondrian.test;

/**
 * <code>CompatibilityTest</code> is a test case which tests
 * MDX syntax compatibility with Microsoft and SAS servers.
 * There is no MDX spec document per se, so compatibility with de-facto
 * standards from the major vendors is important. Uses the FoodMart database.
 *
 * @author sasebb
 * @since March 30, 2005
 */
public class CompatibilityTest extends FoodMartTestCase {
	public CompatibilityTest(String name) {
		super(name);
	}

	/**
	 * Cube names are case in-sensitive, whether or not square brackets are used.
	 */
	public void testCubeCase() {
		String queryFrom = "select {[Measures].[Unit Sales]} on columns from ";
		String result = "Axis #0:" + nl + "{}" + nl + "Axis #1:" + nl + "{[Measures].[Unit Sales]}"
				+ nl + "Row #0: 266,773" + nl;

		runQueryCheckResult(queryFrom + "Sales", result);
		//more later runQueryCheckResult(queryFrom + "SALES", result);
	}

}
