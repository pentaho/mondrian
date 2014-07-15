/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
//
// remberson, Jan 31, 2006
*/
package mondrian.olap;

import mondrian.test.FoodMartTestCase;

public class HierarchyBugTest extends FoodMartTestCase {
    public HierarchyBugTest(String name) {
        super(name);
    }
    public HierarchyBugTest() {
        super();
    }

    /*
        This is code that demonstrates a bug that appears when using
        JPivot with the current version of Mondrian. With the previous
        version of Mondrian (and JPivot), pre compilation Mondrian,
        this was not a bug (or at least Mondrian did not have a null
        hierarchy).
        Here the Time dimension is not returned in axis == 0, rather
        null is returned. This causes a NullPointer exception in JPivot
        when it tries to access the (null) hierarchy's name.
        If the Time hierarchy is miss named in the query string, then
        the parse ought to pick it up.
     */
    public void testNoHierarchy() {
        String queryString =
            "select NON EMPTY "
            + "Crossjoin(Hierarchize(Union({[Time].[Time].LastSibling}, "
            + "[Time].[Time].LastSibling.Children)), "
            + "{[Measures].[Unit Sales],      "
            + "[Measures].[Store Cost]}) ON columns, "
            + "NON EMPTY Hierarchize(Union({[Store].[All Stores]}, "
            + "[Store].[All Stores].Children)) ON rows "
            + "from [Sales]";

        Connection conn = getConnection();
        Query query = conn.parseQuery(queryString);
        Result result = conn.execute(query);

        String failStr = null;
        int len = query.getAxes().length;
        for (int i = 0; i < len; i++) {
            Hierarchy[] hs =
                query.getMdxHierarchiesOnAxis(
                    AxisOrdinal.StandardAxisOrdinal.forLogicalOrdinal(i));
            if (hs == null) {
            } else {
                for (Hierarchy h : hs) {
                    // This should NEVER be null, but it is.
                    if (h == null) {
                        failStr =
                            "Got a null Hierarchy, "
                            + "Should be Time Hierarchy";
                    }
                }
            }
        }
        if (failStr != null) {
            fail(failStr);
        }
    }
}

// End HierarchyBugTest.java
