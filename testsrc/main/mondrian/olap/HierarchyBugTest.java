/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// remberson, Jan 31, 2006
*/

package mondrian.olap;

import mondrian.test.FoodMartTestCase;
import mondrian.olap.*;

public class HierarchyBug extends FoodMartTestCase {
    public HierarchyBug(String name) {
        super(name);
    }
    public HierarchyBug() {
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
    public void noHierarchyTest() {
        String queryString = 
            "select NON EMPTY " +
            "Crossjoin(Hierarchize(Union({[Time].LastSibling}, " +
            "[Time].LastSibling.Children)), " +
            "{[Measures].[Unit Sales],      " +
            "[Measures].[Store Cost]}) ON columns, " +
            "NON EMPTY Hierarchize(Union({[Store].[All Stores]}, " +
            "[Store].[All Stores].Children)) ON rows " +
            "from [Sales]";

        Connection conn = getConnection();
        Query query = conn.parseQuery(queryString);
        Result result = conn.execute(query);

        String failStr = null;
        int len = query.axes.length;
        System.out.println("HierarchyBug.noHierarchyTest: len=" +len);
        for (int i = 0; i < len; i++) {
            Hierarchy[] hs = query.getMdxHierarchiesOnAxis(i);
            if (hs == null) {
                System.out.println("HierarchyBug.noHierarchyTest: got null i=" +i);
            } else {
                for (int j = 0; j < hs.length; j++) {
                    Hierarchy h = hs[j];
                    System.out.print("HierarchyBug.noHierarchyTest: j=" +j);
                    if (h == null) {
                        System.out.println(": got null");
                        failStr = "Got a null Hierarchy, " + 
                            "Should be Time Hierarchy";
                    } else {
                        System.out.println(": h=" +h.getName());
                    }
                }
            }
        }       
        if (failStr != null) {
            fail (failStr);
        }
    }
}
