/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.calc.ResultStyle;
import mondrian.olap.*;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.test.DiffRepository;

import java.util.*;
import java.lang.ref.*;

/**
 * Unit-test for non cacheable elementos of high dimensions.
 *
 * @author jlopez, lcanals
 * @version $Id$
 * @since May, 2008
 */
public class HighDimensionsTest extends FoodMartTestCase {
    public HighDimensionsTest() {

    }

    public HighDimensionsTest(String name) {
        super(name);
    }

    
    public void testBug1971406() throws Exception {
        final Connection connection = TestContext.instance()
            .getFoodMartConnection();
        Query query = connection.parseQuery(
            "with set necj as "
            + "NonEmptyCrossJoin(NonEmptyCrossJoin("
            + "[Customers].[Name].members,[Store].[Store Name].members),"
            + "[Product].[Product Name].members) "
            + "select {[Measures].[Unit Sales]} on columns,"
            + "tail(intersect(necj,necj,ALL),5) on rows from sales");
        final long t0 = System.currentTimeMillis();
        Result result = connection.execute(query);
        for(final Position o : result.getAxes()[0].getPositions()) {
            assertNotNull(o.get(0));
        }
        final long t1 = System.currentTimeMillis();
        assertTrue(t1-t0 < 60000);
    }
}
