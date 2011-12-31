/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.olap.MondrianProperties;
import mondrian.test.BasicQueryTest;

/**
 * Test suite that runs the {@link BasicQueryTest} but with the
 * {@link MockSegmentCache} active.
 *
 * @author LBoudreau
 * @version $Id$
 */
public class SegmentCacheTest extends BasicQueryTest {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(
            MondrianProperties.instance().DisableCaching,
            true);
        getTestContext().getConnection().getCacheControl(null)
            .flushSchemaCache();
        propSaver.set(
            MondrianProperties.instance().SegmentCache,
            MockSegmentCache.class.getName());
    }

    @Override
    protected void tearDown() throws Exception {
        propSaver.reset();
        getTestContext().getConnection().getCacheControl(null)
            .flushSchemaCache();
        super.tearDown();
    }

    public void testCompoundPredicatesCollision() {
        String query =
            "SELECT [Gender].[All Gender] ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";
        String query2 =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * "
            + "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[All Gender]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n";
        String result2 =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 2,716\n";
        assertQueryReturns(query, result);
        assertQueryReturns(query2, result2);
    }
}

// End SegmentCacheTest.java
