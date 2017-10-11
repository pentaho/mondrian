/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Hitachi Vantara.
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianException;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * @author Andrey Khayrutdinov
 */
public class RolapNativeSqlInjectionTest extends FoodMartTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.properties.EnableNativeFilter, true);
        propSaver.set(propSaver.properties.EnableNativeCrossJoin, true);
    }

    public void testMondrian2436() {
        String mdxQuery = ""
            + "select {[Measures].[Store Sales]} on columns, "
            + "filter([Customers].[Name].Members, (([Measures].[Store Sales]) > '(select 1000)')) on rows "
            + "from [Sales]";

        TestContext context = getTestContext().withFreshConnection();
        try {
            context.executeQuery(mdxQuery);
        } catch (MondrianException e) {
            assertNotNull(
                "MondrianEvaluationException is expected on invalid filter condition",
                e.getCause());
            assertEquals(
                "Expected to get decimal, but got (select 1000)",
                e.getCause().getMessage());
            return;
        } finally {
            context.close();
        }

        fail("[Store Sales] filtering should not work for non-valid decimals");
    }
}

// End RolapNativeSqlInjectionTest.java
