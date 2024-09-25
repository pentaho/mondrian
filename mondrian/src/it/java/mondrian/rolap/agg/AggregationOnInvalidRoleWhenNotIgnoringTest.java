/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.rolap.agg;

import mondrian.test.TestContext;
import mondrian.test.loader.CsvDBTestCase;

import static mondrian.rolap.agg.AggregationOnInvalidRoleTest.CUBE;
import static mondrian.rolap.agg.AggregationOnInvalidRoleTest.ROLE;
import static mondrian.rolap.agg.AggregationOnInvalidRoleTest.executeAnalyzerQuery;

/**
 * @author Andrey Khayrutdinov
 */
public class AggregationOnInvalidRoleWhenNotIgnoringTest extends CsvDBTestCase {

    @Override
    protected String getFileName() {
        return "mondrian_2225.csv";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.properties.UseAggregates, true);
        propSaver.set(propSaver.properties.ReadAggregates, true);
        propSaver.set(propSaver.properties.IgnoreInvalidMembers, false);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected TestContext createTestContext() {
        // don't do anything dangerous here, just get standard context
        return TestContext.instance();
    }

    private TestContext createContext() {
        TestContext context = TestContext.instance()
            .create(null, CUBE, null, null, null, ROLE)
            .withRole("Test");
        context.flushSchemaCache();
        return context;
    }


    public void test_ThrowsException_WhenNonIgnoringInvalidMembers() {
        try {
            executeAnalyzerQuery(createContext());
        } catch (Exception e) {
            // that's ok, junit's assertion errors are derived from Error,
            // hence they will not be caught here
            return;
        }
        fail("Schema should not load when restriction is invalid");
    }
}

// End AggregationOnInvalidRole_IfNotIgnore_Test.java
