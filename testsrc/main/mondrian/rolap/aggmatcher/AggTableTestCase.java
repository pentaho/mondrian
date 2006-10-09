/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.aggmatcher;

import mondrian.test.loader.CsvDBTestCase;
import mondrian.test.TestContext;
import mondrian.olap.Schema;
import mondrian.olap.Cube;
import mondrian.olap.MondrianProperties;

/** 
 * This abstract class can be used as the basis for writing aggregate table
 * test in the "testsrc/main/mondrian/rolap/aggmatcher" directory. Taken care
 * of is the setting of the Caching and Aggregate Read/Use properties and
 * the reloading of the aggregate tables after the CSV tables are loaded.
 * The particular cube definition and CSV file to use are abstract methods.
 * 
 * @author <a>Richard M. Emberson</a>
 * @version 
 */
public abstract class AggTableTestCase extends CsvDBTestCase {

    private static final String DIRECTORY =
                            "testsrc/main/mondrian/rolap/aggmatcher";
    
    private TestContext testContext;
    private boolean currentUse;
    private boolean currentRead;
    private boolean do_caching_orig;

    public AggTableTestCase() {
        super();
    }
    public AggTableTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();


/*
        Schema schema = getConnection().getSchema();
        final Cube cube = schema.createCube(cubeDescription);
*/
        String cubeDescription = getCubeDescription();
        this.testContext = TestContext.create(null,
                            cubeDescription, null, null, null);


        // store current property values
        MondrianProperties props = MondrianProperties.instance();
        this.currentUse = props.UseAggregates.get();
        this.currentRead = props.ReadAggregates.get();
        this.do_caching_orig = props.DisableCaching.get();

        // turn off caching
        props.DisableCaching.setString("true");


        
        // re-read aggregates
        props.UseAggregates.setString("true");
        props.ReadAggregates.setString("false");
        props.ReadAggregates.setString("true");
    }
    protected void tearDown() throws Exception {
        // reset property values
        MondrianProperties props = MondrianProperties.instance();
        if (this.currentRead) {
            props.ReadAggregates.setString("true");
        } else {
            props.ReadAggregates.setString("false");
        }
        if (this.currentUse) {
            props.UseAggregates.setString("true");
        } else {
            props.UseAggregates.setString("false");
        }
        if (this.do_caching_orig) {
            props.DisableCaching.setString("true");
        } else {
            props.DisableCaching.setString("false");
        }

        super.tearDown();
    }

    protected TestContext getCubeTestContext() {
        return testContext;
    }
    protected abstract String getCubeDescription();

    protected String getDirectoryName() {
        return DIRECTORY;
    }
}
