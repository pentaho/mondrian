/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.agg.*;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.rolap.sql.TupleConstraint;

import javax.sql.DataSource;

/**
 * Fake DataServicesProvider so that the locator has something to find
 * for DataServicesLocatorTest.testLocatesValidProvider
 */
public class FakeDataServicesProvider implements DataServicesProvider {
    public MemberReader getMemberReader(RolapCubeHierarchy hierarchy) {
        return null;
    }

    public SegmentLoader getSegmentLoader(SegmentCacheManager cacheMgr) {
        return null;
    }

    public DataSource createDataSource(
        DataSource dataSource,
        Util.PropertyList connectInfo,
        StringBuilder builder)
    {
        return null;
    }

    public TupleReader getTupleReader(TupleConstraint constraint) {
        return null;
    }

    public JdbcSchema.Factory getJdbcSchemaFactory() {
        return null;
    }
}
// End FakeDataServicesProvider.java
