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
import mondrian.rolap.sql.TupleConstraint;

import javax.sql.DataSource;

/**
 * An SPI to provide alternate ways of accessing source data.
 */
public interface DataServicesProvider {
    MemberReader getMemberReader(RolapCubeHierarchy hierarchy);

    SegmentLoader getSegmentLoader(SegmentCacheManager cacheMgr);

    DataSource createDataSource(
        DataSource dataSource,
        Util.PropertyList connectInfo,
        StringBuilder builder);

    SqlTupleReader getTupleReader(TupleConstraint constraint);
}
// End DataServicesProvider.java
