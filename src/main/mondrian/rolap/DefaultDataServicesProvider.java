/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.rolap.agg.*;
import mondrian.spi.DataServicesProvider;

/**
 * Default implementation of DataServicesProvider
 */
public class DefaultDataServicesProvider implements DataServicesProvider {
    public MemberReader getMemberReader(RolapCubeHierarchy hierarchy) {
        return new SqlMemberSource(hierarchy);
    }

    public SegmentLoader getSegmentLoader(SegmentCacheManager cacheMgr) {
        return new SegmentLoader(cacheMgr);
    }
}
// End DefaultDataServicesProvider.java
