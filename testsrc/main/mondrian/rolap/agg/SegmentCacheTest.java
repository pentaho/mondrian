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
            MondrianProperties.instance().SegmentCache,
            MockSegmentCache.class.getName());
        propSaver.set(
            MondrianProperties.instance().SegmentCacheFailOnError,
            true);
    }
}
// End SegmentCacheTest.java
