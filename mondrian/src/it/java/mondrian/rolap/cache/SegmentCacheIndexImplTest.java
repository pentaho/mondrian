/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.rolap.cache;

import static org.mockito.Mockito.mock;

import mondrian.spi.SegmentBody;
import mondrian.spi.SegmentHeader;
import mondrian.test.FoodMartTestCase;

public class SegmentCacheIndexImplTest extends FoodMartTestCase {
    public void testNoHeaderOnLoad() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());

        final SegmentHeader header = mock(SegmentHeader.class);
        final SegmentBody body = mock(SegmentBody.class);

        // This should not fail.
        index.loadSucceeded(header, body);
    }
}

//End SegmentCacheIndexImplTest.java
