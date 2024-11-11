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


package mondrian.spi;

import mondrian.olap.QueryTiming;

/**
 * Called when a statement has profile information.
 */
public interface ProfileHandler {
    /**
     * Called when a statement has finished executing.
     *
     * @param plan Annotated plan
     * @param timing Query timings
     */
    public void explain(String plan, QueryTiming timing);
}

// End ProfileHandler.java
