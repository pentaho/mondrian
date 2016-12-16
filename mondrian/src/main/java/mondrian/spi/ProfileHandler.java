/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

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
