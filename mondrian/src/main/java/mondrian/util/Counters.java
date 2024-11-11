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


package mondrian.util;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A collection of counters. Used internally for logging and
 * consistency-checking purposes. Should not be relied upon by applications.
 */
public abstract class Counters {
    /** Number of times {@code SqlStatement.execute} has completed
     * successfully. */
    public static final AtomicLong SQL_STATEMENT_EXECUTE_COUNT =
        new AtomicLong();

    /** Number of times {@code SqlStatement.close} has been called. */
    public static final AtomicLong SQL_STATEMENT_CLOSE_COUNT = new AtomicLong();

    /** Ids of all {@code SqlStatement} instances that are executing. */
    public static final Set<Long> SQL_STATEMENT_EXECUTING_IDS =
        Collections.synchronizedSet(new HashSet<Long>());
}

// End Counters.java
