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


package mondrian.olap;

/**
 * Exception which indicates some resource limit was exceeded.
 * When a client receives a <code>MemoryLimitExceededException</code> the state
 * of the objects associated with the query execution can NOT be
 * counted on being correct - specifically data structures could be
 * in an inconsistent state or missing entirely. No attempt should be
 * make to access or use the result objects.
 */
public class MemoryLimitExceededException
    extends ResultLimitExceededException
{

    public MemoryLimitExceededException(String message) {
        super(message);
    }
}

// End MemoryLimitExceededException.java
