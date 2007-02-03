/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * Exception which indicates some resource limit was exceeded.
 * When a client receives a <code>MemoryLimitExceededException</code> the state
 * of the objects associated with the query execution can NOT be
 * counted on being correct - specifically data structures could be
 * in an inconsistent state or missing entirely. No attempt should be
 * make to access or use the result objects.
 *
 * @version $Id$
 */
public class MemoryLimitExceededException 
        extends ResultLimitExceededException {

    public MemoryLimitExceededException(String message) {
        super(message);
    }
}

// End MemoryLimitExceededException.java
