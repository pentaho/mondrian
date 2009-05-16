/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
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
