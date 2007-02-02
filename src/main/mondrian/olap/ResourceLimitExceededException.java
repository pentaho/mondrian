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
 *
 * @version $Id$
 */
public class ResourceLimitExceeded extends ResultLimitExceeded {
    public ResourceLimitExceeded(String message) {
        super(message);
    }
}

// End ResourceLimitExceeded.java
