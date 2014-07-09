/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

/**
 * A RolapClosure contains the resources needed by a parent-child
 * level to be able to use the closure table when getting a member's
 * children from the DB.
 */
public class RolapClosure {
    /**
     * The closed peer level which knows how to link
     * to the closure table when resolving parent-child relations.
     */
    public final RolapLevel closedPeerLevel;
    /**
     * A SQL DB column in the closure table which contains
     * the distance of the relation for each parent-child tuple.
     */
    final RolapSchema.PhysColumn distanceColumn;

    public RolapClosure(
        RolapLevel closedPeerLevel,
        RolapSchema.PhysColumn distanceColumn)
    {
        this.closedPeerLevel = closedPeerLevel;
        this.distanceColumn = distanceColumn;
    }
}

// End RolapClosure.java
