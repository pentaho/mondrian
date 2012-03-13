/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2010 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

/**
 * Extension to {@link RolapMember} that knows the current cube.
 *
 * <p>This is typical of members that occur in queries (where there is always a
 * current cube) as opposed to members that belong to caches. Members of shared
 * dimensions might occur in several different cubes, or even several times in
 * the same virtual cube.
 *
 * @author jhyde
 * @since 20 March, 2010
 */
public interface RolapMemberInCube extends RolapMember {
    /**
     * Returns the cube this cube member belongs to.
     *
     * <p>This method is not in the {@link RolapMember} interface, because
     * regular members may be shared, and therefore do not belong to a specific
     * cube.
     *
     * @return Cube this cube member belongs to, never null
     */
    RolapCube getCube();
}

// End RolapMemberInCube.java
