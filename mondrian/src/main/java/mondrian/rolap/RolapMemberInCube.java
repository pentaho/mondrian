/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
